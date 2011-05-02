/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.util.JobHistoryUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * The job history events get routed to this class. This class writes the 
 * Job history events to the local file and moves the local file to HDFS on 
 * job completion.
 * JobHistory implementation is in this package to access package private 
 * classes.
 */
public class JobHistoryEventHandler extends AbstractService
    implements EventHandler<JobHistoryEvent> {

  private final AppContext context;
  private final int startCount;

  private FileContext logDirFc; // log Dir FileContext
  private FileContext doneDirFc; // done Dir FileContext

  private Path logDirPath = null;
  private Path doneDirPrefixPath = null; // folder for completed jobs

  private BlockingQueue<JobHistoryEvent> eventQueue =
    new LinkedBlockingQueue<JobHistoryEvent>();
  private Thread eventHandlingThread;
  private volatile boolean stopped;

  private static final Log LOG = LogFactory.getLog(
      JobHistoryEventHandler.class);

  private static final Map<JobId, MetaInfo> fileMap =
    Collections.<JobId,MetaInfo>synchronizedMap(new HashMap<JobId,MetaInfo>());

  public JobHistoryEventHandler(AppContext context, int startCount) {
    super("JobHistoryEventHandler");
    this.context = context;
    this.startCount = startCount;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.service.AbstractService#init(org.apache.hadoop.conf.Configuration)
   * Initializes the FileContext and Path objects for the log and done directories.
   * Creates these directories if they do not already exist.
   */
  @Override
  public void init(Configuration conf) {
    
    String logDir = JobHistoryUtils.getConfiguredHistoryLogDirPrefix(conf);
    String  doneDirPrefix = JobHistoryUtils.getConfiguredHistoryDoneDirPrefix(conf);

    try {
      doneDirPrefixPath = FileContext.getFileContext(conf).makeQualified(
          new Path(doneDirPrefix));
      doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri(), conf);
      if (!doneDirFc.util().exists(doneDirPrefixPath)) {
        try {
          doneDirFc.mkdir(doneDirPrefixPath, new FsPermission(
              JobHistoryUtils.HISTORY_DIR_PERMISSION), true);
        } catch (FileAlreadyExistsException e) {
          LOG.info("JobHistory Done Directory: [" + doneDirPrefixPath
              + "] already exists.");
        }
      }
    } catch (IOException e) {
          LOG.info("error creating done directory on dfs " + e);
          throw new YarnException(e);
    }
    try {
      logDirPath = FileContext.getFileContext(conf).makeQualified(
          new Path(logDir));
      logDirFc = FileContext.getFileContext(logDirPath.toUri(), conf);
      if (!logDirFc.util().exists(logDirPath)) {
        try {
          logDirFc.mkdir(logDirPath, new FsPermission(JobHistoryUtils.HISTORY_DIR_PERMISSION),
              true);
        } catch (FileAlreadyExistsException e) {
          LOG.info("JobHistory Log Directory: [" + doneDirPrefixPath
              + "] already exists.");
        }
      }
    } catch (IOException ioe) {
      LOG.info("Mkdirs failed to create " + logDirPath.toString());
      throw new YarnException(ioe);
    }
    super.init(conf);
  }

  @Override
  public void start() {
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        JobHistoryEvent event = null;
        while (!stopped || !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }
          handleEvent(event);
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    stopped = true;
    //do not interrupt while event handling is in progress
    synchronized(this) {
      eventHandlingThread.interrupt();
    }

    try {
      eventHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info("Interruped Exception while stopping", ie);
    }
    //write all the events remaining in queue
    Iterator<JobHistoryEvent> it = eventQueue.iterator();
    while(it.hasNext()) {
      JobHistoryEvent ev = it.next();
      LOG.info("In stop, writing event " + ev.getType());
      handleEvent(ev);
    }
    
    //close all file handles
    for (MetaInfo mi : fileMap.values()) {
      try {
        mi.closeWriter();
      } catch (IOException e) {
        LOG.info("Exception while closing file " + e.getMessage());
      }
    }

    super.stop();
  }

  /**
   * Create an event writer for the Job represented by the jobID.
   * Writes out the job configuration to the log directory.
   * This should be the first call to history for a job
   * 
   * @param jobId the jobId.
   * @throws IOException
   */
  protected void setupEventWriter(JobId jobId)
  throws IOException {
    if (logDirPath == null) {
      LOG.info("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);
    Configuration conf = getConfig();

    long submitTime = (oldFi == null ? context.getClock().getTime() : oldFi.submitTime);

    // String user = conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    
    Path logFile = JobHistoryUtils.getJobHistoryFile(logDirPath, jobId, startCount);
    String user = conf.get(MRJobConfig.USER_NAME);
    if (user == null) {
      throw new IOException("User is null while setting up jobhistory eventwriter" );
    }
    String jobName = TypeConverter.fromYarn(jobId).toString();
    EventWriter writer = (oldFi == null) ? null : oldFi.writer;
 
    if (writer == null) {
      try {
        FSDataOutputStream out = logDirFc.create(logFile,
            EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
        //TODO Permissions for the history file?
        writer = new EventWriter(out);
      } catch (IOException ioe) {
        LOG.info("Could not create log file: [" + logFile + "] + for job " + "[" + jobName + "]");
        throw ioe;
      }
    }
    
    //This could be done at the end as well in moveToDone
    Path logDirConfPath = null;
    if (conf != null) {
      logDirConfPath = getConfFile(logDirPath, jobId);
      LOG.info("XXX: Attempting to write config to: " + logDirConfPath);
      FSDataOutputStream jobFileOut = null;
      try {
        if (logDirConfPath != null) {
          jobFileOut = logDirFc.create(logDirConfPath,
              EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
          conf.writeXml(jobFileOut);
          jobFileOut.close();
        }
      } catch (IOException e) {
        LOG.info("Failed to close the job configuration file "
            + StringUtils.stringifyException(e));
      }
    }
    
    MetaInfo fi = new MetaInfo(logFile, logDirConfPath, writer, submitTime, user, jobName);
    fileMap.put(jobId, fi);
  }

  /** Close the event writer for this id 
   * @throws IOException */
  public void closeWriter(JobId id) throws IOException {
    try {
      final MetaInfo mi = fileMap.get(id);
      if (mi != null) {
        mi.closeWriter();
      }
      
    } catch (IOException e) {
      LOG.info("Error closing writer for JobID: " + id);
      throw e;
    }
  }

  @Override
  public void handle(JobHistoryEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  protected synchronized void handleEvent(JobHistoryEvent event) {
    // check for first event from a job
    if (event.getHistoryEvent().getEventType() == EventType.JOB_SUBMITTED) {
      try {
        setupEventWriter(event.getJobID());
      } catch (IOException ioe) {
        LOG.error("Error JobHistoryEventHandler in handle " + ioe);
        throw new YarnException(ioe);
      }
    }
    MetaInfo mi = fileMap.get(event.getJobID());
    EventWriter writer = fileMap.get(event.getJobID()).writer;
    try {
      HistoryEvent historyEvent = event.getHistoryEvent();
      mi.writeEvent(historyEvent);
      LOG.info("In HistoryEventHandler " + event.getHistoryEvent().getEventType());
    } catch (IOException e) {
      LOG.error("Error writing History Event " + e);
      throw new YarnException(e);
    }
    // check for done
    if (event.getHistoryEvent().getEventType().equals(EventType.JOB_FINISHED)) {
      try {
        closeEventWriter(event.getJobID());
      } catch (IOException e) {
        throw new YarnException(e);
      }
    }
  }

  protected void closeEventWriter(JobId jobId) throws IOException {
    final MetaInfo mi = fileMap.get(jobId);
    
    try {
      if (mi != null) {
        mi.closeWriter();
      }
     
      if (mi == null || mi.getHistoryFile() == null) {
        LOG.info("No file for job-history with " + jobId + " found in cache!");
      }
      if (mi.getConfFile() == null) {
        LOG.info("No file for jobconf with " + jobId + " found in cache!");
      }
      
      
      //TODO fix - add indexed structure 
      // 
      String doneDir = JobHistoryUtils.getCurrentDoneDir(doneDirPrefixPath.toString());
      Path doneDirPath =
    	  doneDirFc.makeQualified(new Path(doneDir));
      if (!pathExists(doneDirFc, doneDirPath)) {
        try {
          doneDirFc.mkdir(doneDirPath, new FsPermission(JobHistoryUtils.HISTORY_DIR_PERMISSION), true);
        } catch (FileAlreadyExistsException e) {
          LOG.info("Done directory: [" + doneDirPath + "] already exists.");
      }
      }
      Path logFile = mi.getHistoryFile();
      Path qualifiedLogFile = logDirFc.makeQualified(logFile);
      Path qualifiedDoneFile = doneDirFc.makeQualified(new Path(doneDirPath,
          getDoneJobHistoryFileName(jobId)));
      moveToDoneNow(qualifiedLogFile, qualifiedDoneFile);
      
      Path confFile = mi.getConfFile();
      Path qualifiedConfFile = logDirFc.makeQualified(confFile);
      Path qualifiedConfDoneFile = doneDirFc.makeQualified(new Path(doneDirPath, getDoneConfFileName(jobId)));
      moveToDoneNow(qualifiedConfFile, qualifiedConfDoneFile);
      
      logDirFc.delete(qualifiedLogFile, true);
      logDirFc.delete(qualifiedConfFile, true);
    } catch (IOException e) {
      LOG.info("Error closing writer for JobID: " + jobId);
      throw e;
    }
  }

  private static class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;
    long submitTime;
    String user;
    String jobName;

    MetaInfo(Path historyFile, Path conf, EventWriter writer, long submitTime,
             String user, String jobName) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
      this.submitTime = submitTime;
      this.user = user;
      this.jobName = jobName;
    }

    Path getHistoryFile() { return historyFile; }

    Path getConfFile() {return confFile; } 

    synchronized void closeWriter() throws IOException {
      if (writer != null) {
        writer.close();
      }
      writer = null;
    }

    synchronized void writeEvent(HistoryEvent event) throws IOException {
      if (writer != null) {
        writer.write(event);
        writer.flush();
      }
    }
  }

  //TODO Move some of these functions into a utility class.


  private String getDoneJobHistoryFileName(JobId jobId) {
    return TypeConverter.fromYarn(jobId).toString() + JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION;
  }

  private String getDoneConfFileName(JobId jobId) {
    return TypeConverter.fromYarn(jobId).toString() + JobHistoryUtils.CONF_FILE_NAME_SUFFIX;
  }
  
  private Path getConfFile(Path logDir, JobId jobId) {
    Path jobFilePath = null;
    if (logDir != null) {
      jobFilePath = new Path(logDir, TypeConverter.fromYarn(jobId).toString()
          + "_" + startCount + JobHistoryUtils.CONF_FILE_NAME_SUFFIX);
    }
    return jobFilePath;
  }


  //TODO This could be done by the jobHistory server - move files to a temporary location
  //  which is scanned by the JH server - to move them to the final location.
  // Currently JHEventHandler is moving files to the final location.
  private void moveToDoneNow(Path fromPath, Path toPath) throws IOException {
    //check if path exists, in case of retries it may not exist
    if (logDirFc.util().exists(fromPath)) {
      LOG.info("Moving " + fromPath.toString() + " to " +
          toPath.toString());
      //TODO temporarily removing the existing dst
      if (logDirFc.util().exists(toPath)) {
        logDirFc.delete(toPath, true);
      }
      boolean copied = logDirFc.util().copy(fromPath, toPath);
      if (copied)
          LOG.info("Copied to done location: "+ toPath);
      else 
          LOG.info("copy failed");
      doneDirFc.setPermission(toPath,
          new FsPermission(JobHistoryUtils.HISTORY_FILE_PERMISSION));
    }
  }

  boolean pathExists(FileContext fc, Path path) throws IOException {
    return fc.util().exists(path);
  }

  private void writeStatus(String statusstoredir, HistoryEvent event) throws IOException {
    try {
      Path statusstorepath = doneDirFc.makeQualified(new Path(statusstoredir));
      doneDirFc.mkdir(statusstorepath,
         new FsPermission(JobHistoryUtils.HISTORY_DIR_PERMISSION), true);
      Path toPath = new Path(statusstoredir, "jobstats");
      FSDataOutputStream out = doneDirFc.create(toPath, EnumSet
           .of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
      EventWriter writer = new EventWriter(out);
      writer.write(event);
      writer.close();
      out.close();
    } catch (IOException ioe) {
        LOG.error("Status file write failed" +ioe);
        throw ioe;
    }
  }
}
