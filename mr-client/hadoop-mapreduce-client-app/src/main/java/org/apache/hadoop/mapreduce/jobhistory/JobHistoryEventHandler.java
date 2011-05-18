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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
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

  private FileSystem logDirFS; // log Dir FileSystem
  private FileSystem doneDirFS; // done Dir FileSystem

  private Configuration conf;

  private Path logDirPath = null;
  private Path doneDirPrefixPath = null; // folder for completed jobs

  private BlockingQueue<JobHistoryEvent> eventQueue =
    new LinkedBlockingQueue<JobHistoryEvent>();
  private Thread eventHandlingThread;
  private volatile boolean stopped;
  private final Object lock = new Object();

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
   * Initializes the FileSystem and Path objects for the log and done directories.
   * Creates these directories if they do not already exist.
   */
  @Override
  public void init(Configuration conf) {

    this.conf = conf;

    String logDir = JobHistoryUtils.getConfiguredHistoryLogDirPrefix(conf);
    String  doneDirPrefix = JobHistoryUtils.getConfiguredHistoryIntermediateDoneDirPrefix(conf);

    try {
      doneDirPrefixPath = FileSystem.get(conf).makeQualified(
          new Path(doneDirPrefix));
      doneDirFS = FileSystem.get(doneDirPrefixPath.toUri(), conf);
      if (!doneDirFS.exists(doneDirPrefixPath)) {
        try {
          doneDirFS.mkdirs(doneDirPrefixPath, new FsPermission(
              JobHistoryUtils.HISTORY_DIR_PERMISSION));
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
      logDirPath = FileSystem.get(conf).makeQualified(
          new Path(logDir));
      logDirFS = FileSystem.get(logDirPath.toUri(), conf);
      if (!logDirFS.exists(logDirPath)) {
        try {
          logDirFS.mkdirs(logDirPath, new FsPermission(JobHistoryUtils.HISTORY_DIR_PERMISSION));
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
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }
          // If an event has been removed from the queue. Handle it.
          // The rest of the queue is handled via stop()
          // Clear the interrupt status if it's set before calling handleEvent
          // and set it if it was set before calling handleEvent. 
          // Interrupts received from other threads during handleEvent cannot be
          // dealth with - Shell.runCommand() ignores them.
          synchronized (lock) {
            boolean isInterrupted = Thread.interrupted();
          handleEvent(event);
            if (isInterrupted) Thread.currentThread().interrupt();
          }
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping JobHistoryEventHandler");
    stopped = true;
    //do not interrupt while event handling is in progress
    synchronized(lock) {
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
    LOG.info("Stopped JobHistoryEventHandler. super.stop()");
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
      LOG.error("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);
    Configuration conf = getConfig();

    long submitTime = (oldFi == null ? context.getClock().getTime() : oldFi.getJobIndexInfo().getSubmitTime());

    // String user = conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    
    Path logFile = JobHistoryUtils.getStagingJobHistoryFile(logDirPath, jobId, startCount);
    String user = conf.get(MRJobConfig.USER_NAME);
    if (user == null) {
      throw new IOException("User is null while setting up jobhistory eventwriter" );
    }
    String jobName = TypeConverter.fromYarn(jobId).toString();
    EventWriter writer = (oldFi == null) ? null : oldFi.writer;
 
    if (writer == null) {
      try {
        FSDataOutputStream out = logDirFS.create(logFile, true);
        //TODO Permissions for the history file?
        writer = new EventWriter(out);
        LOG.info("Event Writer setup for JobId: " + jobId + ", File: " + logFile);
      } catch (IOException ioe) {
        LOG.info("Could not create log file: [" + logFile + "] + for job " + "[" + jobName + "]");
        throw ioe;
      }
    }
    
    //This could be done at the end as well in moveToDone
    Path logDirConfPath = null;
    if (conf != null) {
      logDirConfPath = JobHistoryUtils.getStagingConfFile(logDirPath, jobId, startCount);
      FSDataOutputStream jobFileOut = null;
      try {
        if (logDirConfPath != null) {
          jobFileOut = logDirFS.create(logDirConfPath, true);
          conf.writeXml(jobFileOut);
          jobFileOut.close();
        }
      } catch (IOException e) {
        LOG.info("Failed to close the job configuration file "
            + StringUtils.stringifyException(e));
      }
    }
    
    MetaInfo fi = new MetaInfo(logFile, logDirConfPath, writer, submitTime, user, jobName, jobId);
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
      LOG.error("Error closing writer for JobID: " + id);
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

  protected void handleEvent(JobHistoryEvent event) {
    // check for first event from a job
    //TODO Log a meta line with version information.
    synchronized (lock) {
    if (event.getHistoryEvent().getEventType() == EventType.JOB_SUBMITTED) {
      try {
        setupEventWriter(event.getJobID());
      } catch (IOException ioe) {
        LOG.error("Error JobHistoryEventHandler in handle " + ioe);
        throw new YarnException(ioe);
      }
    }
    MetaInfo mi = fileMap.get(event.getJobID());
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
        JobFinishedEvent jFinishedEvent = (JobFinishedEvent)event.getHistoryEvent();
        mi.getJobIndexInfo().setFinishTime(jFinishedEvent.getFinishTime());
        mi.getJobIndexInfo().setNumMaps(jFinishedEvent.getFinishedMaps());
        mi.getJobIndexInfo().setNumReduces(jFinishedEvent.getFinishedReduces());
        closeEventWriter(event.getJobID());
      } catch (IOException e) {
        throw new YarnException(e);
      }
    }
  }
  }

  protected void closeEventWriter(JobId jobId) throws IOException {
    final MetaInfo mi = fileMap.get(jobId);
    
    if (mi == null) {
      throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
    }
    try {
        mi.closeWriter();
    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + jobId);
      throw e;
      }
     
    if (mi.getHistoryFile() == null) {
      LOG.warn("No file for job-history with " + jobId + " found in cache!");
      }
      if (mi.getConfFile() == null) {
      LOG.warn("No file for jobconf with " + jobId + " found in cache!");
      }
      
    String doneDir = JobHistoryUtils.getCurrentDoneDir(doneDirPrefixPath
        .toString());
    Path doneDirPath = doneDirFS.makeQualified(new Path(doneDir));
        try {
      if (!pathExists(doneDirFS, doneDirPath)) {
        doneDirFS.mkdirs(doneDirPath, new FsPermission(
            JobHistoryUtils.HISTORY_DIR_PERMISSION));
      }

      if (mi.getHistoryFile() != null) {
      Path logFile = mi.getHistoryFile();
      Path qualifiedLogFile = logDirFS.makeQualified(logFile);
        String doneJobHistoryFileName = FileNameIndexUtils.getDoneFileName(mi
            .getJobIndexInfo());
      Path qualifiedDoneFile = doneDirFS.makeQualified(new Path(doneDirPath,
            doneJobHistoryFileName));
      moveToDoneNow(qualifiedLogFile, qualifiedDoneFile);
      }
      
      if (mi.getConfFile() != null) {
      Path confFile = mi.getConfFile();
      Path qualifiedConfFile = logDirFS.makeQualified(confFile);
        String doneConfFileName = JobHistoryUtils
            .getIntermediateConfFileName(jobId);
        Path qualifiedConfDoneFile = doneDirFS.makeQualified(new Path(
            doneDirPath, doneConfFileName));
      moveToDoneNow(qualifiedConfFile, qualifiedConfDoneFile);
      }
      String doneFileName = JobHistoryUtils.getIntermediateDoneFileName(jobId);
      Path doneFilePath = doneDirFS.makeQualified(new Path(doneDirPath,
          doneFileName));
      touchFile(doneFilePath);
    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + jobId);
      throw e;
    }
  }

  private class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;
    JobIndexInfo jobIndexInfo;

    MetaInfo(Path historyFile, Path conf, EventWriter writer, long submitTime,
             String user, String jobName, JobId jobId) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
      this.jobIndexInfo = new JobIndexInfo(submitTime, -1, user, jobName, jobId, -1, -1);
    }

    Path getHistoryFile() { return historyFile; }

    Path getConfFile() {return confFile; } 

    JobIndexInfo getJobIndexInfo() { return jobIndexInfo; }

    void closeWriter() throws IOException {
      synchronized (lock) {
      if (writer != null) {
        writer.close();
      }
      writer = null;
    }
    }

    void writeEvent(HistoryEvent event) throws IOException {
      synchronized (lock) {
      if (writer != null) {
        writer.write(event);
        writer.flush();
      }
    }
  }
  }

  private void moveToDoneNow(Path fromPath, Path toPath) throws IOException {
    //check if path exists, in case of retries it may not exist
    if (logDirFS.exists(fromPath)) {
      LOG.info("Moving " + fromPath.toString() + " to " +
          toPath.toString());
      //TODO temporarily removing the existing dst
      if (doneDirFS.exists(toPath)) {
        doneDirFS.delete(toPath, true);
      }
      boolean copied =
          FileUtil.copy(logDirFS, fromPath, doneDirFS, toPath, false, conf);
      if (copied)
          LOG.info("Copied to done location: "+ toPath);
      else 
          LOG.info("copy failed");
      doneDirFS.setPermission(toPath,
          new FsPermission(JobHistoryUtils.HISTORY_FILE_PERMISSION));
      
      logDirFS.delete(fromPath, false);
    }
    }

  private void touchFile(Path path) throws IOException {
    doneDirFS.createNewFile(path);
    doneDirFS.setPermission(path, JobHistoryUtils.HISTORY_DIR_PERMISSION);
  }

  boolean pathExists(FileSystem fileSys, Path path) throws IOException {
    return fileSys.exists(path);
  }

  private void writeStatus(String statusstoredir, HistoryEvent event) throws IOException {
    try {
      Path statusstorepath = doneDirFS.makeQualified(new Path(statusstoredir));
      doneDirFS.mkdirs(statusstorepath,
         new FsPermission(JobHistoryUtils.HISTORY_DIR_PERMISSION));
      Path toPath = new Path(statusstoredir, "jobstats");
      FSDataOutputStream out = doneDirFS.create(toPath, true);
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
