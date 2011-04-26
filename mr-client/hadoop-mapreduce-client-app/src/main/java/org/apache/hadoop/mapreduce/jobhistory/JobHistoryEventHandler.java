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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
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

  static final FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0750); // rwxr-x---
  
  public static final FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0740); // rwxr-----

  public JobHistoryEventHandler(AppContext context, int startCount) {
    super("JobHistoryEventHandler");
    this.context = context;
    this.startCount = startCount;
  }

  @Override
  public void init(Configuration conf) {
    String defaultLogDir = conf.get(
        YARNApplicationConstants.APPS_STAGING_DIR_KEY) + "/history/staging";
    String logDir = conf.get(YarnMRJobConfig.HISTORY_STAGING_DIR_KEY,
      defaultLogDir);
    String defaultDoneDir = conf.get(
        YARNApplicationConstants.APPS_STAGING_DIR_KEY) + "/history/done";
    String  doneDirPrefix =
      conf.get(YarnMRJobConfig.HISTORY_DONE_DIR_KEY,
          defaultDoneDir);
    try {
      doneDirPrefixPath = FileContext.getFileContext(conf).makeQualified(
          new Path(doneDirPrefix));
      doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri(), conf);
      if (!doneDirFc.util().exists(doneDirPrefixPath)) {
        doneDirFc.mkdir(doneDirPrefixPath,
          new FsPermission(HISTORY_DIR_PERMISSION), true);
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
        logDirFc.mkdir(logDirPath,
          new FsPermission(HISTORY_DIR_PERMISSION), true);
      }
    } catch (IOException ioe) {
      LOG.info("Mkdirs failed to create " +
          logDirPath.toString());
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
   * This should be the first call to history for a job
   * @param jobId
   * @throws IOException
   */
  protected void setupEventWriter(JobId jobId)
  throws IOException {
    if (logDirPath == null) {
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);

    long submitTime = (oldFi == null ? context.getClock().getTime() : oldFi.submitTime);

    Path logFile = getJobHistoryFile(logDirPath, jobId);
    // String user = conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    String user = getConfig().get(MRJobConfig.USER_NAME);
    if (user == null) {
      throw new IOException("User is null while setting up jobhistory eventwriter" );
    }
    String jobName = TypeConverter.fromYarn(jobId).toString();
    EventWriter writer = (oldFi == null) ? null : oldFi.writer;
 
    if (writer == null) {
      try {
        FSDataOutputStream out = logDirFc.create(logFile, EnumSet
            .of(CreateFlag.OVERWRITE));
        writer = new EventWriter(out);
      } catch (IOException ioe) {
        LOG.info("Could not create log file for job " + jobName);
        throw ioe;
      }
    }
    /*TODO Storing the job conf on the log dir if required*/
    MetaInfo fi = new MetaInfo(logFile, writer, submitTime, user, jobName);
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
      LOG.error("in handler ioException " + e);
      throw new YarnException(e);
    }
    // check for done
    if (event.getHistoryEvent().getEventType().equals(EventType.JOB_FINISHED)) {
      JobFinishedEvent jfe = (JobFinishedEvent) event.getHistoryEvent();
      String statusstoredir = doneDirPrefixPath + "/status/" + mi.user + "/" + mi.jobName;
      try {
        writeStatus(statusstoredir, jfe);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        throw new YarnException(e);
      }
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
      Path logFile = mi.getHistoryFile();
      //TODO fix - add indexed structure 
      // 
      String doneDir = doneDirPrefixPath + "/" + mi.user + "/";
      Path doneDirPath =
    	  doneDirFc.makeQualified(new Path(doneDir));
      if (!pathExists(doneDirFc, doneDirPath)) {
        doneDirFc.mkdir(doneDirPath, new FsPermission(HISTORY_DIR_PERMISSION),
            true);
      }
      // Path localFile = new Path(fromLocalFile);
      Path qualifiedLogFile =
    	  logDirFc.makeQualified(logFile);
      Path qualifiedDoneFile =
    	  doneDirFc.makeQualified(new Path(doneDirPath, mi.jobName));
      if (mi != null) {
        mi.closeWriter();
      }
      moveToDoneNow(qualifiedLogFile, qualifiedDoneFile);
      logDirFc.delete(qualifiedLogFile, true);
    } catch (IOException e) {
      LOG.info("Error closing writer for JobID: " + jobId);
      throw e;
    }
  }

  private static class MetaInfo {
    private Path historyFile;
    private EventWriter writer;
    long submitTime;
    String user;
    String jobName;

    MetaInfo(Path historyFile, EventWriter writer, long submitTime,
             String user, String jobName) {
      this.historyFile = historyFile;
      this.writer = writer;
      this.submitTime = submitTime;
      this.user = user;
      this.jobName = jobName;
    }

    Path getHistoryFile() { return historyFile; }

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

  /**
   * Get the job history file path
   */
  private Path getJobHistoryFile(Path dir, JobId jobId) {
    return new Path(dir, TypeConverter.fromYarn(jobId).toString() + "_" + 
        startCount);

  }

/*
 * 
 */
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
          new FsPermission(HISTORY_FILE_PERMISSION));
    }
  }

  boolean pathExists(FileContext fc, Path path) throws IOException {
    return fc.util().exists(path);
  }

  private void writeStatus(String statusstoredir, HistoryEvent event) throws IOException {
    try {
      Path statusstorepath = doneDirFc.makeQualified(new Path(statusstoredir));
      doneDirFc.mkdir(statusstorepath,
         new FsPermission(HISTORY_DIR_PERMISSION), true);
      Path toPath = new Path(statusstoredir, "jobstats");
      FSDataOutputStream out = doneDirFc.create(toPath, EnumSet
           .of(CreateFlag.OVERWRITE));
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
