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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.lib.TypeConverter;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;

/**
 * The job history events get routed to this class. This class writes the 
 * Job history events to the local file and moves the local file to HDFS on 
 * job completion.
 * JobHistory implementation is in this package to access package private 
 * classes.
 */
public class JobHistoryEventHandler 
    implements EventHandler<JobHistoryEvent> {

  private FileContext logDirFc; // log Dir FileContext
  private FileContext doneDirFc; // done Dir FileContext
  private Configuration conf;

  private Path logDir = null;
  private Path done = null; // folder for completed jobs

  private static final Log LOG = LogFactory.getLog(
      JobHistoryEventHandler.class);

  private EventWriter eventWriter = null;

  private static final Map<JobID, MetaInfo> fileMap =
    Collections.<JobID,MetaInfo>synchronizedMap(new HashMap<JobID,MetaInfo>());

  static final FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0750); // rwxr-x---
  
  public static final FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0740); // rwxr-----

  public JobHistoryEventHandler(Configuration conf) {
    this.conf = conf;
/*
    String localDir = conf.get("yarn.server.nodemanager.jobhistory",
        "file:///" +
        new File(System.getProperty("yarn.log.dir")).getAbsolutePath() +
        File.separator + "history");
*/
    String localDir = conf.get("yarn.server.nodemanager.jobhistory.localdir",
      "file:///tmp/yarn");
    logDir = new Path(localDir);
    String  doneLocation =
      conf.get("yarn.server.nodemanager.jobhistory",
      "file:///tmp/yarn/done");
    if (doneLocation != null) {
      try {
        done = FileContext.getFileContext(conf).makeQualified(new Path(doneLocation));
        doneDirFc = FileContext.getFileContext(done.toUri(), conf);
        if (!doneDirFc.util().exists(done))
          doneDirFc.mkdir(done,
            new FsPermission(HISTORY_DIR_PERMISSION), true);
        } catch (IOException e) {
          LOG.info("error creating done directory on dfs " + e);
          throw new YarnException(e);
      }
    }
    try {
      logDirFc = FileContext.getFileContext(logDir.toUri(), conf);
      if (!logDirFc.util().exists(logDir)) {
        logDirFc.mkdir(logDir, new FsPermission(HISTORY_DIR_PERMISSION), true);
      }
    } catch (IOException ioe) {
      LOG.info("Mkdirs failed to create " +
          logDir.toString());
      throw new YarnException(ioe);
    }
  }

  /**
   * Create an event writer for the Job represented by the jobID.
   * This should be the first call to history for a job
   * @param jobId
   * @throws IOException
   */
  protected void setupEventWriter(JobID jobId)
  throws IOException {
    if (logDir == null) {
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);

    long submitTime = (oldFi == null ? System.currentTimeMillis() : oldFi.submitTime);

    Path logFile = getJobHistoryFile(logDir, jobId);
    // String user = conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    String user = conf.get(MRJobConfig.USER_NAME);
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
    this.eventWriter = writer;
    /*TODO Storing the job conf on the log dir if required*/
    MetaInfo fi = new MetaInfo(logFile, writer, submitTime, user, jobName);
    fileMap.put(jobId, fi);
  }

  /** Close the event writer for this id 
   * @throws IOException */
  public void closeWriter(JobID id) throws IOException {
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

  public synchronized void handle(JobHistoryEvent event) {
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
    } catch (IOException e) {
      LOG.error("in handler ioException " + e);
      throw new YarnException(e);
    }
    // check for done
    if (event.getHistoryEvent().getEventType().equals(EventType.JOB_FINISHED)) {
      JobFinishedEvent jfe = (JobFinishedEvent) event.getHistoryEvent();
      String statusstoredir = done + "/status/" + mi.user + "/" + mi.jobName;
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

  protected void closeEventWriter(JobID jobId) throws IOException {
    final MetaInfo mi = fileMap.get(jobId);
    try {
      Path fromLocalFile = mi.getHistoryFile();
      // Path toPath = new Path(done, mi.jobName);
      String jobhistorydir = done + "/" + mi.user + "/";
      Path jobhistorydirpath =
    	  logDirFc.makeQualified(new Path(jobhistorydir));
      logDirFc.mkdir(jobhistorydirpath,
         new FsPermission(HISTORY_DIR_PERMISSION), true);
      // Path localFile = new Path(fromLocalFile);
      Path localQualifiedFile =
    	  logDirFc.makeQualified(fromLocalFile);
      Path jobHistoryFile =
    	  logDirFc.makeQualified(new Path(jobhistorydirpath, mi.jobName));
      if (mi != null) {
        mi.closeWriter();
      }
      moveToDoneNow(localQualifiedFile, jobHistoryFile);
      logDirFc.delete(localQualifiedFile, true);
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
      }
    }
  }

  /**
   * Get the job history file path
   */
  public static Path getJobHistoryFile(Path dir, JobID jobId) {
    return new Path(dir, TypeConverter.fromYarn(jobId).toString());
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
