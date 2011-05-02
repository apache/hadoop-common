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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.util.JobHistoryUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/*
 * Loads and manages the Job history cache.
 */
public class JobHistory implements HistoryContext {

  private Map<JobId, Job> completedJobCache =
    new ConcurrentHashMap<JobId, Job>();
  private Configuration conf;
  private final ApplicationId appID;
  private final String userName;
  private final LinkedList<Job> jobQ = new LinkedList<Job>();
  private static final Log LOG = LogFactory.getLog(JobHistory.class);
  private final int retiredJobsCacheSize = 1000; //TODO make it configurable


  public JobHistory(Configuration conf) {
    this.conf = conf;
    userName = conf.get(MRJobConfig.USER_NAME, "history-user");
    //TODO fixme - bogus appID for now
    this.appID = RecordFactoryProvider.getRecordFactory(conf)
        .newRecordInstance(ApplicationId.class);
  }
  @Override
  public synchronized Job getJob(JobId jobId) {
    Job job = completedJobCache.get(jobId);
    if (job == null) {
      try {
        job = new CompletedJob(conf, jobId);
      } catch (IOException e) {
        LOG.warn("HistoryContext getJob failed " + e);
        throw new YarnException(e);
      }
      completedJobCache.put(jobId, job);
      jobQ.add(job);
      if (jobQ.size() > retiredJobsCacheSize) {
         Job removed = jobQ.remove();
         completedJobCache.remove(removed.getID());
      }
    }
    return job;
  }

  @Override
  public Map<JobId, Job> getAllJobs(ApplicationId appID) {
    //currently there is 1 to 1 mapping between app and job id
    org.apache.hadoop.mapreduce.JobID oldJobID = TypeConverter.fromYarn(appID);
    Map<JobId, Job> jobs = new HashMap<JobId, Job>();
    JobId jobID = TypeConverter.toYarn(oldJobID);
    jobs.put(jobID, getJob(jobID));
    return jobs;
  }
  
  //TODO FIX ME use indexed search so we do not reload the 
  // previously processed files
  @Override
  public Map<JobId, Job> getAllJobs() {
    //currently there is 1 to 1 mapping between app and job id
    Map<JobId, Job> jobs = new HashMap<JobId, Job>();
    String jobhistoryDir = JobHistoryUtils.getConfiguredHistoryDoneDirPrefix(conf);
    
    String currentJobHistoryDoneDir = JobHistoryUtils.getCurrentDoneDir(jobhistoryDir);
    
    try {
      Path done = FileContext.getFileContext(conf).makeQualified(
          new Path(currentJobHistoryDoneDir));
      FileContext doneDirFc = FileContext.getFileContext(done.toUri(), conf);
      RemoteIterator<LocatedFileStatus> historyFiles = doneDirFc.util()
          .listFiles(done, true);
      if (historyFiles != null) {
        FileStatus f;
        while (historyFiles.hasNext()) {
          f = historyFiles.next();
          if (f.isDirectory()) continue;
          if (!f.getPath().getName().endsWith(JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION)) continue;
          //TODO_JH_Change to parse the name properly
          String fileName = f.getPath().getName();
          String jobName = fileName.substring(0, fileName.indexOf(JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION));
          LOG.info("Processing job: " + jobName);
          org.apache.hadoop.mapreduce.JobID oldJobID = JobID.forName(jobName);
          JobId jobID = TypeConverter.toYarn(oldJobID);
          Job job = new CompletedJob(conf, jobID, false);
          jobs.put(jobID, job);
          // completedJobCache.put(jobID, job);
        }
      }
    } catch (IOException ie) {
      LOG.info("Error while creating historyFileMap" + ie);
    }
    return jobs;
  }
  @Override
  public ApplicationId getApplicationID() {
    return appID;
  }
  @Override
  public EventHandler getEventHandler() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public CharSequence getUser() {
    return userName;
  }
  
  
 @Override
 public Clock getClock() {
   return null;
 }
}
