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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.yarn.service.Service;
import org.junit.Test;

public class TestJobHistoryParsing {
  private static final Log LOG = LogFactory.getLog(TestJobHistoryParsing.class);
  //TODO FIX once final CompletedStatusStore is available
//  private static final String STATUS_STORE_DIR_KEY =
//    "yarn.server.nodemanager.jobstatus";
  @Test
  public void testHistoryParsing() throws Exception {
    Configuration conf = new Configuration();
    MRApp app = new MRApp(2, 1, true, this.getClass().getName(), true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobId jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);
    
    //make sure all events are flushed
    app.waitForState(Service.STATE.STOPPED);
    
    String jobhistoryDir = JobHistoryUtils.getConfiguredHistoryIntermediateDoneDirPrefix(conf);
    JobHistory jobHistory = new JobHistory();
    jobHistory.init(conf);
    
    String currentJobHistoryDir = JobHistoryUtils.getCurrentDoneDir(jobhistoryDir);
    JobIndexInfo jobIndexInfo = jobHistory.getJobMetaInfo(jobId).getJobIndexInfo();
    String jobhistoryFileName = FileNameIndexUtils.getDoneFileName(jobIndexInfo);
    
    Path historyFilePath = new Path(currentJobHistoryDir, jobhistoryFileName);
    FSDataInputStream in = null;
    LOG.info("JobHistoryFile is: " + historyFilePath);
    try {
      FileContext fc = FileContext.getFileContext(conf);
      in = fc.open(fc.makeQualified(historyFilePath));
    } catch (IOException ioe) {
      LOG.info("Can not open history file: " + historyFilePath, ioe);
      throw (new Exception("Can not open History File"));
    }
    
    JobHistoryParser parser = new JobHistoryParser(in);
    JobInfo jobInfo = parser.parse();
    
    Assert.assertTrue ("Incorrect username ",
        jobInfo.getUsername().equals("mapred"));
    Assert.assertTrue("Incorrect jobName ",
        jobInfo.getJobname().equals("test"));
    Assert.assertTrue("Incorrect queuename ",
        jobInfo.getJobQueueName().equals("default"));
    Assert.assertTrue("incorrect conf path",
        jobInfo.getJobConfPath().equals("test"));
    Assert.assertTrue("incorrect finishedMap ",
        jobInfo.getFinishedMaps() == 2);
    Assert.assertTrue("incorrect finishedReduces ",
        jobInfo.getFinishedReduces() == 1);
    int totalTasks = jobInfo.getAllTasks().size();
    Assert.assertTrue("total number of tasks is incorrect  ", totalTasks == 3);

    //Assert at taskAttempt level
    for (TaskInfo taskInfo :  jobInfo.getAllTasks().values()) {
      int taskAttemptCount = taskInfo.getAllTaskAttempts().size();
      Assert.assertTrue("total number of task attempts ", 
          taskAttemptCount == 1);
    }
//
//   // Test for checking jobstats for job status store
//    Path statusFilePath = new Path(jobstatusDir, "jobstats");
//    try {
//      FileContext fc = FileContext.getFileContext(statusFilePath.toUri());
//      in = fc.open(statusFilePath);
//    } catch (IOException ioe) {
//      LOG.info("Can not open status file "+ ioe);
//      throw (new Exception("Can not open status File"));
//    }
//    parser = new JobHistoryParser(in);
//    jobInfo = parser.parse();
//    Assert.assertTrue("incorrect finishedMap in job stats file ",
//        jobInfo.getFinishedMaps() == 2);
//    Assert.assertTrue("incorrect finishedReduces in job stats file ",
//        jobInfo.getFinishedReduces() == 1);
  }

  public static void main(String[] args) throws Exception {
    TestJobHistoryParsing t = new TestJobHistoryParsing();
    t.testHistoryParsing();
  }
}
