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

import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.lib.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobState;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskState;
import org.apache.hadoop.mapreduce.v2.api.TaskType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Test;

public class TestJobHistoryEvents {
  private static final Log LOG = LogFactory.getLog(TestJobHistoryEvents.class);

  @Test
  public void testHistoryEvents() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.job.user.name", "test");
    MRApp app = new HistoryEnabledApp(2, 1, true);
    app.submit(conf);
    Job job = app.getContext().getAllJobs().values().iterator().next();
    JobID jobId = job.getID();
    LOG.info("JOBID is " + TypeConverter.fromYarn(jobId).toString());
    app.waitForState(job, JobState.SUCCEEDED);
    /*
     * Use HistoryContext to read logged events and verify the number of 
     * completed maps 
    */
    HistoryContext context = new JobHistory(conf);
    Job parsedJob = context.getJob(jobId);
    Assert.assertEquals("CompletedMaps not correct", 2,
        parsedJob.getCompletedMaps());
    
    
    Map<TaskID, Task> tasks = parsedJob.getTasks();
    Assert.assertEquals("No of tasks not correct", 3, tasks.size());
    for (Task task : tasks.values()) {
      verifyTask(task);
    }
    
    Map<TaskID, Task> maps = parsedJob.getTasks(TaskType.MAP);
    Assert.assertEquals("No of maps not correct", 2, maps.size());
    
    Map<TaskID, Task> reduces = parsedJob.getTasks(TaskType.REDUCE);
    Assert.assertEquals("No of reduces not correct", 1, reduces.size());
    
    
    Assert.assertEquals("CompletedReduce not correct", 1,
        parsedJob.getCompletedReduces());
    
    Assert.assertEquals("Job state not currect", JobState.SUCCEEDED,
        parsedJob.getState());
  }

  private void verifyTask(Task task) {
    Assert.assertEquals("Task state not currect", TaskState.SUCCEEDED,
        task.getState());
    Map<TaskAttemptID, TaskAttempt> attempts = task.getAttempts();
    Assert.assertEquals("No of attempts not correct", 1, attempts.size());
    for (TaskAttempt attempt : attempts.values()) {
      verifyAttempt(attempt);
    }
  }

  private void verifyAttempt(TaskAttempt attempt) {
    Assert.assertEquals("TaskAttempt state not currect", 
        TaskAttemptState.SUCCEEDED, attempt.getState());
  }

  static class HistoryEnabledApp extends MRApp {
    public HistoryEnabledApp(int maps, int reduces, boolean autoComplete) {
      super(maps, reduces, autoComplete);
    }

    @Override
    protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
        Configuration conf) {
      return new JobHistoryEventHandler(conf);
    }
  }

  public static void main(String[] args) throws Exception {
    TestJobHistoryEvents t = new TestJobHistoryEvents();
    t.testHistoryEvents();
  }
}
