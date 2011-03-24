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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.JobState;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

/**
 * Loads the basic job level data upfront.
 * Data from job history file is loaded lazily.
 */
public class CompletedJob implements org.apache.hadoop.mapreduce.v2.app.job.Job {
  
  static final Log LOG = LogFactory.getLog(CompletedJob.class);
  private final Counters counters;
  private final Configuration conf;
  private final JobID jobID;
  private final List<String> diagnostics = new ArrayList<String>();
  private final JobReport report;
  private final Map<TaskID, Task> tasks = new HashMap<TaskID, Task>();
  private final Map<TaskID, Task> mapTasks = new HashMap<TaskID, Task>();
  private final Map<TaskID, Task> reduceTasks = new HashMap<TaskID, Task>();
  
  private TaskAttemptCompletionEvent[] completionEvents;
  private JobInfo jobInfo;


  public CompletedJob(Configuration conf, JobID jobID) throws IOException {
    this.conf = conf;
    this.jobID = jobID;
    //TODO fix
    /*
    String  doneLocation =
      conf.get(JTConfig.JT_JOBHISTORY_COMPLETED_LOCATION,
      "file:///tmp/yarn/done/status");
    String user =
      conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    String statusstoredir =
      doneLocation + "/" + user + "/" + TypeConverter.fromYarn(jobID).toString();
    Path statusFile = new Path(statusstoredir, "jobstats");
    try {
      FileContext fc = FileContext.getFileContext(statusFile.toUri(), conf);
      FSDataInputStream in = fc.open(statusFile);
      JobHistoryParser parser = new JobHistoryParser(in);
      jobStats = parser.parse();
    } catch (IOException e) {
      LOG.info("Could not open job status store file from dfs " +
        TypeConverter.fromYarn(jobID).toString());
      throw new IOException(e);
    }
    */
    
    //TODO: load the data lazily. for now load the full data upfront
    loadFullHistoryData();

    counters = TypeConverter.toYarn(jobInfo.getTotalCounters());
    diagnostics.add(jobInfo.getErrorInfo());
    report = new JobReport();
    report.id = jobID;
    report.state = JobState.valueOf(jobInfo.getJobStatus());
    report.startTime = jobInfo.getLaunchTime();
    report.finishTime = jobInfo.getFinishTime();
  }

  @Override
  public int getCompletedMaps() {
    return jobInfo.getFinishedMaps();
  }

  @Override
  public int getCompletedReduces() {
    return jobInfo.getFinishedReduces();
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public JobID getID() {
    return jobID;
  }

  @Override
  public JobReport getReport() {
    return report;
  }

  @Override
  public JobState getState() {
    return report.state;
  }

  @Override
  public Task getTask(TaskID taskID) {
    return tasks.get(taskID);
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    return completionEvents;
  }

  @Override
  public Map<TaskID, Task> getTasks() {
    return tasks;
  }

  //History data is leisurely loaded when task level data is requested
  private synchronized void loadFullHistoryData() {
    if (jobInfo != null) {
      return; //data already loaded
    }
    String user = conf.get(MRJobConfig.USER_NAME);
    if (user == null) {
      LOG.error("user null is not allowed");
    }
    String jobName = TypeConverter.fromYarn(jobID).toString();
    String jobhistoryDir = conf.get("yarn.server.nodemanager.jobhistory",
        "file:///tmp/yarn/done")
        + "/" + user;
    FSDataInputStream in = null;
    String jobhistoryFileName = jobName; // TODO use existing hadoop dire
                                         // structure
    Path historyFilePath = new Path(jobhistoryDir, jobhistoryFileName);

    try {
      FileContext fc = FileContext.getFileContext(historyFilePath.toUri());
      in = fc.open(historyFilePath);
      JobHistoryParser parser = new JobHistoryParser(in);
      jobInfo = parser.parse();
      LOG.info("jobInfo loaded");
    } catch (IOException e) {
      throw new YarnException("Could not load history file " + historyFilePath,
          e);
    }
    
    // populate the tasks
    for (Map.Entry<org.apache.hadoop.mapreduce.TaskID, TaskInfo> entry : jobInfo
        .getAllTasks().entrySet()) {
      TaskID yarnTaskID = TypeConverter.toYarn(entry.getKey());
      TaskInfo taskInfo = entry.getValue();
      Task task = new CompletedTask(yarnTaskID, taskInfo);
      tasks.put(yarnTaskID, task);
      if (task.getType() == TaskType.MAP) {
        mapTasks.put(task.getID(), task);
      } else if (task.getType() == TaskType.REDUCE) {
        reduceTasks.put(task.getID(), task);
      }
    }
    
    // TODO: populate the TaskAttemptCompletionEvent
    completionEvents = new TaskAttemptCompletionEvent[0];
    
    
  }

  @Override
  public List<String> getDiagnostics() {
    return diagnostics;
  }

  @Override
  public CharSequence getName() {
    return jobInfo.getJobname();
  }

  @Override
  public int getTotalMaps() {
    return jobInfo.getTotalMaps();
  }

  @Override
  public int getTotalReduces() {
    return jobInfo.getTotalReduces();
  }

  @Override
  public Map<TaskID, Task> getTasks(TaskType taskType) {
    if (TaskType.MAP.equals(taskType)) {
      return mapTasks;
    } else {//we have only two type of tasks
      return reduceTasks;
    }
  }
}
