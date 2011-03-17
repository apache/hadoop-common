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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.lib.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.TaskState;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

public class CompletedTask implements Task {


  private final TaskType type;
  private final Counters counters;
  private final long startTime;
  private final long finishTime;
  private final TaskState state;
  private final TaskID taskID;
  private final TaskReport report;
  private final Map<TaskAttemptID, TaskAttempt> attempts =
    new LinkedHashMap<TaskAttemptID, TaskAttempt>();
  
  private static final Log LOG = LogFactory.getLog(CompletedTask.class);

  CompletedTask(TaskID taskID, TaskInfo taskinfo) {
    this.taskID = taskID;
    this.startTime = taskinfo.getStartTime();
    this.finishTime = taskinfo.getFinishTime();
    this.type = TypeConverter.toYarn(taskinfo.getTaskType());
    this.counters = TypeConverter.toYarn(
        new org.apache.hadoop.mapred.Counters(taskinfo.getCounters()));
    this.state = TaskState.valueOf(taskinfo.getTaskStatus());
    for (TaskAttemptInfo attemptHistory : 
                taskinfo.getAllTaskAttempts().values()) {
      CompletedTaskAttempt attempt = new CompletedTaskAttempt(taskID, 
          attemptHistory);
      attempts.put(attempt.getID(), attempt);
    }
    
    report = new TaskReport();
    report.id = taskID;
    report.startTime = startTime;
    report.finishTime = finishTime;
    report.state = state;
    report.progress = getProgress();
    report.counters = getCounters();
    report.runningAttempts = new ArrayList<TaskAttemptID>();
    report.runningAttempts.addAll(attempts.keySet());
    //report.successfulAttempt = taskHistory.; //TODO
  }

  @Override
  public boolean canCommit(TaskAttemptID taskAttemptID) {
    return false;
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptID attemptID) {
    return attempts.get(attemptID);
  }

  @Override
  public Map<TaskAttemptID, TaskAttempt> getAttempts() {
    return attempts;
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public TaskID getID() {
    return taskID;
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public TaskReport getReport() {
    return report;
  }

  @Override
  public TaskType getType() {
    return type;
  }

  @Override
  public boolean isFinished() {
    return true;
  }

  @Override
  public TaskState getState() {
    return state;
  }

}
