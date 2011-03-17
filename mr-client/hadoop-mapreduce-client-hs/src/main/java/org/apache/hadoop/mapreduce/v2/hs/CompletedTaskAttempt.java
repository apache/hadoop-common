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
import java.util.List;

import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.lib.TypeConverter;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.TaskID;

public class CompletedTaskAttempt implements TaskAttempt {

  private final TaskAttemptInfo attemptInfo;
  private final TaskAttemptID attemptId;
  private final Counters counters;
  private final TaskAttemptState state;
  private final TaskAttemptReport report;
  private final List<CharSequence> diagnostics = new ArrayList<CharSequence>();

  CompletedTaskAttempt(TaskID taskID, TaskAttemptInfo attemptInfo) {
    this.attemptInfo = attemptInfo;
    this.attemptId = TypeConverter.toYarn(attemptInfo.getAttemptId());
    this.counters = TypeConverter.toYarn(
        new org.apache.hadoop.mapred.Counters(attemptInfo.getCounters()));
    this.state = TaskAttemptState.valueOf(attemptInfo.getState());
    
    if (attemptInfo.getError() != null) {
      diagnostics.add(attemptInfo.getError());
    }
    
    report = new TaskAttemptReport();
    report.id = attemptId;
    report.state = state;
    report.progress = getProgress();
    report.startTime = attemptInfo.getStartTime();
    report.finishTime = attemptInfo.getFinishTime();
    report.diagnosticInfo = attemptInfo.getError();
    //result.phase = attemptInfo.get;//TODO
    report.stateString = state.toString();
    report.counters = getCounters();
  }

  @Override
  public ContainerID getAssignedContainerID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    return attemptInfo.getHostname();
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public TaskAttemptID getID() {
    return attemptId;
  }

  @Override
  public float getProgress() {
    return 1.0f;
  }

  @Override
  public TaskAttemptReport getReport() {
    return report;
  }

  @Override
  public TaskAttemptState getState() {
    return state;
  }

  @Override
  public boolean isFinished() {
    return true;
  }

  @Override
  public List<CharSequence> getDiagnostics() {
    return diagnostics;
  }

  @Override
  public long getLaunchTime() {
    return report.startTime;
  }

  @Override
  public long getFinishTime() {
    return report.finishTime;
  }
}
