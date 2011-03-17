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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;

public class TaskAttemptStatusUpdateEvent extends TaskAttemptEvent {

  private TaskAttemptStatus reportedTaskAttemptStatus;

  public TaskAttemptStatusUpdateEvent(TaskAttemptID id,
      TaskAttemptStatus taskAttemptStatus) {
    super(id, TaskAttemptEventType.TA_UPDATE);
    this.reportedTaskAttemptStatus = taskAttemptStatus;
  }

  public TaskAttemptStatus getReportedTaskAttemptStatus() {
    return reportedTaskAttemptStatus;
  }

  /**
   * The internal TaskAttemptStatus object corresponding to remote Task status.
   * 
   */
  public static class TaskAttemptStatus {
    public org.apache.hadoop.mapreduce.v2.api.TaskAttemptID id;
    public float progress;
    public org.apache.hadoop.mapreduce.v2.api.Counters counters;
    public java.lang.CharSequence diagnosticInfo;
    public java.lang.CharSequence stateString;
    public org.apache.hadoop.mapreduce.v2.api.Phase phase;
    public long outputSize;
    public List<TaskAttemptID> fetchFailedMaps;
  }
}
