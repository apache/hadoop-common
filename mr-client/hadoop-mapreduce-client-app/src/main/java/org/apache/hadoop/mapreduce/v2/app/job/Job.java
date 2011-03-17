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

package org.apache.hadoop.mapreduce.v2.app.job;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.JobState;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

/**
 * Main interface to interact with the job. Provides only getters. 
 */
public interface Job {

  JobID getID();
  CharSequence getName();
  JobState getState();
  JobReport getReport();
  Counters getCounters();
  Map<TaskID,Task> getTasks();
  Map<TaskID,Task> getTasks(TaskType taskType);
  Task getTask(TaskID taskID);
  List<String> getDiagnostics();
  int getTotalMaps();
  int getTotalReduces();
  int getCompletedMaps();
  int getCompletedReduces();

  TaskAttemptCompletionEvent[]
      getTaskAttemptCompletionEvents(int fromEventId, int maxEvents);
}
