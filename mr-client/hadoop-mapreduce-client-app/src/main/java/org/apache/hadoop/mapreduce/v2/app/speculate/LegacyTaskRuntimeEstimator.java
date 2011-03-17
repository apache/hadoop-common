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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.TaskID;



public class LegacyTaskRuntimeEstimator extends StartEndTimesBase {

  private final Map<TaskAttempt, AtomicLong> attemptRuntimeEstimates
      = new ConcurrentHashMap<TaskAttempt, AtomicLong>();
  private final Map<TaskAttempt, AtomicLong> attemptRuntimeEstimateVariances
      = new ConcurrentHashMap<TaskAttempt, AtomicLong>();


  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {
    super.updateAttempt(status, timestamp);

    String stateString = status.stateString.toString();

    TaskAttemptID attemptID = status.id;
    TaskID taskID = attemptID.taskID;
    JobID jobID = taskID.jobID;
    Job job = context.getJob(jobID);

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }

    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt == null) {
      return;
    }

    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;

    // We need to do two things.
    //  1: If this is a completion, we accumulate statistics in the superclass
    //  2: If this is not a completion, we learn more about it.

    // This is not a completion, but we're cooking.
    //
    if (stateString.equals(TaskAttemptState.RUNNING.name())) {
      // See if this task is already in the registry
      AtomicLong estimateContainer = attemptRuntimeEstimates.get(taskAttempt);
      AtomicLong estimateVarianceContainer
          = attemptRuntimeEstimateVariances.get(taskAttempt);

      if (estimateContainer == null) {
        synchronized (attemptRuntimeEstimates) {
          if (attemptRuntimeEstimates.get(taskAttempt) == null) {
            attemptRuntimeEstimates.put(taskAttempt, new AtomicLong());
          }

          estimateContainer = attemptRuntimeEstimates.get(taskAttempt);
        }
      }

      if (estimateVarianceContainer == null) {
        synchronized (attemptRuntimeEstimateVariances) {
          if (attemptRuntimeEstimateVariances.get(taskAttempt) == null) {
            attemptRuntimeEstimateVariances.put(taskAttempt, new AtomicLong());
          }

          estimateVarianceContainer
              = attemptRuntimeEstimateVariances.get(taskAttempt);
        }
      }

      long estimate = -1;
      long varianceEstimate = -1;

      // This code assumes that we'll never consider starting a third
      //  speculative task attempt if two are already running for this task
      if (start > 0 && timestamp > start) {
        estimate = (long) ((timestamp - start) / Math.max(0.0001, status.progress));
        varianceEstimate = (long) (estimate * status.progress / 10);
      }

      estimateContainer.set(estimate);
      estimateVarianceContainer.set(varianceEstimate);
    }
  }

  private long storedPerAttemptValue
       (Map<TaskAttempt, AtomicLong> data, TaskAttemptID attemptID) {
    TaskID taskID = attemptID.taskID;
    JobID jobID = taskID.jobID;
    Job job = context.getJob(jobID);

    Task task = job.getTask(taskID);

    if (task == null) {
      return -1L;
    }

    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt == null) {
      return -1L;
    }

    AtomicLong estimate = data.get(taskAttempt);

    return estimate == null ? -1L : estimate.get();

  }

  @Override
  public long estimatedRuntime(TaskAttemptID attemptID) {
    return storedPerAttemptValue(attemptRuntimeEstimates, attemptID);
  }

  @Override
  public long runtimeEstimateVariance(TaskAttemptID attemptID) {
    return storedPerAttemptValue(attemptRuntimeEstimateVariances, attemptID);
  }
}
