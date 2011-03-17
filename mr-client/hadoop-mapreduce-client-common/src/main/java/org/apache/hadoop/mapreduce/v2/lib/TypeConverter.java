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

package org.apache.hadoop.mapreduce.v2.lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.mapreduce.v2.api.Counter;
import org.apache.hadoop.mapreduce.v2.api.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.JobState;
import org.apache.hadoop.mapreduce.v2.api.Phase;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskState;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

public class TypeConverter {

  public static org.apache.hadoop.mapred.JobID fromYarn(JobID id) {
    String identifier = fromClusterTimeStamp(id.appID.clusterTimeStamp);
    return new org.apache.hadoop.mapred.JobID(identifier, id.id);
  }

  //currently there is 1-1 mapping between appid and jobid
  public static org.apache.hadoop.mapreduce.JobID fromYarn(ApplicationID appID) {
    String identifier = fromClusterTimeStamp(appID.clusterTimeStamp);
    return new org.apache.hadoop.mapred.JobID(identifier, appID.id);
  }

  public static JobID toYarn(org.apache.hadoop.mapreduce.JobID id) {
    JobID jobID = new JobID();
    jobID.id = id.getId(); //currently there is 1-1 mapping between appid and jobid
    jobID.appID = new ApplicationID();
    jobID.appID.id = id.getId();
    jobID.appID.clusterTimeStamp = toClusterTimeStamp(id.getJtIdentifier());
    return jobID;
  }

  private static String fromClusterTimeStamp(long clusterTimeStamp) {
    return Long.toString(clusterTimeStamp);
  }

  private static long toClusterTimeStamp(String identifier) {
    return Long.parseLong(identifier);
  }

  public static org.apache.hadoop.mapreduce.TaskType fromYarn(
      TaskType taskType) {
    switch (taskType) {
    case MAP:
      return org.apache.hadoop.mapreduce.TaskType.MAP;
    case REDUCE:
      return org.apache.hadoop.mapreduce.TaskType.REDUCE;
    default:
      throw new YarnException("Unrecognized task type: " + taskType);
    }
  }

  public static TaskType
      toYarn(org.apache.hadoop.mapreduce.TaskType taskType) {
    switch (taskType) {
    case MAP:
      return TaskType.MAP;
    case REDUCE:
      return TaskType.REDUCE;
    default:
      throw new YarnException("Unrecognized task type: " + taskType);
    }
  }

  public static org.apache.hadoop.mapred.TaskID fromYarn(TaskID id) {
    return new org.apache.hadoop.mapred.TaskID(fromYarn(id.jobID), fromYarn(id.taskType),
        id.id);
  }

  public static TaskID toYarn(org.apache.hadoop.mapreduce.TaskID id) {
    TaskID taskID = new TaskID();
    taskID.id = id.getId();
    taskID.taskType = toYarn(id.getTaskType());
    taskID.jobID = toYarn(id.getJobID());
    return taskID;
  }

  public static Phase toYarn(org.apache.hadoop.mapred.TaskStatus.Phase phase) {
    switch (phase) {
    case STARTING:
      return Phase.STARTING;
    case MAP:
      return Phase.MAP;
    case SHUFFLE:
      return Phase.SHUFFLE;
    case SORT:
      return Phase.SORT;
    case REDUCE:
      return Phase.REDUCE;
    case CLEANUP:
      return Phase.CLEANUP;
    }
    throw new YarnException("Unrecognized Phase: " + phase);
  }

  public static TaskCompletionEvent[] fromYarn(
      org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent[] newEvents) {
    TaskCompletionEvent[] oldEvents =
        new TaskCompletionEvent[newEvents.length];
    int i = 0;
    for (org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent newEvent 
        : newEvents) {
      oldEvents[i++] = fromYarn(newEvent);
    }
    return oldEvents;
  }

  public static TaskCompletionEvent fromYarn(
      org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent newEvent) {
    return new TaskCompletionEvent(newEvent.eventId,
              fromYarn(newEvent.attemptId), newEvent.attemptId.id,
              newEvent.attemptId.taskID.taskType.equals(TaskType.MAP),
              fromYarn(newEvent.status),
              newEvent.mapOutputServerAddress.toString());
  }

  public static TaskCompletionEvent.Status fromYarn(
      TaskAttemptCompletionEventStatus newStatus) {
    switch (newStatus) {
    case FAILED:
      return TaskCompletionEvent.Status.FAILED;
    case KILLED:
      return TaskCompletionEvent.Status.KILLED;
    case OBSOLETE:
      return TaskCompletionEvent.Status.OBSOLETE;
    case SUCCEEDED:
      return TaskCompletionEvent.Status.SUCCEEDED;
    case TIPFAILED:
      return TaskCompletionEvent.Status.TIPFAILED;
    }
    throw new YarnException("Unrecognized status: " + newStatus);
  }

  public static org.apache.hadoop.mapred.TaskAttemptID fromYarn(
      TaskAttemptID id) {
    return new org.apache.hadoop.mapred.TaskAttemptID(fromYarn(id.taskID),
        id.id);
  }

  public static TaskAttemptID toYarn(
      org.apache.hadoop.mapred.TaskAttemptID id) {
    TaskAttemptID taskAttemptID = new TaskAttemptID();
    taskAttemptID.taskID = toYarn(id.getTaskID());
    taskAttemptID.id = id.getId();
    return taskAttemptID;
  }

  public static TaskAttemptID toYarn(
      org.apache.hadoop.mapreduce.TaskAttemptID id) {
    TaskAttemptID taskAttemptID = new TaskAttemptID();
    taskAttemptID.taskID = toYarn(id.getTaskID());
    taskAttemptID.id = id.getId();
    return taskAttemptID;
  }
  
  public static org.apache.hadoop.mapreduce.Counters fromYarn(
      Counters yCntrs) {
    org.apache.hadoop.mapreduce.Counters counters = 
      new org.apache.hadoop.mapreduce.Counters();
    for (CounterGroup yGrp : yCntrs.groups.values()) {
      for (Counter yCntr : yGrp.counters.values()) {
        org.apache.hadoop.mapreduce.Counter c = 
          counters.findCounter(yGrp.displayname.toString(), 
              yCntr.displayName.toString());
        c.setValue(yCntr.value);
      }
    }
    return counters;
  }

  public static Counters toYarn(org.apache.hadoop.mapred.Counters counters) {
    Counters yCntrs = new Counters();
    yCntrs.groups = new HashMap<CharSequence, CounterGroup>();
    for (org.apache.hadoop.mapred.Counters.Group grp : counters) {
      CounterGroup yGrp = new CounterGroup();
      yGrp.name = grp.getName();
      yGrp.displayname = grp.getDisplayName();
      yGrp.counters = new HashMap<CharSequence, Counter>();
      for (org.apache.hadoop.mapred.Counters.Counter cntr : grp) {
        Counter yCntr = new Counter();
        yCntr.name = cntr.getName();
        yCntr.displayName = cntr.getDisplayName();
        yCntr.value = cntr.getValue();
        yGrp.counters.put(yCntr.name, yCntr);
      }
      yCntrs.groups.put(yGrp.name, yGrp);
    }
    return yCntrs;
  }

  public static Counters toYarn(org.apache.hadoop.mapreduce.Counters counters) {
    Counters yCntrs = new Counters();
    yCntrs.groups = new HashMap<CharSequence, CounterGroup>();
    for (org.apache.hadoop.mapreduce.CounterGroup grp : counters) {
      CounterGroup yGrp = new CounterGroup();
      yGrp.name = grp.getName();
      yGrp.displayname = grp.getDisplayName();
      yGrp.counters = new HashMap<CharSequence, Counter>();
      for (org.apache.hadoop.mapreduce.Counter cntr : grp) {
        Counter yCntr = new Counter();
        yCntr.name = cntr.getName();
        yCntr.displayName = cntr.getDisplayName();
        yCntr.value = cntr.getValue();
        yGrp.counters.put(yCntr.name, yCntr);
      }
      yCntrs.groups.put(yGrp.name, yGrp);
    }
    return yCntrs;
  }
  
  public static org.apache.hadoop.mapred.JobStatus fromYarn(
      JobReport jobreport, String jobFile, String trackingUrl) {
    String user = null,  jobName = null;
    JobPriority jobPriority = JobPriority.NORMAL;
    return new org.apache.hadoop.mapred.JobStatus(fromYarn(jobreport.id),
        jobreport.setupProgress, jobreport.mapProgress,
        jobreport.reduceProgress, jobreport.cleanupProgress,
        fromYarn(jobreport.state),
        jobPriority, user, jobName, jobFile, trackingUrl);
  }
  
  public static int fromYarn(JobState state) {
    switch (state) {
    case NEW:
      return org.apache.hadoop.mapred.JobStatus.PREP;
    case RUNNING:
      return org.apache.hadoop.mapred.JobStatus.RUNNING;
    case KILL_WAIT:
    case KILLED:
      return org.apache.hadoop.mapred.JobStatus.KILLED;
    case SUCCEEDED:
      return org.apache.hadoop.mapred.JobStatus.SUCCEEDED;
    case FAILED:
    case ERROR:
      return org.apache.hadoop.mapred.JobStatus.FAILED;
    }
    throw new YarnException("Unrecognized job state: " + state);
  }

  public static org.apache.hadoop.mapred.TIPStatus fromYarn(
      TaskState state) {
    switch (state) {
    case NEW:
    case SCHEDULED:
      return org.apache.hadoop.mapred.TIPStatus.PENDING;
    case RUNNING:
      return org.apache.hadoop.mapred.TIPStatus.RUNNING;
    case KILL_WAIT:
    case KILLED:
      return org.apache.hadoop.mapred.TIPStatus.KILLED;
    case SUCCEEDED:
      return org.apache.hadoop.mapred.TIPStatus.COMPLETE;
    case FAILED:
      return org.apache.hadoop.mapred.TIPStatus.FAILED;
    }
    throw new YarnException("Unrecognized task state: " + state);
  }
  
  public static TaskReport fromYarn(org.apache.hadoop.mapreduce.v2.api.TaskReport report) {
      return new TaskReport(fromYarn(report.id), report.progress, report.state.toString(),
      (String[]) report.diagnostics.toArray(), fromYarn(report.state), report.startTime, report.finishTime,
      fromYarn(report.counters));
  }
  
  public static List<TaskReport> fromYarn(
      List<org.apache.hadoop.mapreduce.v2.api.TaskReport> taskReports) {
    List<TaskReport> reports = new ArrayList<TaskReport>();
    for (org.apache.hadoop.mapreduce.v2.api.TaskReport r : taskReports) {
      reports.add(fromYarn(r));
    }
    return reports;
  }
}

