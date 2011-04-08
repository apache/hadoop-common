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

package org.apache.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class TypeConverter {

  public static org.apache.hadoop.mapred.JobID fromYarn(JobId id) {
    String identifier = fromClusterTimeStamp(id.getAppId().getClusterTimestamp());
    return new org.apache.hadoop.mapred.JobID(identifier, id.getId());
  }

  //currently there is 1-1 mapping between appid and jobid
  public static org.apache.hadoop.mapreduce.JobID fromYarn(ApplicationId appID) {
    String identifier = fromClusterTimeStamp(appID.getClusterTimestamp());
    return new org.apache.hadoop.mapred.JobID(identifier, appID.getId());
  }

  public static JobId toYarn(org.apache.hadoop.mapreduce.JobID id) {
    JobId jobId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class);
    jobId.setId(id.getId()); //currently there is 1-1 mapping between appid and jobid
    
    ApplicationId appId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class);
    appId.setId(id.getId());
    appId.setClusterTimestamp(toClusterTimeStamp(id.getJtIdentifier()));
    jobId.setAppId(appId);
    return jobId;
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

  public static org.apache.hadoop.mapred.TaskID fromYarn(TaskId id) {
    return new org.apache.hadoop.mapred.TaskID(fromYarn(id.getJobId()), fromYarn(id.getTaskType()),
        id.getId());
  }

  public static TaskId toYarn(org.apache.hadoop.mapreduce.TaskID id) {
    TaskId taskId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class);
    taskId.setId(id.getId());
    taskId.setTaskType(toYarn(id.getTaskType()));
    taskId.setJobId(toYarn(id.getJobID()));
    return taskId;
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
      TaskAttemptCompletionEvent[] newEvents) {
    TaskCompletionEvent[] oldEvents =
        new TaskCompletionEvent[newEvents.length];
    int i = 0;
    for (TaskAttemptCompletionEvent newEvent 
        : newEvents) {
      oldEvents[i++] = fromYarn(newEvent);
    }
    return oldEvents;
  }

  public static TaskCompletionEvent fromYarn(
      TaskAttemptCompletionEvent newEvent) {
    return new TaskCompletionEvent(newEvent.getEventId(),
              fromYarn(newEvent.getAttemptId()), newEvent.getAttemptId().getId(),
              newEvent.getAttemptId().getTaskId().getTaskType().equals(TaskType.MAP),
              fromYarn(newEvent.getStatus()),
              newEvent.getMapOutputServerAddress());
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
      TaskAttemptId id) {
    return new org.apache.hadoop.mapred.TaskAttemptID(fromYarn(id.getTaskId()),
        id.getId());
  }

  public static TaskAttemptId toYarn(
      org.apache.hadoop.mapred.TaskAttemptID id) {
    TaskAttemptId taskAttemptId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taskAttemptId.setTaskId(toYarn(id.getTaskID()));
    taskAttemptId.setId(id.getId());
    return taskAttemptId;
  }

  public static TaskAttemptId toYarn(
      org.apache.hadoop.mapreduce.TaskAttemptID id) {
    TaskAttemptId taskAttemptId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taskAttemptId.setTaskId(toYarn(id.getTaskID()));
    taskAttemptId.setId(id.getId());
    return taskAttemptId;
  }
  
  public static org.apache.hadoop.mapreduce.Counters fromYarn(
      Counters yCntrs) {
    org.apache.hadoop.mapreduce.Counters counters = 
      new org.apache.hadoop.mapreduce.Counters();
    for (CounterGroup yGrp : yCntrs.getAllCounterGroups().values()) {
      for (Counter yCntr : yGrp.getAllCounters().values()) {
        org.apache.hadoop.mapreduce.Counter c = 
          counters.findCounter(yGrp.getDisplayName(), 
              yCntr.getDisplayName());
        c.setValue(yCntr.getValue());
      }
    }
    return counters;
  }

  public static Counters toYarn(org.apache.hadoop.mapred.Counters counters) {
    Counters yCntrs = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Counters.class);
    yCntrs.addAllCounterGroups(new HashMap<String, CounterGroup>());
    for (org.apache.hadoop.mapred.Counters.Group grp : counters) {
      CounterGroup yGrp = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(CounterGroup.class);
      yGrp.setName(grp.getName());
      yGrp.setDisplayName(grp.getDisplayName());
      yGrp.addAllCounters(new HashMap<String, Counter>());
      for (org.apache.hadoop.mapred.Counters.Counter cntr : grp) {
        Counter yCntr = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Counter.class);
        yCntr.setName(cntr.getName());
        yCntr.setDisplayName(cntr.getDisplayName());
        yCntr.setValue(cntr.getValue());
        yGrp.setCounter(yCntr.getName(), yCntr);
      }
      yCntrs.setCounterGroup(yGrp.getName(), yGrp);
    }
    return yCntrs;
  }

  public static Counters toYarn(org.apache.hadoop.mapreduce.Counters counters) {
    Counters yCntrs = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Counters.class);
    yCntrs.addAllCounterGroups(new HashMap<String, CounterGroup>());
    for (org.apache.hadoop.mapreduce.CounterGroup grp : counters) {
      CounterGroup yGrp = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(CounterGroup.class);
      yGrp.setName(grp.getName());
      yGrp.setDisplayName(grp.getDisplayName());
      yGrp.addAllCounters(new HashMap<String, Counter>());
      for (org.apache.hadoop.mapreduce.Counter cntr : grp) {
        Counter yCntr = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Counter.class);
        yCntr.setName(cntr.getName());
        yCntr.setDisplayName(cntr.getDisplayName());
        yCntr.setValue(cntr.getValue());
        yGrp.setCounter(yCntr.getName(), yCntr);
      }
      yCntrs.setCounterGroup(yGrp.getName(), yGrp);
    }
    return yCntrs;
  }
  
  public static org.apache.hadoop.mapred.JobStatus fromYarn(
      JobReport jobreport, String jobFile, String trackingUrl) {
    String user = null,  jobName = null;
    JobPriority jobPriority = JobPriority.NORMAL;
    return new org.apache.hadoop.mapred.JobStatus(fromYarn(jobreport.getJobId()),
        jobreport.getSetupProgress(), jobreport.getMapProgress(),
        jobreport.getReduceProgress(), jobreport.getCleanupProgress(),
        fromYarn(jobreport.getJobState()),
        jobPriority, user, jobName, jobFile, trackingUrl);
  }
  
  public static int fromYarn(JobState state) {
    switch (state) {
    case NEW:
    case INITED:
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
  
  public static TaskReport fromYarn(org.apache.hadoop.mapreduce.v2.api.records.TaskReport report) {
    String[] diagnostics = null;
    if (report.getDiagnosticsList() != null) {
      diagnostics = new String[report.getDiagnosticsCount()];
      int i = 0;
      for (String cs : report.getDiagnosticsList()) {
        diagnostics[i++] = cs.toString();
      }
    } else {
      diagnostics = new String[0];
    }
    
    TaskReport rep = new TaskReport(fromYarn(report.getTaskId()), 
        report.getProgress(), report.getTaskState().toString(),
      diagnostics, fromYarn(report.getTaskState()), report.getStartTime(), report.getFinishTime(),
      fromYarn(report.getCounters()));
    List<org.apache.hadoop.mapreduce.TaskAttemptID> runningAtts 
          = new ArrayList<org.apache.hadoop.mapreduce.TaskAttemptID>();
    for (org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId id 
        : report.getRunningAttemptsList()) {
      runningAtts.add(fromYarn(id));
    }
    rep.setRunningTaskAttemptIds(runningAtts);
    if (report.getSuccessfulAttempt() != null) {
      rep.setSuccessfulAttemptId(fromYarn(report.getSuccessfulAttempt()));
    }
    return rep;
  }
  
  public static List<TaskReport> fromYarn(
      List<org.apache.hadoop.mapreduce.v2.api.records.TaskReport> taskReports) {
    List<TaskReport> reports = new ArrayList<TaskReport>();
    for (org.apache.hadoop.mapreduce.v2.api.records.TaskReport r : taskReports) {
      reports.add(fromYarn(r));
    }
    return reports;
  }
}

