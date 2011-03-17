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

package org.apache.hadoop.mapreduce.v2.app;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.lib.TypeConverter;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.JobState;
import org.apache.hadoop.mapreduce.v2.api.Phase;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskState;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

public class MockJobs extends MockApps {
  static final Iterator<JobState> JOB_STATES = Iterators.cycle(
      JobState.values());
  static final Iterator<TaskState> TASK_STATES = Iterators.cycle(
      TaskState.values());
  static final Iterator<TaskAttemptState> TASK_ATTEMPT_STATES = Iterators.cycle(
      TaskAttemptState.values());
  static final Iterator<TaskType> TASK_TYPES = Iterators.cycle(
      TaskType.values());
  static final Iterator<JobCounter> JOB_COUNTERS = Iterators.cycle(
      JobCounter.values());
  static final Iterator<FileSystemCounter> FS_COUNTERS = Iterators.cycle(
      FileSystemCounter.values());
  static final Iterator<TaskCounter> TASK_COUNTERS = Iterators.cycle(
      TaskCounter.values());
  static final Iterator<String> FS_SCHEMES = Iterators.cycle("FILE", "HDFS",
      "LAFS", "CEPH");
  static final Iterator<String> USER_COUNTER_GROUPS = Iterators.cycle(
      "com.company.project.subproject.component.subcomponent.UserDefinedSpecificSpecialTask$Counters",
      "PigCounters");
  static final Iterator<String> USER_COUNTERS = Iterators.cycle(
      "counter1", "counter2", "counter3");
  static final Iterator<Phase> PHASES = Iterators.cycle(Phase.values());
  static final Iterator<String> DIAGS = Iterators.cycle(
      "Error: java.lang.OutOfMemoryError: Java heap space",
      "Lost task tracker: tasktracker.domain/127.0.0.1:40879");

  public static String newJobName() {
    return newAppName();
  }

  public static Map<JobID, Job> newJobs(ApplicationID appID, int numJobsPerApp,
                                        int numTasksPerJob,
                                        int numAttemptsPerTask) {
    Map<JobID, Job> map = Maps.newHashMap();
    for (int j = 0; j < numJobsPerApp; ++j) {
      Job job = newJob(appID, j, numTasksPerJob, numAttemptsPerTask);
      map.put(job.getID(), job);
    }
    return map;
  }

  public static JobID newJobID(ApplicationID appID, int i) {
    JobID id = new JobID();
    id.appID = appID;
    id.id = i;
    return id;
  }

  public static JobReport newJobReport(JobID id) {
    JobReport report = new JobReport();
    report.id = id;
    report.startTime = System.currentTimeMillis()
        - (int)(Math.random() * 1000000);
    report.finishTime = System.currentTimeMillis()
        + (int)(Math.random() * 1000000) + 1;
    report.mapProgress = (float)Math.random();
    report.reduceProgress = (float)Math.random();
    report.state = JOB_STATES.next();
    return report;
  }

  public static TaskReport newTaskReport(TaskID id) {
    TaskReport report = new TaskReport();
    report.id = id;
    report.startTime = System.currentTimeMillis()
        - (int)(Math.random() * 1000000);
    report.finishTime = System.currentTimeMillis()
        + (int)(Math.random() * 1000000) + 1;
    report.progress = (float)Math.random();
    report.counters = newCounters();
    report.state = TASK_STATES.next();
    return report;
  }

  public static TaskAttemptReport newTaskAttemptReport(TaskAttemptID id) {
    TaskAttemptReport report = new TaskAttemptReport();
    report.id = id;
    report.startTime = System.currentTimeMillis()
        - (int)(Math.random() * 1000000);
    report.finishTime = System.currentTimeMillis()
        + (int)(Math.random() * 1000000) + 1;
    report.phase = PHASES.next();
    report.state = TASK_ATTEMPT_STATES.next();
    report.progress = (float)Math.random();
    report.counters = newCounters();
    return report;
  }

  @SuppressWarnings("deprecation")
  public static Counters newCounters() {
    org.apache.hadoop.mapred.Counters hc =
        new org.apache.hadoop.mapred.Counters();
    for (JobCounter c : JobCounter.values()) {
      hc.findCounter(c).setValue((long)(Math.random() * 1000));
    }
    for (TaskCounter c : TaskCounter.values()) {
      hc.findCounter(c).setValue((long)(Math.random() * 1000));
    }
    int nc = FileSystemCounter.values().length * 4;
    for (int i = 0; i < nc; ++i) {
      for (FileSystemCounter c : FileSystemCounter.values()) {
        hc.findCounter(FS_SCHEMES.next(), c).
            setValue((long)(Math.random() * 1000000));
      }
    }
    for (int i = 0; i < 2 * 3; ++i) {
      hc.findCounter(USER_COUNTER_GROUPS.next(), USER_COUNTERS.next()).
          setValue((long)(Math.random() * 100000));
    }
    return TypeConverter.toYarn(hc);
  }

  public static Map<TaskAttemptID, TaskAttempt> newTaskAttempts(TaskID tid,
                                                                int m) {
    Map<TaskAttemptID, TaskAttempt> map = Maps.newHashMap();
    for (int i = 0; i < m; ++i) {
      TaskAttempt ta = newTaskAttempt(tid, i);
      map.put(ta.getID(), ta);
    }
    return map;
  }

  public static TaskAttempt newTaskAttempt(TaskID tid, int i) {
    final TaskAttemptID taid = new TaskAttemptID();
    taid.taskID = tid;
    taid.id = i;
    final TaskAttemptReport report = newTaskAttemptReport(taid);
    final List<CharSequence> diags = Lists.newArrayList();
    diags.add(DIAGS.next());
    return new TaskAttempt() {
      @Override
      public TaskAttemptID getID() {
        return taid;
      }

      @Override
      public TaskAttemptReport getReport() {
        return report;
      }

      @Override
      public long getLaunchTime() {
        return 0;
      }

      @Override
      public long getFinishTime() {
        return 0;
      }

      @Override
      public Counters getCounters() {
        return report.counters;
      }

      @Override
      public float getProgress() {
        return report.progress;
      }

      @Override
      public TaskAttemptState getState() {
        return report.state;
      }

      @Override
      public boolean isFinished() {
        switch (report.state) {
          case SUCCEEDED:
          case FAILED:
          case KILLED: return true;
        }
        return false;
      }

      @Override
      public ContainerID getAssignedContainerID() {
        ContainerID id = new ContainerID();
        id.appID = taid.taskID.jobID.appID;
        return id;
      }

      @Override
      public String getAssignedContainerMgrAddress() {
        return "localhost";
      }

      @Override
      public List<CharSequence> getDiagnostics() {
        return diags;
      }
    };
  }

  public static Map<TaskID, Task> newTasks(JobID jid, int n, int m) {
    Map<TaskID, Task> map = Maps.newHashMap();
    for (int i = 0; i < n; ++i) {
      Task task = newTask(jid, i, m);
      map.put(task.getID(), task);
    }
    return map;
  }

  public static Task newTask(JobID jid, int i, int m) {
    final TaskID tid = new TaskID();
    tid.jobID = jid;
    tid.id = i;
    tid.taskType = TASK_TYPES.next();
    final TaskReport report = newTaskReport(tid);
    final Map<TaskAttemptID, TaskAttempt> attempts = newTaskAttempts(tid, m);
    return new Task() {
      @Override
      public TaskID getID() {
        return tid;
      }

      @Override
      public TaskReport getReport() {
        return report;
      }

      @Override
      public Counters getCounters() {
        return report.counters;
      }

      @Override
      public float getProgress() {
        return report.progress;
      }

      @Override
      public TaskType getType() {
        return tid.taskType;
      }

      @Override
      public Map<TaskAttemptID, TaskAttempt> getAttempts() {
        return attempts;
      }

      @Override
      public TaskAttempt getAttempt(TaskAttemptID attemptID) {
        return attempts.get(attemptID);
      }

      @Override
      public boolean isFinished() {
        switch (report.state) {
          case SUCCEEDED:
          case KILLED:
          case FAILED: return true;
        }
        return false;
      }

      @Override
      public boolean canCommit(TaskAttemptID taskAttemptID) {
        return false;
      }

      @Override
      public TaskState getState() {
        return report.state;
      }
    };
  }

  public static Counters getCounters(Collection<Task> tasks) {
    Counters counters = JobImpl.newCounters();
    return JobImpl.incrTaskCounters(counters, tasks);
  }

  static class TaskCount {
    int maps;
    int reduces;
    int completedMaps;
    int completedReduces;

    void incr(Task task) {
      TaskType type = task.getType();
      boolean finished = task.isFinished();
      if (type == TaskType.MAP) {
        if (finished) {
          ++completedMaps;
        }
        ++maps;
      } else if (type == TaskType.REDUCE) {
        if (finished) {
          ++completedReduces;
        }
        ++reduces;
      }
    }
  }

  static TaskCount getTaskCount(Collection<Task> tasks) {
    TaskCount tc = new TaskCount();
    for (Task task : tasks) {
      tc.incr(task);
    }
    return tc;
  }

  public static Job newJob(ApplicationID appID, int i, int n, int m) {
    final JobID id = newJobID(appID, i);
    final String name = newJobName();
    final JobReport report = newJobReport(id);
    final Map<TaskID, Task> tasks = newTasks(id, n, m);
    final TaskCount taskCount = getTaskCount(tasks.values());
    final Counters counters = getCounters(tasks.values());
    return new Job() {
      @Override
      public JobID getID() {
        return id;
      }

      @Override
      public CharSequence getName() {
        return name;
      }

      @Override
      public JobState getState() {
        return report.state;
      }

      @Override
      public JobReport getReport() {
        return report;
      }

      @Override
      public Counters getCounters() {
        return counters;
      }

      @Override
      public Map<TaskID, Task> getTasks() {
        return tasks;
      }

      @Override
      public Task getTask(TaskID taskID) {
        return tasks.get(taskID);
      }

      @Override
      public int getTotalMaps() {
        return taskCount.maps;
      }

      @Override
      public int getTotalReduces() {
        return taskCount.reduces;
      }

      @Override
      public int getCompletedMaps() {
        return taskCount.completedMaps;
      }

      @Override
      public int getCompletedReduces() {
        return taskCount.completedReduces;
      }

      @Override
      public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(int fromEventId,
                                                           int maxEvents) {
        return null;
      }

      @Override
      public Map<TaskID, Task> getTasks(TaskType taskType) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public List<String> getDiagnostics() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
  }
}
