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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleanupEvent;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.JobState;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.TaskState;

/**
 * Mock MRAppMaster. Doesn't start RPC servers.
 * No threads are started except of the event Dispatcher thread.
 */
public class MRApp extends MRAppMaster {

  int maps;
  int reduces;

  //if true tasks complete automatically as soon as they are launched
  protected boolean autoComplete = false;

  public MRApp(int maps, int reduces, boolean autoComplete) {
    super(new ApplicationID());
    this.maps = maps;
    this.reduces = reduces;
    this.autoComplete = autoComplete;
  }

  public Job submit(Configuration conf) throws Exception {
    String user = conf.get(MRJobConfig.USER_NAME, "mapred");
    conf.set(MRJobConfig.USER_NAME, user);
    init(conf);
    start();
    Job job = getContext().getAllJobs().values().iterator().next();
    return job;
  }

  public void waitForState(TaskAttempt attempt, 
      TaskAttemptState finalState) throws Exception {
    int timeoutSecs = 0;
    TaskAttemptReport report = attempt.getReport();
    while (!finalState.equals(report.state) &&
        timeoutSecs++ < 20) {
      System.out.println("TaskAttempt State is : " + report.state +
          " Waiting for state : " + finalState +
          "   progress : " + report.progress);
      report = attempt.getReport();
      Thread.sleep(500);
    }
    System.out.println("TaskAttempt State is : " + report.state);
    Assert.assertEquals("TaskAttempt state is not correct (timedout)",
        finalState, 
        report.state);
  }

  public void waitForState(Task task, TaskState finalState) throws Exception {
    int timeoutSecs = 0;
    TaskReport report = task.getReport();
    while (!finalState.equals(report.state) &&
        timeoutSecs++ < 20) {
      System.out.println("Task State is : " + report.state +
          " Waiting for state : " + finalState +
          "   progress : " + report.progress);
      report = task.getReport();
      Thread.sleep(500);
    }
    System.out.println("Task State is : " + report.state);
    Assert.assertEquals("Task state is not correct (timedout)", finalState, 
        report.state);
  }

  public void waitForState(Job job, JobState finalState) throws Exception {
    int timeoutSecs = 0;
    JobReport report = job.getReport();
    while (!finalState.equals(report.state) &&
        timeoutSecs++ < 20) {
      System.out.println("Job State is : " + report.state +
          " Waiting for state : " + finalState +
          "   map progress : " + report.mapProgress + 
          "   reduce progress : " + report.reduceProgress);
      report = job.getReport();
      Thread.sleep(500);
    }
    System.out.println("Job State is : " + report.state);
    Assert.assertEquals("Job state is not correct (timedout)", finalState, 
        job.getState());
  }

  public void verifyCompleted() {
    for (Job job : getContext().getAllJobs().values()) {
      JobReport jobReport = job.getReport();
      Assert.assertTrue("Job start time is  not less than finish time",
          jobReport.startTime < jobReport.finishTime);
      Assert.assertTrue("Job finish time is in future",
          jobReport.finishTime < System.currentTimeMillis());
      for (Task task : job.getTasks().values()) {
        TaskReport taskReport = task.getReport();
        Assert.assertTrue("Task start time is  not less than finish time",
            taskReport.startTime < taskReport.finishTime);
        for (TaskAttempt attempt : task.getAttempts().values()) {
          TaskAttemptReport attemptReport = attempt.getReport();
          Assert.assertTrue("Attempt start time is  not less than finish time",
              attemptReport.startTime < attemptReport.finishTime);
        }
      }
    }
  }

  protected void startJobs() {
    Job job = new TestJob(getAppID(), getDispatcher().getEventHandler(),
        getTaskAttemptListener());
    ((AppContext) getContext()).getAllJobs().put(job.getID(), job);

    getDispatcher().register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        createJobHistoryHandler(getConfig()));
    getDispatcher().register(JobFinishEvent.Type.class,
        new EventHandler<JobFinishEvent>() {
          @Override
          public void handle(JobFinishEvent event) {
            stop();
          }
        });
    
    /** create a job event for job intialization **/
    JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    /** send init on the job. this triggers the job execution.**/
    getDispatcher().getEventHandler().handle(initJobEvent);
  }

  @Override
  protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    return new TaskAttemptListener(){
      @Override
      public InetSocketAddress getAddress() {
        return null;
      }
      @Override
      public void register(TaskAttemptID attemptID, 
          org.apache.hadoop.mapred.Task task, WrappedJvmID jvmID) {}
      @Override
      public void unregister(TaskAttemptID attemptID, WrappedJvmID jvmID) {
      }
    };
  }

  @Override
  protected ContainerLauncher createContainerLauncher(AppContext context) {
    return new MockContainerLauncher();
  }

  class MockContainerLauncher implements ContainerLauncher {
    @Override
    public void handle(ContainerLauncherEvent event) {
      switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getTaskAttemptID(),
                TaskAttemptEventType.TA_CONTAINER_LAUNCHED));
        
        attemptLaunched(event.getTaskAttemptID());
        break;
      case CONTAINER_REMOTE_CLEANUP:
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getTaskAttemptID(),
                TaskAttemptEventType.TA_CONTAINER_CLEANED));
        break;
      }
    }
  }

  protected void attemptLaunched(TaskAttemptID attemptID) {
    if (autoComplete) {
      // send the done event
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptID,
              TaskAttemptEventType.TA_DONE));
    }
  }

  @Override
  protected ContainerAllocator createContainerAllocator(
      ClientService clientService, AppContext context) {
    return new ContainerAllocator(){
      private int containerCount;
      @Override
      public void handle(ContainerAllocatorEvent event) {
        ContainerID cId = new ContainerID();
        cId.appID = getContext().getApplicationID();
        cId.id = containerCount++;
        getContext().getEventHandler().handle(
            new TaskAttemptContainerAssignedEvent(event.getAttemptID(), cId,
                "dummy", null));
      }
    };
  }

  @Override
  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleaner() {
      @Override
      public void handle(TaskCleanupEvent event) {
        //send the cleanup done event
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getAttemptID(),
                TaskAttemptEventType.TA_CLEANUP_DONE));
      }
    };
  }

  @Override
  protected ClientService createClientService(AppContext context) {
    return new ClientService(){
      @Override
      public InetSocketAddress getBindAddress() {
        return null;
      }

      @Override
      public int getHttpPort() {
        return -1;
      }
    };
  }

  class TestJob extends JobImpl {
    //overwrite the init transition
    StateMachineFactory<JobImpl, JobState, JobEventType, JobEvent> localFactory
         //overwrite the init transition
         = stateMachineFactory.addTransition
                 (JobState.NEW,
                  EnumSet.of(JobState.RUNNING, JobState.FAILED),
                  JobEventType.JOB_INIT,
                  // This is abusive.
                  new TestInitTransition(getConfig(), maps, reduces));

    private final StateMachine<JobState, JobEventType, JobEvent>
           localStateMachine;
    
    @Override
    protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
      return localStateMachine;
    }

    public TestJob(ApplicationID appID, EventHandler eventHandler,
        TaskAttemptListener taskAttemptListener) {
      super(appID, new Configuration(), eventHandler, taskAttemptListener,
          new JobTokenSecretManager(), new Credentials());

      // This "this leak" is okay because the retained pointer is in an
      //  instance variable.
      localStateMachine = localFactory.make(this);
    }
    
  }
  
  //Override InitTransition to not look for split files etc
  static class TestInitTransition extends JobImpl.InitTransition {
    private Configuration config;
    private int maps;
    private int reduces;
    TestInitTransition(Configuration config, int maps, int reduces) {
      this.config = config;
      this.maps = maps;
      this.reduces = reduces;
    }
    @Override
    protected void setup(JobImpl job) throws IOException {
      job.conf = config;
      job.conf.setInt(MRJobConfig.NUM_REDUCES, reduces);
      job.remoteJobConfFile = new Path("test");
    }
    @Override
    protected TaskSplitMetaInfo[] createSplits(JobImpl job, JobID jobId) {
      TaskSplitMetaInfo[] splits = new TaskSplitMetaInfo[maps];
      for (int i = 0; i < maps ; i++) {
        splits[i] = new TaskSplitMetaInfo();
      }
      return splits;
    }
  }

}
 
