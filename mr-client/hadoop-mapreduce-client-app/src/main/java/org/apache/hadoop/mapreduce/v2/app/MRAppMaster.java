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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleanerImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.mapreduce.v2.api.JobID;

/**
 * The Map-Reduce Application Master.
 * The state machine is encapsulated in the implementation of Job interface.
 * All state changes happens via Job interface. Each event 
 * results in a Finite State Transition in Job.
 * 
 * MR AppMaster is the composition of loosely coupled services. The services 
 * interact with each other via events. The components resembles the 
 * Actors model. The component acts on received event and send out the 
 * events to other components.
 * This keeps it highly concurrent with no or minimal synchronization needs.
 * 
 * The events are dispatched by a central Dispatch mechanism. All components
 * register to the Dispatcher.
 * 
 * The information is shared across different components using AppContext.
 */

public class MRAppMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(MRAppMaster.class);

  private final Clock clock;

  private ApplicationID appID;
  private AppContext context;
  private Dispatcher dispatcher;
  private ClientService clientService;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private TaskCleaner taskCleaner;
  private Speculator speculator;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();

  public MRAppMaster(ApplicationID applicationId) {
    this(applicationId, null);
  }

  public MRAppMaster(ApplicationID applicationId, Clock clock) {
    super(MRAppMaster.class.getName());
    if (clock == null) {
      clock = new Clock();
    }
    this.clock = clock;
    this.appID = applicationId;
    LOG.info("Created MRAppMaster for application " + applicationId);
  }

  @Override
  public void init(final Configuration conf) {
    context = new RunningAppContext(); 

    dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context);
    addIfService(taskAttemptListener);

    //service to do the task cleanup
    taskCleaner = createTaskCleaner(context);
    addIfService(taskCleaner);
    //service to launch allocated containers via NodeManager
    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher);
    
    //service to handle requests from JobClient
    clientService = createClientService(context);
    addIfService(clientService);

    //service to allocate containers from RM
    containerAllocator = createContainerAllocator(clientService, context);
    addIfService(containerAllocator);

    //register the event dispatchers
    dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);
    dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);
    dispatcher.register(JobEventType.class, new JobEventDispatcher());
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, 
        new TaskAttemptEventDispatcher());
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);
    
    if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
        || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
      //optional service to speculate on task attempts' progress
      speculator = createSpeculator(conf, context);
      addIfService(speculator);
      dispatcher.register(Speculator.EventType.class, speculator);
    } else {
      dispatcher.register
          (Speculator.EventType.class, new NullSpeculatorEventHandler());
    }

    super.init(conf);
  }

  static class NullSpeculatorEventHandler
      implements EventHandler<SpeculatorEvent> {
    @Override
    public void handle(SpeculatorEvent event) {
      // no code
    }
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      Configuration conf) {
    return new EventHandler<JobHistoryEvent>() {
      @Override
      public void handle(JobHistoryEvent event) {
      }
    };
    //TODO use the real job history handler.
    //return new JobHistoryEventHandler(conf);
  }

  protected Speculator createSpeculator(Configuration conf, AppContext context) {
    Class<? extends Speculator> speculatorClass;

    try {
      speculatorClass
          // "yarn.mapreduce.job.speculator.class"
          = conf.getClass(YarnMRJobConfig.SPECULATOR_CLASS,
                          DefaultSpeculator.class,
                          Speculator.class);
      Constructor<? extends Speculator> speculatorConstructor
          = speculatorClass.getConstructor
               (Configuration.class, AppContext.class);
      Speculator result = speculatorConstructor.newInstance(conf, context);

      return result;
    } catch (InstantiationException ex) {
      LOG.error("Can't make a speculator -- check "
          + YarnMRJobConfig.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    } catch (IllegalAccessException ex) {
      LOG.error("Can't make a speculator -- check "
          + YarnMRJobConfig.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    } catch (InvocationTargetException ex) {
      LOG.error("Can't make a speculator -- check "
          + YarnMRJobConfig.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    } catch (NoSuchMethodException ex) {
      LOG.error("Can't make a speculator -- check "
          + YarnMRJobConfig.SPECULATOR_CLASS + " " + ex);
      throw new YarnException(ex);
    }
  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpl(context, jobTokenSecretManager);
    return lis;
  }

  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleanerImpl(context);
  }

  protected ContainerLauncher createContainerLauncher(AppContext context) {
    return new ContainerLauncherImpl(context);
  }
  
  protected ContainerAllocator createContainerAllocator(ClientService
      clientService, AppContext context) {
    //return new StaticContainerAllocator(context);
    return new RMContainerAllocator(clientService, context);
  }

  //TODO:should have an interface for MRClientService
  protected ClientService createClientService(AppContext context) {
    return new MRClientService(context);
  }

  public ApplicationID getAppID() {
    return appID;
  }

  public AppContext getContext() {
    return context;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  //Returns null if speculation is not enabled
  public Speculator getSpeculator() {
    return speculator;
  }

  public ContainerAllocator getContainerAllocator() {
    return containerAllocator;
  }
  
  public ContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }

  public TaskAttemptListener getTaskAttemptListener() {
    return taskAttemptListener;
  }

  class RunningAppContext implements AppContext {

    private Map<JobID, Job> jobs = new ConcurrentHashMap<JobID, Job>();
   
    @Override
    public ApplicationID getApplicationID() {
      return appID;
    }

    @Override
    public Job getJob(JobID jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobID, Job> getAllJobs() {
      return jobs;
    }

    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    @Override
    public CharSequence getUser() {
      return getConfig().get(MRJobConfig.USER_NAME);
    }

  }

  @Override
  public void start() {
    startJobs();
    //start all the components
    super.start();
  }

  /**
   * This can be overridden to instantiate multiple jobs and create a 
   * workflow.
   */
  protected void startJobs() {

    Configuration config = getConfig();

    Credentials fsTokens = new Credentials();

    if (UserGroupInformation.isSecurityEnabled()) {
      // Read the file-system tokens from the localized tokens-file.
      try {
        Path jobSubmitDir =
            FileContext.getLocalFSFileContext().makeQualified(
                new Path(new File(YARNApplicationConstants.JOB_SUBMIT_DIR)
                    .getAbsolutePath()));
        Path jobTokenFile =
            new Path(jobSubmitDir, YarnConfiguration.APPLICATION_TOKENS_FILE);
        fsTokens.addAll(Credentials.readTokenStorageFile(jobTokenFile, config));
        LOG.info("jobSubmitDir=" + jobSubmitDir + " jobTokenFile="
            + jobTokenFile);

        UserGroupInformation currentUser =
            UserGroupInformation.getCurrentUser();
        for (Token<? extends TokenIdentifier> tk : fsTokens.getAllTokens()) {
          LOG.info(" --- DEBUG: Token of kind " + tk.getKind()
              + "in current ugi in the AppMaster for service "
              + tk.getService());
          currentUser.addToken(tk); // For use by AppMaster itself.
        }
      } catch (IOException e) {
        throw new YarnException(e);
      }
    }

    //create single job
    Job job =
        new JobImpl(appID, config, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, fsTokens);
    ((RunningAppContext) context).jobs.put(job.getID(), job);

    dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        createJobHistoryHandler(config));
    dispatcher.register(JobFinishEvent.Type.class,
        new EventHandler<JobFinishEvent>() {
          @Override
          public void handle(JobFinishEvent event) {
            // job has finished
            // this is the only job, so shutdown the Appmaster
            // note in a workflow scenario, this may lead to creation of a new
            // job

            // TODO:currently just wait for sometime so clients can know the
            // final states. Will be removed once RM come on.
            try {
              Thread.sleep(15000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            LOG.info("Calling stop for all the services");
            try {
              stop();
            } catch (Throwable t) {
              LOG.warn("Graceful stop failed ", t);
            }
            //TODO: this is required because rpc server does not shutdown
            //inspite of calling server.stop().
            //Bring the process down by force.
            //Not needed after HADOOP-7140
            LOG.info("Exiting MR AppMaster..GoodBye!");
            System.exit(0);
          }
        });

    /** create a job event for job intialization **/
    JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    /** send init on the job. this triggers the job execution.**/
    dispatcher.getEventHandler().handle(initJobEvent);
  }

  private class JobEventDispatcher implements EventHandler<JobEvent> {
    @Override
    public void handle(JobEvent event) {
      ((EventHandler<JobEvent>)context.getJob(event.getJobId())).handle(event);
    }
  }  
  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @Override
    public void handle(TaskEvent event) {
      Task task = context.getJob(event.getTaskID().jobID).getTask(
          event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher 
          implements EventHandler<TaskAttemptEvent> {
    @Override
    public void handle(TaskAttemptEvent event) {
      Job job = context.getJob(event.getTaskAttemptID().taskID.jobID);
      Task task = job.getTask(event.getTaskAttemptID().taskID);
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }

  public static void main(String[] args) {
    try {
      //Configuration.addDefaultResource("job.xml");
      ApplicationID applicationId = new ApplicationID();
      applicationId.clusterTimeStamp = Long.valueOf(args[0]);
      applicationId.id = Integer.valueOf(args[1]);
      MRAppMaster appMaster = new MRAppMaster(applicationId);
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      conf.addResource(new Path(YARNApplicationConstants.JOB_CONF_FILE));
      conf.set(MRJobConfig.USER_NAME, 
          System.getProperty("user.name")); 
      UserGroupInformation.setConfiguration(conf);
      appMaster.init(conf);
      appMaster.start();
    } catch (Throwable t) {
      LOG.error("Caught throwable. Exiting:", t);
      System.exit(1);
    }
  } 
}
