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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.SNEventType;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * This class manages the state of a application master. Also, it
 * provide a read only interface for all the services to get information
 * about this application.
 *
 */
@Private
@Unstable
public class ApplicationMasterInfo implements AppContext, EventHandler<ASMEvent<ApplicationEventType>> {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterInfo.class);
  private final ApplicationSubmissionContext submissionContext;
  private ApplicationMaster master;
  private final EventHandler handler;
  private Container masterContainer;
  final private String user;
  private int numFailed = 0;
  /* the list of nodes that this AM was launched on */
  List<String> hostNamesLaunched = new ArrayList<String>();
  /* this transition is too generalized, needs to be broken up as and when we 
   * keeping adding states. This will keep evolving and is not final yet.
   */
  private final  KillTransition killTransition =  new KillTransition();
  private final StatusUpdateTransition statusUpdatetransition = new StatusUpdateTransition();
  private final ExpireTransition expireTransition = new ExpireTransition();
  private final FailedTransition failedTransition = new FailedTransition();
  private final AllocateTransition allocateTransition = new AllocateTransition();
  
  private final StateMachine<ApplicationState, ApplicationEventType, 
  ASMEvent<ApplicationEventType>> stateMachine;

  private final StateMachineFactory<ApplicationMasterInfo,
  ApplicationState, ApplicationEventType, ASMEvent<ApplicationEventType>> stateMachineFactory 

  = new StateMachineFactory
  <ApplicationMasterInfo, ApplicationState, ApplicationEventType, ASMEvent<ApplicationEventType>>
  (ApplicationState.PENDING)

  .addTransition(ApplicationState.PENDING, ApplicationState.ALLOCATING,
  ApplicationEventType.ALLOCATE, allocateTransition)
  
  .addTransition(ApplicationState.EXPIRED_PENDING, ApplicationState.ALLOCATING, 
  ApplicationEventType.ALLOCATE, allocateTransition)
  
  .addTransition(ApplicationState.PENDING, ApplicationState.CLEANUP, 
  ApplicationEventType.KILL, killTransition)

  .addTransition(ApplicationState.ALLOCATING, ApplicationState.ALLOCATED,
  ApplicationEventType.ALLOCATED, new AllocatedTransition())

  .addTransition(ApplicationState.ALLOCATING, ApplicationState.CLEANUP, 
  ApplicationEventType.KILL, killTransition)

  .addTransition(ApplicationState.ALLOCATED, ApplicationState.CLEANUP, 
  ApplicationEventType.KILL, killTransition)

  .addTransition(ApplicationState.ALLOCATED, ApplicationState.LAUNCHING,
  ApplicationEventType.LAUNCH, new LaunchTransition())

  .addTransition(ApplicationState.LAUNCHING, ApplicationState.LAUNCHED,
  ApplicationEventType.LAUNCHED, new LaunchedTransition())
  
  .addTransition(ApplicationState.LAUNCHING, ApplicationState.KILLED,
   ApplicationEventType.KILL, killTransition)
   
  .addTransition(ApplicationState.LAUNCHED, ApplicationState.CLEANUP, 
  ApplicationEventType.KILL, killTransition)
  
  .addTransition(ApplicationState.LAUNCHED, ApplicationState.FAILED,
  ApplicationEventType.EXPIRE, expireTransition)
  
  .addTransition(ApplicationState.LAUNCHED, ApplicationState.RUNNING, 
  ApplicationEventType.REGISTERED, new RegisterTransition())
  
  /* for now we assume that acting on expiry is synchronous and we do not 
   * have to wait for cleanup acks from scheduler negotiator and launcher.
   */
  .addTransition(ApplicationState.LAUNCHED, ApplicationState.EXPIRED_PENDING,
      ApplicationEventType.EXPIRE, expireTransition)
      
  .addTransition(ApplicationState.RUNNING,  ApplicationState.EXPIRED_PENDING, 
  ApplicationEventType.EXPIRE, expireTransition)
  
  .addTransition(ApplicationState.EXPIRED_PENDING, ApplicationState.FAILED,
      ApplicationEventType.FAILED_MAX_RETRIES, failedTransition)
      
  .addTransition(ApplicationState.RUNNING, ApplicationState.COMPLETED,
  ApplicationEventType.FINISH, new DoneTransition())

  .addTransition(ApplicationState.RUNNING, ApplicationState.RUNNING,
  ApplicationEventType.STATUSUPDATE, statusUpdatetransition)

  .addTransition(ApplicationState.COMPLETED, ApplicationState.COMPLETED, 
  ApplicationEventType.EXPIRE)

  .installTopology();



  public ApplicationMasterInfo(EventHandler handler, String user,
  ApplicationSubmissionContext submissionContext, String clientToken) {
    this.user = user;
    this.handler = handler;
    this.submissionContext = submissionContext;
    master = new ApplicationMaster();
    master.applicationId = submissionContext.applicationId;
    master.status = new ApplicationStatus();
    master.status.applicationId = submissionContext.applicationId;
    master.status.progress = -1.0f;
    stateMachine = stateMachineFactory.make(this);
    master.state = ApplicationState.PENDING;
    master.clientToken = clientToken;
  }

  @Override
  public ApplicationSubmissionContext getSubmissionContext() {
    return submissionContext;
  }

  @Override
  public Resource getResource() {
    return submissionContext.masterCapability;
  }

  @Override
  public synchronized ApplicationID getApplicationID() {
    return this.master.applicationId;
  }

  @Override
  public synchronized ApplicationStatus getStatus() {
    return master.status;
  }

  public synchronized void updateStatus(ApplicationStatus status) {
    this.master.status = status;
  }

  @Override
  public synchronized ApplicationMaster getMaster() {
    return master;
  }

  /* make sure the master state is in sync with statemachine state */
  public synchronized ApplicationState getState() {
    return master.state;
  }

  @Override
  public synchronized Container getMasterContainer() {
    return masterContainer;
  }


  @Override
  public String getUser() {
    return this.user;
  }

  @Override
  public synchronized long getLastSeen() {
    return this.master.status.lastSeen;
  }

  @Override
  public synchronized int getFailedCount() {
    return numFailed;
  }
  
  @Override
  public String getName() {
    return submissionContext.applicationName.toString();
  }

  @Override
  public String getQueue() {
    return submissionContext.queue.toString();
  }
  
  /* the applicaiton master completed successfully */
  private static class DoneTransition implements 
    SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {

    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      masterInfo.handler.handle(new ASMEvent<SNEventType>(
        SNEventType.CLEANUP, masterInfo));
      masterInfo.handler.handle(new ASMEvent<AMLauncherEventType>(
        AMLauncherEventType.CLEANUP, masterInfo));
      masterInfo.handler.handle(new ASMEvent<ApplicationTrackerEventType>(
      ApplicationTrackerEventType.REMOVE, masterInfo));
    }
  }
  
  private static class KillTransition implements
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {
    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      masterInfo.handler.handle(new ASMEvent<SNEventType>(SNEventType.CLEANUP, masterInfo));
      masterInfo.handler.handle(new ASMEvent<AMLauncherEventType>(AMLauncherEventType.CLEANUP, masterInfo));
      masterInfo.handler.handle(new ASMEvent<ApplicationTrackerEventType>(ApplicationTrackerEventType.REMOVE,
          masterInfo));
    }
  }

  private static class LaunchTransition implements
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {
    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      masterInfo.handler.handle(new ASMEvent<AMLauncherEventType>(
      AMLauncherEventType.LAUNCH, masterInfo));
    }
  }

  private static class LaunchedTransition implements
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {
    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      /* make sure the time stamp is update else expiry thread will expire this */
      masterInfo.master.status.lastSeen = System.currentTimeMillis();
    }
  }

  private static class  ExpireTransition implements
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {
    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      /* for now this is the same as killed transition but will change later */
      masterInfo.handler.handle(new ASMEvent<SNEventType>(SNEventType.CLEANUP,
        masterInfo));
      masterInfo.handler.handle(new ASMEvent<AMLauncherEventType>(
        AMLauncherEventType.CLEANUP, masterInfo));
      masterInfo.handler.handle(new ASMEvent<ApplicationTrackerEventType>(
      ApplicationTrackerEventType.REMOVE, masterInfo));
      masterInfo.numFailed++;
    }
  }


  /* Transition to start the process of allocating for the AM container */
  private static class AllocateTransition implements 
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {
    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      /* notify tracking applications that an applicaiton has been added */
      masterInfo.handler.handle(new ASMEvent<ApplicationTrackerEventType>(
        ApplicationTrackerEventType.ADD, masterInfo));
      
      /* schedule for a slot */
      masterInfo.handler.handle(new ASMEvent<SNEventType>(SNEventType.SCHEDULE,
      masterInfo));
    }
  }
  
  /* Transition on a container allocated for a container */
  private static class AllocatedTransition implements SingleArcTransition<ApplicationMasterInfo,
  ASMEvent<ApplicationEventType>> {

    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      /* set the container that was generated by the scheduler negotiator */
      masterInfo.masterContainer = event.getAppContext().getMasterContainer();
    }    
  }

  private static class RegisterTransition implements  SingleArcTransition<ApplicationMasterInfo,
  ASMEvent<ApplicationEventType>> {
    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      ApplicationMaster registeredMaster = event.getAppContext().getMaster();
      masterInfo.master.host = registeredMaster.host;
      masterInfo.master.httpPort = registeredMaster.httpPort;
      masterInfo.master.rpcPort = registeredMaster.rpcPort;
      masterInfo.master.status = registeredMaster.status;
      masterInfo.master.status.progress = 0.0f;
      masterInfo.master.status.lastSeen = System.currentTimeMillis();
    }
  }

  /* transition to finishing state on a cleanup, for now its not used, but will need it 
   * later */
  private static class FailedTransition implements 
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {

    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      LOG.info("Failed application: " + masterInfo.getApplicationID());
    } 
  }


  /* Just a status update transition */
  private static class StatusUpdateTransition implements 
  SingleArcTransition<ApplicationMasterInfo, ASMEvent<ApplicationEventType>> {

    @Override
    public void transition(ApplicationMasterInfo masterInfo,
    ASMEvent<ApplicationEventType> event) {
      masterInfo.master.status = event.getAppContext().getStatus();
      masterInfo.master.status.lastSeen = System.currentTimeMillis();
    }
  }

  @Override
  public synchronized void handle(ASMEvent<ApplicationEventType> event) {
    ApplicationID appID =  event.getAppContext().getApplicationID();
    LOG.info("Processing event for " + appID +  " of type " + event.getType());
    final ApplicationState oldState = getState();
    try {
      /* keep the master in sync with the state machine */
      stateMachine.doTransition(event.getType(), event);
      master.state = stateMachine.getCurrentState();
      LOG.info("State is " +  stateMachine.getCurrentState());
    } catch (InvalidStateTransitonException e) {
      LOG.error("Can't handle this event at current state", e);
      /* TODO fail the application on the failed transition */
    }
    if (oldState != getState()) {
      LOG.info(appID + " State change from " 
      + oldState + " to "
      + getState());
    }
  }
}