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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Application;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.ASMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;


/**
 * This is the main class for the applications manager. This keeps track
 * of the application masters running in the system and is responsible for 
 * getting a container for AM and launching it.
 * {@link ApplicationsManager} is the interface that clients use to talk to 
 * ASM via the RPC servers. {@link ApplicationMasterHandler} is the interface that 
 * AM's use to talk to the ASM via the RPC.
 */
public class ApplicationsManagerImpl extends CompositeService
  implements ApplicationsManager, ApplicationMasterHandler  {
  private static final Log LOG = LogFactory.getLog(ApplicationsManagerImpl.class);

  final private AtomicInteger applicationCounter = new AtomicInteger(0);
  final private YarnScheduler scheduler;
  private AMTracker amTracker;
  private ClientToAMSecretManager clientToAMSecretManager =
    new ClientToAMSecretManager();
  private final EventHandler eventHandler;
  private final ApplicationTokenSecretManager applicationTokenSecretManager;
  private final ASMContext asmContext; 
  
  public ApplicationsManagerImpl(ApplicationTokenSecretManager 
      applicationTokenSecretManager, YarnScheduler scheduler, ASMContext asmContext) {
    super("ApplicationsManager");
    this.scheduler = scheduler;
    this.asmContext = asmContext;
    this.eventHandler = this.asmContext.getDispatcher().getEventHandler();
    this.applicationTokenSecretManager = applicationTokenSecretManager;
  }
  

  /**
   * create a new am heart beat handler.
   * @return create a new am heart beat handler.
   */
  protected AMTracker createNewAMTracker() {
    return new AMTracker(this.asmContext);
  }

  /**
   * Create a new scheduler negotiator.
   * @param scheduler the scheduler 
   * @return scheduler negotiator that talks to the scheduler.
   */
  protected EventHandler<ASMEvent<SNEventType>> createNewSchedulerNegotiator(YarnScheduler scheduler) {
    return new SchedulerNegotiator(this.asmContext, scheduler);
  }

  /**
   * create a new application master launcher.
   * @param tokenSecretManager the token manager for applications.
   * @return {@link ApplicationMasterLauncher} responsible for launching
   * application masters.
   */
  protected EventHandler<ASMEvent<AMLauncherEventType>> createNewApplicationMasterLauncher(
      ApplicationTokenSecretManager tokenSecretManager) {
    return  new ApplicationMasterLauncher(tokenSecretManager,
        this.clientToAMSecretManager, this.asmContext);
  }

  /**
   * Add to service if a service object.
   * @param object
   */
  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  @Override
  public synchronized void init(Configuration conf) {
    addIfService(createNewApplicationMasterLauncher(applicationTokenSecretManager));
    addIfService(createNewSchedulerNegotiator(scheduler));
    this.amTracker = createNewAMTracker();
    addIfService(amTracker);
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized ApplicationMaster getApplicationMaster(ApplicationID applicationId) {
    ApplicationMaster appMaster =
      amTracker.get(applicationId).getMaster();
    return appMaster;
  }
  
  @Override
  public ApplicationID getNewApplicationID() {
    ApplicationID applicationId =
      org.apache.hadoop.yarn.server.resourcemanager.resource.ApplicationID.create(
          ResourceManager.clusterTimeStamp, applicationCounter.incrementAndGet());
    LOG.info("Allocated new applicationId: " + applicationId.id);
    return applicationId;
  }

  @Override
  public synchronized void submitApplication(ApplicationSubmissionContext context)
  throws IOException {
    String user;
    ApplicationID applicationId = context.applicationId;
    String clientTokenStr = null;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
      if (UserGroupInformation.isSecurityEnabled()) {
        Token<ApplicationTokenIdentifier> clientToken =
          new Token<ApplicationTokenIdentifier>(
              new ApplicationTokenIdentifier(applicationId),
              this.clientToAMSecretManager);
        clientTokenStr = clientToken.encodeToUrlString();
        LOG.debug("Sending client token as " + clientTokenStr);
      }
    } catch (IOException e) {
      LOG.info("Error in submitting application", e);
      throw e;
    } 

    context.queue =
        (context.queue == null ? "default" : context.queue.toString());
    context.applicationName =
        (context.applicationName == null ? "N/A" : context.applicationName);

    amTracker.addMaster(user, context, clientTokenStr);
    // TODO this should happen via dispatcher. should move it out to scheudler
    // negotiator.
    /* schedule */    
    LOG.info("Application with id " + applicationId.id + " submitted by user " + 
        user + " with " + context);
  }

  @Override
  public synchronized void finishApplicationMaster(ApplicationMaster applicationMaster)
  throws IOException {
    amTracker.finish(applicationMaster.applicationId);
  }

  @Override
  public synchronized void finishApplication(ApplicationID applicationId) 
  throws IOException {
    /* remove the applicaiton from the scheduler  for now. Later scheduler should
     * be a event handler of adding and cleaning up appications*/
    amTracker.kill(applicationId);
  }

  @Override
  public synchronized void applicationHeartbeat(ApplicationStatus status) 
  throws IOException {
    amTracker.heartBeat(status);
  }

  @Override
  public synchronized void registerApplicationMaster(ApplicationMaster applicationMaster)
  throws IOException {
    amTracker.registerMaster(applicationMaster);
 }

  @Override
  public synchronized List<AppContext> getAllApplications() {
    return amTracker.getAllApplications();
  }

  public synchronized ApplicationMasterInfo getApplicationMasterInfo(ApplicationID
      applicationId) {
    return amTracker.get(applicationId);
  }
  
  static class AppImpl implements Application {
    final ApplicationMaster am;
    final String user;
    final String queue;
    final String name;
    
    AppImpl(ApplicationMaster am, String user, String queue, String name) { 
      this.am = am; 
      this.user = user;
      this.queue = queue;
      this.name = name;
    }

    @Override public ApplicationID id() { return am.applicationId; }
    @Override public CharSequence user() { return user; }
    @Override public CharSequence name() { return name; }
    @Override public CharSequence queue() { return queue; }
    @Override public ApplicationStatus status() { return am.status; }
    @Override public CharSequence master() { return am.host; }
    @Override public int httpPort() { return am.httpPort; }
    @Override public ApplicationState state() { return am.state; }
    @Override public boolean isFinished() {
      switch (am.state) {
        case COMPLETED:
        case FAILED:
        case KILLED: return true;
      }
      return false;
    }
  }

  @Override
  public List<Application> getApplications() {
    List<Application> apps = new ArrayList<Application>();
    for (AppContext am: getAllApplications()) {
      apps.add(new AppImpl(am.getMaster(), 
          am.getUser(), am.getQueue(), am.getName()));
    }
    return apps;
  }

  @Override
  public Application getApplication(ApplicationID appID) {
    ApplicationMasterInfo master = amTracker.get(appID);
    return (master == null) ? null : 
      new AppImpl(master.getMaster(), 
          master.getUser(), master.getQueue(), master.getName());
  }
}
