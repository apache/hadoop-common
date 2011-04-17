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

package org.apache.hadoop.yarn.server.resourcemanager;


import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.AvroRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.SyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

/**
 * The ResourceManager is the main class that is a set of components.
 *
 */
public class ResourceManager extends CompositeService {
  private static final Log LOG = LogFactory.getLog(ResourceManager.class);
  public static final long clusterTimeStamp = System.currentTimeMillis();
  private YarnConfiguration conf;
  
  private ApplicationsManagerImpl applicationsManager;
  
  private ContainerTokenSecretManager containerTokenSecretManager =
      new ContainerTokenSecretManager();

  private ApplicationTokenSecretManager appTokenSecretManager =
      new ApplicationTokenSecretManager();

  
  private ResourceScheduler scheduler;
  private RMResourceTrackerImpl rmResourceTracker;
  private ClientRMService clientRM;
  private ApplicationMasterService masterService;
  private AtomicBoolean shutdown = new AtomicBoolean(false);
  private WebApp webApp;
  private final ASMContext asmContext;
  
  public ResourceManager() {
    super("ResourceManager");
    this.asmContext = new ASMContextImpl();
  }
  
  
  public interface ASMContext {
    public SyncDispatcher getDispatcher();
  }
  
  public static class ASMContextImpl implements ASMContext {
    private final SyncDispatcher asmEventDispatcher;
   
    public ASMContextImpl() {
      this.asmEventDispatcher = new SyncDispatcher();
    }
    
    @Override
    public SyncDispatcher getDispatcher() {
      return this.asmEventDispatcher;
    }
  }

  @Override
  public synchronized void init(Configuration conf) {
    // Initialize the config
    this.conf = new YarnConfiguration(conf);
    // Initialize the scheduler
    this.scheduler = 
      ReflectionUtils.newInstance(
          conf.getClass(YarnConfiguration.RESOURCE_SCHEDULER, 
              FifoScheduler.class, ResourceScheduler.class), 
          this.conf);
    this.scheduler.reinitialize(this.conf, this.containerTokenSecretManager);
    /* add the scheduler to be notified of events from the applications managers */
    this.asmContext.getDispatcher().register(ApplicationTrackerEventType.class, this.scheduler);
    //TODO change this to be random
    this.appTokenSecretManager.setMasterKey(ApplicationTokenSecretManager
        .createSecretKey("Dummy".getBytes()));

    applicationsManager = createApplicationsManagerImpl();
    addService(applicationsManager);
    
    rmResourceTracker = createRMResourceTracker(this.scheduler);
    addService(rmResourceTracker);
    
    clientRM = createClientRMService();
    addService(clientRM);
    
    masterService = createApplicationMasterService();
    addService(masterService) ;
    super.init(conf);
  }
  
  @Override
  public void start() { 

    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new AvroRuntimeException("Failed to login", ie);
    }

    webApp = WebApps.$for("yarn", masterService).at(
      conf.get(YarnConfiguration.RM_WEBAPP_BIND_ADDRESS,
      YarnConfiguration.DEFAULT_RM_WEBAPP_BIND_ADDRESS)).
    start(new RMWebApp(this));

    super.start();

    synchronized(shutdown) {
      try {
        while(!shutdown.get()) {
          shutdown.wait();
        }
      } catch(InterruptedException ie) {
        LOG.info("Interrupted while waiting", ie);
      }
    }
  }
  
  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(conf, RMConfig.RM_KEYTAB,
        YarnConfiguration.RM_SERVER_PRINCIPAL_KEY);
  }

  @Override
  public void stop() {
    if (webApp != null) {
      webApp.stop();
    }
  
    synchronized(shutdown) {
      shutdown.set(true);
      shutdown.notifyAll();
    }
    super.stop();
  }
  
  protected RMResourceTrackerImpl createRMResourceTracker(ResourceListener listener) {
    return new RMResourceTrackerImpl(this.containerTokenSecretManager, listener);
  }
  
  protected ApplicationsManagerImpl createApplicationsManagerImpl() {
    return new ApplicationsManagerImpl(
        this.appTokenSecretManager, this.scheduler, this.asmContext);
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(applicationsManager, rmResourceTracker, scheduler);
  }

  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(
      this.appTokenSecretManager, applicationsManager, scheduler, this.asmContext);
  }
  
  /**
   * return applications manager.
   * @return
   */
  public ApplicationsManager getApplicationsManager() {
    return applicationsManager;
  }
  
  /**
   * return the scheduler.
   * @return
   */
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }
  
  /**
   * return the resource tracking component.
   * @return
   */
  public RMResourceTrackerImpl getResourceTracker() {
    return this.rmResourceTracker;
  }
  
  
  public static void main(String argv[]) {
    ResourceManager resourceManager = null;
    try {
      Configuration conf = new YarnConfiguration();
      resourceManager = new ResourceManager();
      resourceManager.init(conf);
      resourceManager.start();
    } catch (Exception e) {
      LOG.error("Error starting RM", e);
    } finally {
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }
}
