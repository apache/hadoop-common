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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_BIND_ADDRESS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_BIND_ADDRESS;
import static org.apache.hadoop.yarn.service.Service.STATE.STARTED;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerManager;
import org.apache.hadoop.yarn.ContainerStatus;
import org.apache.hadoop.yarn.YarnRemoteException;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerManagerSecurityInfo;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.ServiceStateChangeListener;

public class ContainerManagerImpl extends CompositeService implements
    ServiceStateChangeListener, ContainerManager,
    EventHandler<ContainerManagerEvent> {

  private static final Log LOG = LogFactory.getLog(ContainerManagerImpl.class);

  final Context context;
  private Server server;
  private InetSocketAddress cmAddr;
  private final ResourceLocalizationService rsrcLocalizationSrvc;
  private final ContainersLauncher containersLauncher;
  private final AuxServices auxiluaryServices;

  private final NodeStatusUpdater nodeStatusUpdater;
  private ContainerTokenSecretManager containerTokenSecretManager;

  protected AsyncDispatcher dispatcher;

  private DeletionService deletionService;

  public ContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater) {
    super(ContainerManagerImpl.class.getName());
    this.context = context;
    dispatcher = new AsyncDispatcher();
    this.deletionService = deletionContext;

    rsrcLocalizationSrvc =
        createResourceLocalizationService(exec, deletionContext);
    addService(rsrcLocalizationSrvc);

    containersLauncher = createContainersLauncher(context, exec);
    addService(containersLauncher);

    this.nodeStatusUpdater = nodeStatusUpdater;
    // Create the secretManager if need be.
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Security is enabled on NodeManager. "
          + "Creating ContainerTokenSecretManager");
      this.containerTokenSecretManager = new ContainerTokenSecretManager();
    }

    // Start configurable services
    auxiluaryServices = new AuxServices();
    auxiluaryServices.register(this);
    addService(auxiluaryServices);

    ContainersMonitor containersMonitor = new ContainersMonitor();
    addService(containersMonitor);

    dispatcher.register(ContainerEventType.class,
        new ContainerEventDispatcher());
    dispatcher.register(ApplicationEventType.class,
        new ApplicationEventDispatcher());
    dispatcher.register(LocalizerEventType.class, rsrcLocalizationSrvc);
    dispatcher.register(AuxServicesEventType.class, auxiluaryServices);
    dispatcher.register(ContainersMonitorEventType.class, containersMonitor);
    dispatcher.register(ContainersLauncherEventType.class, containersLauncher);
    addService(dispatcher);
  }

  protected ResourceLocalizationService createResourceLocalizationService(
      ContainerExecutor exec, DeletionService deletionContext) {
    return new ResourceLocalizationService(this.dispatcher, exec,
        deletionContext);
  }

  protected ContainersLauncher createContainersLauncher(Context context,
      ContainerExecutor exec) {
    return new ContainersLauncher(context, this.dispatcher, exec);
  }

  @Override
  public void init(Configuration conf) {
    cmAddr = NetUtils.createSocketAddr(
        conf.get(NM_BIND_ADDRESS, DEFAULT_NM_BIND_ADDRESS));
    Configuration cmConf = new Configuration(conf);
    cmConf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        ContainerManagerSecurityInfo.class, SecurityInfo.class);
    super.init(cmConf);
  }

  @Override
  public void start() {

    // Enqueue user dirs in deletion context

    YarnRPC rpc = YarnRPC.create(getConfig());
    if (UserGroupInformation.isSecurityEnabled()) {
      // This is fine as status updater is started before ContainerManager and
      // RM gives the shared secret in registration during StatusUpdter#start()
      // itself.
      this.containerTokenSecretManager.setSecretKey(
          this.nodeStatusUpdater.getNodeName(),
          this.nodeStatusUpdater.getRMNMSharedSecret());
    }
    server =
        rpc.getServer(ContainerManager.class, this, cmAddr, getConfig(),
            this.containerTokenSecretManager);
    LOG.info("ContainerManager started at " + cmAddr);
    server.start();
    super.start();
  }

  @Override
  public void stop() {
    if (auxiluaryServices.getServiceState() == STARTED) {
      auxiluaryServices.unregister(this);
    }
    if (server != null) {
      server.close();
    }
    super.stop();
  }

  @Override
  public Void cleanupContainer(ContainerID containerID)
      throws YarnRemoteException {
    // TODO Is this necessary?
    return null;
  }

  @Override
  public Void startContainer(ContainerLaunchContext launchContext)
      throws YarnRemoteException {
    Container container = new ContainerImpl(this.dispatcher, launchContext);
    ContainerID containerID = launchContext.id;
    ApplicationID applicationID = containerID.appID;
    if (this.context.getContainers().putIfAbsent(containerID, container) != null) {
      throw RPCUtil.getRemoteException("Container " + containerID
          + " already is running on this node!!");
    }
//    if (LOG.isDebugEnabled()) { TODO
      LOG.info("CONTAINER: " + launchContext);
//    }

    // Create the application
    Application application = new ApplicationImpl(this.dispatcher,
        launchContext.user, launchContext.id.appID,
        launchContext.env, launchContext.resources,
        launchContext.containerTokens);
    if (this.context.getApplications().putIfAbsent(applicationID, application) == null) {
      LOG.info("Creating a new application reference for app "
          + applicationID);
    }

    // TODO: Validate the request
    dispatcher.getEventHandler().handle(new ApplicationInitEvent(container));
    return null;
  }

  @Override
  public Void stopContainer(ContainerID containerID)
      throws YarnRemoteException {
    Container container = this.context.getContainers().get(containerID);
    if (container == null) {
      LOG.warn("Trying to stop unknown container " + containerID);
      return null;
      //throw RPCUtil.getRemoteException("Trying to stop unknown container "
      //    + containerID + " on this NodeManager!!");
    }
    dispatcher.getEventHandler().handle(
        new ContainerEvent(containerID, ContainerEventType.KILL_CONTAINER));

    // TODO: Move this code to appropriate place once kill_container is
    // implemented.
    nodeStatusUpdater.sendOutofBandHeartBeat();

    return null;
  }

  @Override
  public ContainerStatus getContainerStatus(ContainerID containerID)
      throws YarnRemoteException {
    LOG.info("Getting container-status for " + containerID);
    Container container = this.context.getContainers().get(containerID);
    if (container != null) {
      ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
      LOG.info("Returning " + containerStatus);
      return containerStatus;
    } else {
      throw RPCUtil.getRemoteException("Container " + containerID
          + " is not handled by this NodeManager");
    }
  }

  class ContainerEventDispatcher implements EventHandler<ContainerEvent> {
    @Override
    public void handle(ContainerEvent event) {
      Map<ContainerID,Container> containers =
        ContainerManagerImpl.this.context.getContainers();
      Container c = containers.get(event.getContainerID());
      if (c != null) {
        c.handle(event);
      } else {
        LOG.warn("Event " + event + " sent to absent container " +
            event.getContainerID());
      }
    }
  }

  class ApplicationEventDispatcher implements EventHandler<ApplicationEvent> {

    @Override
    public void handle(ApplicationEvent event) {
      Application app = 
      ContainerManagerImpl.this.context.getApplications().get(
          event.getApplicationID());
      if (app != null) {
        app.handle(event);
      } else {
        LOG.warn("Event " + event + " sent to absent application " +
            event.getApplicationID());
      }
    }
    
  }

  @Override
  public void handle(ContainerManagerEvent event) {
    switch (event.getType()) {
    case FINISH_APPS:
      CMgrCompletedAppsEvent appsFinishedEvent =
          (CMgrCompletedAppsEvent) event;
      for (ApplicationID appID : appsFinishedEvent.getAppsToCleanup()) {
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(appID,
                ApplicationEventType.FINISH_APPLICATION));
      }
      break;
    case FINISH_CONTAINERS:
      CMgrCompletedContainersEvent containersFinishedEvent =
          (CMgrCompletedContainersEvent) event;
      for (org.apache.hadoop.yarn.Container container : containersFinishedEvent
          .getContainersToCleanup()) {
        this.dispatcher.getEventHandler().handle(
            new ContainerEvent(container.id,
                ContainerEventType.KILL_CONTAINER));
      }
      break;
    default:
      LOG.warn("Invalid event " + event.getType() + ". Ignoring.");
    }
  }

  @Override
  public void stateChanged(Service service) {
    // TODO Auto-generated method stub
  }

}
