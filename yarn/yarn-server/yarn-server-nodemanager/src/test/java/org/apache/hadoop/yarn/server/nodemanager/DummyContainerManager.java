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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.HashMap;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;

public class DummyContainerManager extends ContainerManagerImpl {

  private static final Log LOG = LogFactory
      .getLog(DummyContainerManager.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  public DummyContainerManager(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater) {
    super(context, exec, deletionContext, nodeStatusUpdater);
  }

  @Override
  protected ResourceLocalizationService createResourceLocalizationService(ContainerExecutor exec,
      DeletionService deletionContext) {
    return new ResourceLocalizationService(super.dispatcher, exec, deletionContext) {
      @Override
      public void handle(LocalizerEvent event) {
        switch (event.getType()) {
        case INIT_APPLICATION_RESOURCES:
          Application app =
              ((ApplicationLocalizerEvent) event).getApplication();
          // Simulate event from ApplicationLocalization.
          this.dispatcher.getEventHandler().handle(
              new ApplicationInitedEvent(app.getAppId(),
                  new HashMap<Path, String>(), new Path("workDir")));
          break;
        case CLEANUP_CONTAINER_RESOURCES:
          Container container =
              ((ContainerLocalizerEvent) event).getContainer();
          // TODO: delete the container dir
          this.dispatcher.getEventHandler().handle(
              new ContainerEvent(container.getContainerID(),
                  ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
          break;
        case DESTROY_APPLICATION_RESOURCES:
          Application application =
            ((ApplicationLocalizerEvent) event).getApplication();

          // decrement reference counts of all resources associated with this
          // app
          this.dispatcher.getEventHandler().handle(
              new ApplicationEvent(application.getAppId(),
                  ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
          break;
        }
      }
    };
  }

  @Override
  protected ContainersLauncher createContainersLauncher(Context context,
      ContainerExecutor exec) {
    return new ContainersLauncher(context, super.dispatcher, exec) {
      @Override
      public void handle(ContainersLauncherEvent event) {
        Container container = event.getContainer();
        ContainerId containerId = container.getContainerID();
        switch (event.getType()) {
        case LAUNCH_CONTAINER:
          dispatcher.getEventHandler().handle(
              new ContainerEvent(containerId,
                  ContainerEventType.CONTAINER_LAUNCHED));
          break;
        case CLEANUP_CONTAINER:
          dispatcher.getEventHandler().handle(
              new ContainerExitEvent(containerId,
                  ContainerEventType.CONTAINER_KILLED_ON_REQUEST, 0));
          break;
        }
      }
    };
  }

  public static void waitForContainerState(ContainerManager containerManager,
      ContainerId containerID, ContainerState finalState)
      throws InterruptedException, YarnRemoteException {
    waitForContainerState(containerManager, containerID, finalState, 20);
  }

  public static void waitForContainerState(ContainerManager containerManager,
        ContainerId containerID, ContainerState finalState, int timeOutMax)
        throws InterruptedException, YarnRemoteException {
      GetContainerStatusRequest request = recordFactory.newRecordInstance(GetContainerStatusRequest.class);
      request.setContainerId(containerID);
      ContainerStatus containerStatus =
          containerManager.getContainerStatus(request).getStatus();
      int timeoutSecs = 0;
    while (!containerStatus.getState().equals(finalState)
        && timeoutSecs++ < timeOutMax) {
        Thread.sleep(1000);
        LOG.info("Waiting for container to get into state " + finalState
            + ". Current state is " + containerStatus.getState());
        containerStatus = containerManager.getContainerStatus(request).getStatus();
      }
      LOG.info("Container state is " + containerStatus.getState());
      Assert.assertEquals("ContainerState is not correct (timedout)",
          finalState, containerStatus.getState());
    }
}
