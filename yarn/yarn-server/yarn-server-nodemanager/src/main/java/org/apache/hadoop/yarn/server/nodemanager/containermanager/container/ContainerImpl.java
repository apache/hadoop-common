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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.AvroUtil;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerStatus;

public class ContainerImpl implements Container {

  private final Dispatcher dispatcher;
  private final ContainerLaunchContext launchContext;

  private static final Log LOG = LogFactory.getLog(Container.class);

  public ContainerImpl(Dispatcher dispatcher,
      ContainerLaunchContext launchContext) {
    this.dispatcher = dispatcher;
    this.launchContext = launchContext;

    stateMachine = stateMachineFactory.make(this);
  }

  // State Machine for each container.
  private static StateMachineFactory
           <ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>
        stateMachineFactory =
      new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.NEW)
    // From NEW State
    .addTransition(ContainerState.NEW, ContainerState.LOCALIZING,
        ContainerEventType.INIT_CONTAINER)
    .addTransition(ContainerState.NEW, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER)

    // From LOCALIZING State
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.LOCALIZED,
        ContainerEventType.CONTAINER_RESOURCES_LOCALIZED,
        new LocalizedTransition())
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.CONTAINER_RESOURCES_CLEANINGUP,
        ContainerEventType.KILL_CONTAINER,
        new KillDuringLocalizationTransition())

    // From LOCALIZED State
    .addTransition(ContainerState.LOCALIZED, ContainerState.RUNNING,
        ContainerEventType.CONTAINER_LAUNCHED, new LaunchTransition())
    .addTransition(ContainerState.LOCALIZED, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition())

    // From RUNNING State
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition())
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())

    // From CONTAINER_EXITED_WITH_SUCCESS State
    .addTransition(ContainerState.EXITED_WITH_SUCCESS, ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
                   ContainerState.EXITED_WITH_SUCCESS,
                   ContainerEventType.KILL_CONTAINER)

    // From EXITED_WITH_FAILURE State
    .addTransition(ContainerState.EXITED_WITH_FAILURE, ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
                   ContainerState.EXITED_WITH_FAILURE,
                   ContainerEventType.KILL_CONTAINER)

    // From KILLING State.
    .addTransition(ContainerState.KILLING,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.CONTAINER_CLEANEDUP_AFTER_KILL,
        new ContainerKilledTransition())
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition())
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition())

    // From CONTAINER_CLEANEDUP_AFTER_KILL State.
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP)

    // From DONE
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER)

    // create the topology tables
    .installTopology();

  private final StateMachine<ContainerState, ContainerEventType, ContainerEvent>
    stateMachine;

  private synchronized org.apache.hadoop.yarn.ContainerState getCurrentState() {
    switch (stateMachine.getCurrentState()) {
    case NEW:
    case LOCALIZING:
    case LOCALIZED:
      return org.apache.hadoop.yarn.ContainerState.INTIALIZING;
    case RUNNING:
    case EXITED_WITH_SUCCESS:
    case EXITED_WITH_FAILURE:
    case KILLING:
    case CONTAINER_CLEANEDUP_AFTER_KILL:
    case CONTAINER_RESOURCES_CLEANINGUP:
      return org.apache.hadoop.yarn.ContainerState.RUNNING;
    case DONE:
    default:
      return org.apache.hadoop.yarn.ContainerState.COMPLETE;
    }
  }

  @Override
  public synchronized org.apache.hadoop.yarn.Container getContainer() {
    org.apache.hadoop.yarn.Container c = new org.apache.hadoop.yarn.Container();
    c.id = this.launchContext.id;
    c.resource = this.launchContext.resource;
    c.state = getCurrentState();
    return c;
  }

  @Override
  public ContainerLaunchContext getLaunchContext() {
    return launchContext;
  }

  @Override
  public ContainerStatus getContainerStatus() {
    ContainerStatus containerStatus = new ContainerStatus();
    containerStatus.state = getCurrentState();
    containerStatus.containerID = this.launchContext.id;
    // TODO: Exit status.
    return containerStatus;
  }

  static class ContainerTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Just drain the event and change the state.
    }

  }

  static class LocalizedTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // XXX This needs to be container-oriented
      // Inform the AuxServices about the opaque serviceData
      ContainerLaunchContext ctxt = container.getLaunchContext();
      Map<CharSequence,ByteBuffer> csd = ctxt.serviceData;
      if (csd != null) {
        // TODO: Isn't this supposed to happen only once per Application?
        for (Map.Entry<CharSequence,ByteBuffer> service : csd.entrySet()) {
          container.dispatcher.getEventHandler().handle(
              new AuxServicesEvent(AuxServicesEventType.APPLICATION_INIT,
                ctxt.user.toString(), ctxt.id.appID,
                service.getKey().toString(), service.getValue()));
        }
      }
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.LAUNCH_CONTAINER));
    }
  }

  static class LaunchTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the ContainersMonitor to start monitoring the container's
      // resource usage.
      container.dispatcher.getEventHandler().handle(
          new ContainersMonitorEvent(
              ContainersMonitorEventType.START_MONITORING_CONTAINER));
    }
  }

  static class ExitedWithSuccessTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // TODO: Add containerWorkDir to the deletion service.

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizerEvent(
            LocalizerEventType.CLEANUP_CONTAINER_RESOURCES, container));
    }
  }

  static class ExitedWithFailureTransition extends ContainerTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // TODO: Add containerWorkDir to the deletion service.
      // TODO: Add containerOuputDir to the deletion service.

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizerEvent(
            LocalizerEventType.CLEANUP_CONTAINER_RESOURCES, container));
    }
  }

  static class KillDuringLocalizationTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizerEvent(
            LocalizerEventType.CLEANUP_CONTAINER_RESOURCES, container));

    }
  }

  static class KillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Kill the process/process-grp
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.CLEANUP_CONTAINER));
    }
  }

  static class ContainerKilledTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // The process/process-grp is killed. Decrement reference counts and
      // cleanup resources
      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizerEvent(
            LocalizerEventType.CLEANUP_CONTAINER_RESOURCES, container));
    }
  }

  @Override
  public synchronized void handle(ContainerEvent event) {

    ContainerID containerID = event.getContainerID();
    LOG.info("Processing " + containerID + " of type " + event.getType());

    ContainerState oldState = stateMachine.getCurrentState();
    ContainerState newState = null;
    try {
      newState =
          stateMachine.doTransition(event.getType(), event);
    } catch (InvalidStateTransitonException e) {
      LOG.warn("Can't handle this event at current state", e);
    }
    if (oldState != newState) {
      LOG.info("Container " + containerID + " transitioned from " + oldState
          + " to " + newState);
    }
  }

  @Override
  public String toString() {
    return AvroUtil.toString(launchContext.id);
  }
}
