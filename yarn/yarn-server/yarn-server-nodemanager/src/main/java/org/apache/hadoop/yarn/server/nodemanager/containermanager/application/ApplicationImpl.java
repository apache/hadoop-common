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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.AvroUtil;

import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.LocalResourceVisibility;

public class ApplicationImpl implements Application {

  final Dispatcher dispatcher;
  final String user;
  final ApplicationID appId;
  final Map<CharSequence,CharSequence> env;
  final Map<CharSequence,LocalResource> resources;
  final ByteBuffer containerTokens;
  Map<Path,String> localizedResources;

  private static final Log LOG = LogFactory.getLog(Application.class);

  private List<ContainerID> containersWaitingForAppInit =
      new ArrayList<ContainerID>();

  // TODO check for suitability of symlink name
  static Map<String,LocalResource>
    filterResources(Map<CharSequence,LocalResource> resources,
        LocalResourceVisibility state) {
    Map<String,LocalResource> ret =
      new HashMap<String,LocalResource>();
    for (Map.Entry<CharSequence,LocalResource> rsrc : resources.entrySet()) {
      if (state.equals(rsrc.getValue().state)) {
        ret.put(rsrc.getKey().toString(), rsrc.getValue());
      }
    }
    return ret;
  }

  public ApplicationImpl(Dispatcher dispatcher,
      CharSequence user,
      ApplicationID appId,
      Map<CharSequence,CharSequence> env,
      Map<CharSequence,LocalResource> resources,
      ByteBuffer containerTokens) {
    this.dispatcher = dispatcher;
    this.user = user.toString();
    this.appId = appId;
    this.env = env;
    this.resources = null == resources
      ? new HashMap<CharSequence,LocalResource>()
      : resources;
    this.containerTokens = containerTokens;
    stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public String getUser() {
    return user.toString();
  }

  @Override
  public ApplicationID getAppId() {
    return appId;
  }

  @Override
  public Map<CharSequence,CharSequence> getEnvironment() {
    return env;
  }

  @Override
  public Map<String,LocalResource>
      getResources(LocalResourceVisibility vis) {
    final Map<String,LocalResource> ret;
    if (LocalResourceVisibility.PUBLIC.equals(vis)) {
      ret = filterResources(resources, LocalResourceVisibility.PUBLIC);
    } else {
      // TODO separate these
      ret = filterResources(resources, LocalResourceVisibility.PRIVATE);
      ret.putAll(filterResources(resources, LocalResourceVisibility.APPLICATION));
    }
    return Collections.unmodifiableMap(ret);
  }

  @Override
  public Map<Path,String> getLocalizedResources() {
    if (ApplicationState.INITED.equals(stateMachine.getCurrentState())) {
      return localizedResources;
    }
    throw new IllegalStateException(
        "Invalid request for " + stateMachine.getCurrentState());
  }

  @Override
  public Credentials getCredentials() throws IOException {
    Credentials ret = new Credentials();
    if (containerTokens != null) {
      DataInputByteBuffer buf = new DataInputByteBuffer();
      buf.reset(containerTokens);
      ret.readTokenStorageStream(buf);
      for (Token<? extends TokenIdentifier> tk : ret.getAllTokens()) {
        LOG.info(" In Nodemanager , token " + tk);
      }
    }
    return ret;
  }

  private static StateMachineFactory
         <ApplicationImpl, ApplicationState, ApplicationEventType, ApplicationEvent>
       stateMachineFactory =
      new StateMachineFactory
         <ApplicationImpl, ApplicationState, ApplicationEventType, ApplicationEvent>
       (ApplicationState.NEW)
    .addTransition(ApplicationState.NEW, ApplicationState.INITING,
        ApplicationEventType.INIT_APPLICATION, new AppInitTransition())
    .addTransition(ApplicationState.INITING, ApplicationState.INITING,
        ApplicationEventType.INIT_APPLICATION, new AppIsInitingTransition())
    .addTransition(ApplicationState.INITING, ApplicationState.FINISHING,
        ApplicationEventType.FINISH_APPLICATION, new AppFinishingTransition())
    .addTransition(ApplicationState.INITING, ApplicationState.INITED,
        ApplicationEventType.APPLICATION_INITED, new AppInitedTransition())
    .addTransition(ApplicationState.INITED,
        ApplicationState.INITED, ApplicationEventType.INIT_APPLICATION,
        new AppHasInitedTransition())
    .addTransition(ApplicationState.INITED,
        ApplicationState.FINISHING, ApplicationEventType.FINISH_APPLICATION,
        new AppFinishingTransition())
    .addTransition(ApplicationState.FINISHING,
        ApplicationState.FINISHED, ApplicationEventType.APPLICATION_FINISHED)
    // TODO failure transitions are completely broken

    // create the topology tables
    .installTopology();

  private final StateMachine<ApplicationState, ApplicationEventType, ApplicationEvent>
    stateMachine;

  static class AppInitTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent) event;
      ContainerID cId = initEvent.getContainerRequestingAppInit();
      app.containersWaitingForAppInit.add(cId);
      app.dispatcher.getEventHandler().handle(
          new ContainerEvent(cId, ContainerEventType.INIT_CONTAINER));
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizerEvent(
            LocalizerEventType.INIT_APPLICATION_RESOURCES, app));
    }
  }

  static class AppIsInitingTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent) event;
      ContainerID cId = initEvent.getContainerRequestingAppInit();
      app.containersWaitingForAppInit.add(cId);
      app.dispatcher.getEventHandler().handle(
          new ContainerEvent(cId, ContainerEventType.INIT_CONTAINER));
    }
  }

  static class AppInitedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {

      ApplicationInitedEvent initedEvent = (ApplicationInitedEvent) event;
      app.localizedResources = initedEvent.getLocalizedResources();
      // Start all the containers waiting for ApplicationInit
      for (ContainerID containerID : app.containersWaitingForAppInit) {
        app.dispatcher.getEventHandler().handle(
            new ContainerEvent(containerID,
              ContainerEventType.CONTAINER_RESOURCES_LOCALIZED));
      }
    }
  }

  static class AppHasInitedTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent) event;
      ContainerID containerID = initEvent.getContainerRequestingAppInit();
      app.dispatcher.getEventHandler().handle(
          new ContainerEvent(containerID, ContainerEventType.INIT_CONTAINER));
      app.dispatcher.getEventHandler().handle(
          new ContainerEvent(containerID,
            ContainerEventType.CONTAINER_RESOURCES_LOCALIZED));
    }
  }

  static class AppFinishingTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {

      // Send event to ContainersLauncher to finish all the containers of this
      // application.
      for (ContainerID containerID : app.containersWaitingForAppInit) {
        app.dispatcher.getEventHandler().handle(
            new ContainerEvent(containerID,
              ContainerEventType.KILL_CONTAINER));
      }

      // Delete Application level resources
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizerEvent(
            LocalizerEventType.DESTROY_APPLICATION_RESOURCES, app));

      // TODO: Trigger the LogsManager
    }
  }

  @Override
  public synchronized void handle(ApplicationEvent event) {

    ApplicationID applicationID = event.getApplicationID();
    LOG.info("Processing " + applicationID + " of type " + event.getType());

    ApplicationState oldState = stateMachine.getCurrentState();
    ApplicationState newState = null;
    try {
      // queue event requesting init of the same app
      newState = stateMachine.doTransition(event.getType(), event);
    } catch (InvalidStateTransitonException e) {
      LOG.warn("Can't handle this event at current state", e);
    }
    if (oldState != newState) {
      LOG.info("Application " + applicationID + " transitioned from "
          + oldState + " to " + newState);
    }
  }

  @Override
  public String toString() {
    return AvroUtil.toString(appId);
  }
}
