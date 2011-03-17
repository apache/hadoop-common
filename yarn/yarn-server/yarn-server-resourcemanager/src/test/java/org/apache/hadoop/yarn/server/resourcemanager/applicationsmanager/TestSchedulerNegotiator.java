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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.ASMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterTracker.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestSchedulerNegotiator extends TestCase {
  private SchedulerNegotiator schedulerNegotiator;
  private DummyScheduler scheduler;
  private final int testNum = 99999;
  
  private final ASMContext context = new ResourceManager.ASMContextImpl();
  ApplicationMasterInfo masterInfo;
  private EventHandler handler;
  
  private class DummyScheduler implements ResourceScheduler {
    @Override
    public List<Container> allocate(ApplicationID applicationId,
        List<ResourceRequest> ask, List<Container> release) throws IOException {
      ArrayList<Container> containers = new ArrayList<Container>();
      Container container = new Container();
      container.id = new ContainerID();
      container.id.appID = applicationId;
      container.id.id = testNum;
      containers.add(container);
      return containers;
    }
    @Override
    public void addApplication(ApplicationID applicationId, String user,
        String unused, Priority priority)
        throws IOException {
    }
    @Override
    public void removeApplication(ApplicationID applicationId)
        throws IOException {
    }
    @Override
    public void reinitialize(Configuration conf,
        ContainerTokenSecretManager secretManager) {
    }
    @Override
    public NodeInfo addNode(NodeID nodeId, String hostName, Node node,
        Resource capability) {
      return null;
    }
    @Override
    public NodeResponse nodeUpdate(NodeInfo nodeInfo,
        Map<CharSequence, List<Container>> containers) {
      return null;
    }
    @Override
    public void removeNode(NodeInfo node) {
    }
  }
  
  @Before
  public void setUp() {
    scheduler = new DummyScheduler();
    schedulerNegotiator = new SchedulerNegotiator(context, scheduler);
    schedulerNegotiator.init(new Configuration());
    schedulerNegotiator.start();
    handler = context.getDispatcher().getEventHandler();
  }
  
  @After
  public void tearDown() {
    schedulerNegotiator.stop();
  }
  
  public void waitForState(ApplicationState state, ApplicationMasterInfo info) {
    int count = 0;
    while (info.getState() != state && count < 100) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
       e.printStackTrace();
      }
      count++;
    }
    Assert.assertEquals(state, info.getState());
  }
  
  @Test
  public void testSchedulerNegotiator() throws Exception {
    ApplicationSubmissionContext submissionContext = new ApplicationSubmissionContext();
    submissionContext.applicationId = new ApplicationID();
    submissionContext.applicationId.clusterTimeStamp = System.currentTimeMillis();
    submissionContext.applicationId.id = 1;
    masterInfo =
      new ApplicationMasterInfo(this.context.getDispatcher().getEventHandler(),
          "dummy", submissionContext, "dummyClientToken");
    context.getDispatcher().register(ApplicationEventType.class, masterInfo);
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.
    ALLOCATE, masterInfo));
    waitForState(ApplicationState.ALLOCATED, masterInfo);
    Container container = masterInfo.getMasterContainer();
    assertTrue(container.id.id == testNum);
  }
}