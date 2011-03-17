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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterTracker.NodeResponse;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.HeartbeatResponse;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;
import org.junit.After;
import org.junit.Before;

import junit.framework.TestCase;

public class TestRMNMRPCResponseId extends TestCase {
  RMResourceTrackerImpl rmResourceTrackerImpl;
  ContainerTokenSecretManager containerTokenSecretManager =
    new ContainerTokenSecretManager();
  ResourceListener listener = new DummyResourceListener();
  private NodeID nodeid;
  
  private class DummyResourceListener implements ResourceListener {

    @Override
    public NodeInfo addNode(NodeID nodeId, String hostName, Node node,
        Resource capability) {
      nodeid = nodeId;
      return new NodeManager(nodeId, hostName, node, capability);
    }

    @Override
    public void removeNode(NodeInfo node) {
      /* do nothing */
    }

    @Override
    public NodeResponse nodeUpdate(NodeInfo nodeInfo,
        Map<CharSequence, List<Container>> containers) {
      return new NodeResponse(new ArrayList<ApplicationID>(),
          new ArrayList<Container>(), new ArrayList<Container>());
    }
  }
  
  @Before
  public void setUp() {
    rmResourceTrackerImpl = new RMResourceTrackerImpl(containerTokenSecretManager);
    rmResourceTrackerImpl.register(listener);
  }
  
  @After
  public void tearDown() {
    /* do nothing */
  }
  
  public void testRPCResponseId() throws IOException {
    String node = "localhost";
    Resource capability = new Resource();
    rmResourceTrackerImpl.registerNodeManager(node, capability);
    org.apache.hadoop.yarn.NodeStatus nodeStatus = new org.apache.hadoop.yarn.NodeStatus();
    nodeStatus.nodeId = nodeid;
    nodeStatus.responseId = 0;
    HeartbeatResponse response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.responseId == 1);
    nodeStatus.responseId = response.responseId;
    response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.responseId == 2);   
    /* try calling with less response id */
    response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.responseId == 2);   
    nodeStatus.responseId = 0;
    response = rmResourceTrackerImpl.nodeHeartbeat(nodeStatus);
    assertTrue(response.reboot == true);
  }
}