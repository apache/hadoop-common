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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterTracker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerToken;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test ClusterInfo class.
 *
 */
public class TestClusterTracker extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestClusterTracker.class);
  private ResourceScheduler scheduler;
  private ClusterTracker clusterTracker;
  private final int memoryCapability = 1024;
  
  private class TestScheduler extends FifoScheduler {
    @Override
    protected ClusterTracker createClusterTracker() {
      clusterTracker = new ClusterTrackerImpl();
      return clusterTracker;
    }
  }
  
  @Before
  public void setUp() {
    scheduler = new TestScheduler();
  }
  
  @After
  public void tearDown() {
    /* Nothing to do */
  }
  
  public NodeInfo addNodes(String commonName, int i, int memoryCapability) {
    NodeID nodeId = new NodeID();
    nodeId.id = i;
    String hostName = commonName + "_" + i;
    Node node = new NodeBase(hostName, NetworkTopology.DEFAULT_RACK);
    Resource capability = new Resource();
    capability.memory = 1024;
    return scheduler.addNode(nodeId, hostName, node, capability);
  }
  
  @Test
  public void testAddingNodes() {
    Resource expectedResource = new Resource();
    List<NodeInfo> nodesAdded = new ArrayList<NodeInfo>();
    for (int i = 0; i < 10; i++) {
      NodeInfo node = addNodes("localhost", i, 1024);
      nodesAdded.add(node);
      /* check if the node exists */
      assertTrue(clusterTracker.isTracked(node));
      Resource clusterResource = clusterTracker.getClusterResource();
      expectedResource.memory = expectedResource.memory + memoryCapability;
      assertTrue(clusterResource.memory == expectedResource.memory);
    }
    for (NodeInfo node: nodesAdded) {
      clusterTracker.removeNode(node);
      assertFalse(clusterTracker.isTracked(node));
      Resource clusterResource = clusterTracker.getClusterResource();
      expectedResource.memory = expectedResource.memory - memoryCapability;
      assertTrue(clusterResource.memory == expectedResource.memory);
    }
  }
  
  @Test
  public void testAddingContainers() {
    NodeInfo node = null;
    for (int i = 0; i < 10; i++) {
      node = addNodes("localhost", i, 1024);
    }
    ApplicationID appId = new ApplicationID();
    appId.clusterTimeStamp = System.currentTimeMillis();
    appId.id = 1;
    /* test for null containers */
    clusterTracker.addAllocatedContainers(node, appId, null);
    Container container = new Container();
    container.containerToken = new ContainerToken();
    container.hostName = node.getHostName();
    container.id = new ContainerID();
    container.id.appID = appId;
    container.id.id = 1;
    Resource resource = new Resource();
    resource.memory = 100;
    container.resource = resource;
    List<Container> list = new ArrayList<Container>();
    list.add(container);
    int oldMemory  = node.getAvailableResource().memory;
    clusterTracker.addAllocatedContainers(node, appId, list);
    int  tmpMemory = node.getAvailableResource().memory;
    int diff = oldMemory- tmpMemory;
    LOG.info("The difference in cluster metrics is " + diff);
    assertTrue(diff == 100);
    for (Container c : list) {
      clusterTracker.releaseContainer(appId, c);
    }
    diff = oldMemory - node.getAvailableResource().memory;
    assertTrue(diff == 0);
  }
}