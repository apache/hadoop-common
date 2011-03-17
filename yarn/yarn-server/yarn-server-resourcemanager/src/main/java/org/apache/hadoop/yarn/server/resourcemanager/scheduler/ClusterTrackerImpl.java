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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;

@Evolving
@Private
public class ClusterTrackerImpl extends AbstractService implements ClusterTracker {
  
  public ClusterTrackerImpl() {
    super("ClusterTrackerImpl");
  }

  private static final Log LOG = LogFactory.getLog(ClusterTrackerImpl.class);
  private Map<String, NodeManager> nodes = new HashMap<String, NodeManager>();
  private Resource clusterResource = new Resource();
  private Configuration conf;
  
  public void init(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public synchronized Resource getClusterResource() {
    return clusterResource;
  }

  @Override
  public synchronized void removeNode(NodeInfo nodeInfo) {
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        clusterResource, nodeInfo.getTotalCapability());
    nodes.remove(nodeInfo.getHostName());
  }
  
  @Override
  public synchronized boolean isTracked(NodeInfo nodeInfo) {
    NodeManager node = nodes.get(nodeInfo.getHostName());
    return (node == null? false: true);
  }
 
  @Override
  public synchronized NodeInfo addNode(NodeID nodeId, 
      String hostName, Node node, Resource capability) {
    NodeManager nodeManager = new NodeManager(nodeId, hostName, node, capability);
    nodes.put(nodeManager.getHostName(), nodeManager);
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        clusterResource, nodeManager.getTotalCapability());
    return nodeManager;
  }

  @Override
  public synchronized boolean releaseContainer(ApplicationID applicationId, 
      Container container) {
    // Reap containers
    LOG.info("Application " + applicationId + " released container " + container);
    NodeManager nodeManager = nodes.get(container.hostName.toString());
    return nodeManager.releaseContainer(container);
  }
  
  @Override
  public synchronized NodeResponse nodeUpdate(NodeInfo nodeInfo, 
      Map<CharSequence,List<Container>> containers) {
    NodeManager node = nodes.get(nodeInfo.getHostName());
    LOG.debug("nodeUpdate: node=" + nodeInfo.getHostName() + 
        " available=" + nodeInfo.getAvailableResource().memory);
    return node.statusUpdate(containers);
    
  }

  @Override
  public synchronized void addAllocatedContainers(NodeInfo nodeInfo, 
      ApplicationID applicationId, List<Container> containers) {
    NodeManager node = nodes.get(nodeInfo.getHostName());
    node.allocateContainer(applicationId, containers);
  }

  @Override
  public synchronized void finishedApplication(ApplicationID applicationId,
      List<NodeInfo> nodesToNotify) {
    for (NodeInfo node: nodesToNotify) {
      NodeManager nodeManager = nodes.get(node.getHostName());
      nodeManager.notifyFinishedApplication(applicationId);
    }
  }
}