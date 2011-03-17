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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerManager;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.ContainerStatus;
import org.apache.hadoop.yarn.HeartbeatResponse;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.NodeStatus;
import org.apache.hadoop.yarn.RegistrationResponse;
import org.apache.hadoop.yarn.Resource;

@Private
public class NodeManager implements ContainerManager {
  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  
  final private String hostName;
  final private String rackName;
  final private NodeID nodeId;
  final private Resource capability;
  Resource available = new Resource();
  Resource used = new Resource();

  final RMResourceTrackerImpl resourceTracker;
  final NodeInfo nodeInfo;
  final Map<CharSequence, List<Container>> containers = 
    new HashMap<CharSequence, List<Container>>();
  
  public NodeManager(String hostName, String rackName, int memory, 
      RMResourceTrackerImpl resourceTracker) throws IOException {
    this.hostName = hostName;
    this.rackName = rackName;
    this.resourceTracker = resourceTracker;
    this.capability = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(
          memory);
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        available, capability);

    RegistrationResponse response =
        resourceTracker.registerNodeManager(hostName, capability);
    this.nodeId = response.nodeID;
    this.nodeInfo = resourceTracker.getNodeManager(nodeId);
   
    // Sanity check
    Assert.assertEquals(memory, 
       nodeInfo.getAvailableResource().memory);
  }
  
  public String getHostName() {
    return hostName;
  }

  public String getRackName() {
    return rackName;
  }

  public NodeID getNodeId() {
    return nodeId;
  }

  public Resource getCapability() {
    return capability;
  }

  public Resource getAvailable() {
    return available;
  }
  
  public Resource getUsed() {
    return used;
  }
  
  int responseID = 0;
  
  public void heartbeat() throws AvroRemoteException {
    NodeStatus nodeStatus = 
      org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeStatus.createNodeStatus(
          nodeId, containers);
    nodeStatus.responseId = responseID;
    HeartbeatResponse response = resourceTracker.nodeHeartbeat(nodeStatus);
    responseID = response.responseId;
  }

  @Override
  synchronized public Void cleanupContainer(ContainerID containerID)
      throws AvroRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  synchronized public Void startContainer(ContainerLaunchContext containerLaunchContext)
      throws AvroRemoteException {
    String applicationId = String.valueOf(containerLaunchContext.id.appID.id);

    List<Container> applicationContainers = containers.get(applicationId);
    if (applicationContainers == null) {
      applicationContainers = new ArrayList<Container>();
      containers.put(applicationId, applicationContainers);
    }
    
    // Sanity check
    for (Container container : applicationContainers) {
      if (container.id.compareTo(containerLaunchContext.id) == 0) {
        throw new IllegalStateException(
            "Container " + containerLaunchContext.id + 
            " already setup on node " + hostName);
      }
    }

    Container container = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Container.create(
          containerLaunchContext.id, 
          hostName, containerLaunchContext.resource);
    applicationContainers.add(container);
    
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        available, containerLaunchContext.resource);
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        used, containerLaunchContext.resource);
    
    LOG.info("DEBUG --- startContainer:" +
        " node=" + hostName +
        " application=" + applicationId + 
        " container=" + container +
        " available=" + available +
        " used=" + used);

    return null;
  }

  synchronized public void checkResourceUsage() {
    LOG.info("Checking resource usage for " + hostName);
    Assert.assertEquals(available.memory, 
        nodeInfo.getAvailableResource().memory);
    Assert.assertEquals(used.memory, 
        nodeInfo.getUsedResource().memory);
  }
  
  @Override
  synchronized public Void stopContainer(ContainerID containerID) throws AvroRemoteException {
    String applicationId = String.valueOf(containerID.appID.id);
    
    // Mark the container as COMPLETE
    List<Container> applicationContainers = containers.get(applicationId);
    for (Container c : applicationContainers) {
      if (c.id.compareTo(containerID) == 0) {
        c.state = ContainerState.COMPLETE;
      }
    }
    
    // Send a heartbeat
    heartbeat();
    
    // Remove container and update status
    int ctr = 0;
    Container container = null;
    for (Iterator<Container> i=applicationContainers.iterator(); i.hasNext();) {
      container = i.next();
      if (container.id.compareTo(containerID) == 0) {
        i.remove();
        ++ctr;
      }
    }
    
    if (ctr != 1) {
      throw new IllegalStateException("Container " + containerID + 
          " stopped " + ctr + " times!");
    }
    
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        available, container.resource);
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        used, container.resource);

    LOG.info("DEBUG --- stopContainer:" +
        " node=" + hostName +
        " application=" + applicationId + 
        " container=" + containerID +
        " available=" + available +
        " used=" + used);

    return null;
  }

  @Override
  synchronized public ContainerStatus getContainerStatus(ContainerID containerID)
      throws AvroRemoteException {
    // TODO Auto-generated method stub
    return null;
  }
}
