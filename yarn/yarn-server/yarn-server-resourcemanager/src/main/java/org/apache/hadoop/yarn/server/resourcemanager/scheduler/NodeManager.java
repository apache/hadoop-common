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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;

/**
 * This class is used by ClusterInfo to keep track of all the applications/containers
 * running on a node.
 *
 */
@Private
@Unstable
public class NodeManager implements NodeInfo {
  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  private final NodeID nodeId;
  private final String hostName;
  private Resource totalCapability;
  private Resource availableResource = new Resource();
  private Resource usedResource = new Resource();
  private final Node node;
  
  private static final Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
  private static final List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
  private static final ApplicationID[] EMPTY_APPLICATION_ARRAY = new ApplicationID[]{};
  private static final List<ApplicationID> EMPTY_APPLICATION_LIST = Arrays.asList(EMPTY_APPLICATION_ARRAY);
  
  public static final String ANY = "*";  
  /* set of containers that are allocated containers */
  private final Map<ContainerID, Container> allocatedContainers = 
    new TreeMap<ContainerID, Container>();
    
  /* set of containers that are currently active on a node manager */
  private final Map<ContainerID, Container> activeContainers =
    new TreeMap<ContainerID, Container>();
  
  /* set of containers that need to be cleaned */
  private final Set<Container> containersToClean = 
    new TreeSet<Container>(new org.apache.hadoop.yarn.server.resourcemanager.resource.Container.Comparator());

  
  /* the list of applications that have finished and need to be purged */
  private final List<ApplicationID> finishedApplications = new ArrayList<ApplicationID>();
  
  private volatile int numContainers;
  
  public NodeManager(NodeID nodeId, String hostname, 
      Node node, Resource capability) {
    this.nodeId = nodeId;   
    this.totalCapability = capability; 
    this.hostName = hostname;
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        availableResource, capability);
    this.node = node;
  }

  /**
   * NodeInfo for this node.
   * @return the {@link NodeInfo} for this node.
   */
  public NodeInfo getNodeInfo() {
    return this;
  }
  
  /**
   * The Scheduler has allocated containers on this node to the 
   * given application.
   * 
   * @param applicationId application
   * @param containers allocated containers
   */
  public synchronized void allocateContainer(ApplicationID applicationId, 
      List<Container> containers) {
    if (containers == null) {
      LOG.error("Adding null containers for application " + applicationId);
      return;
    }   
    for (Container container : containers) {
      allocateContainer(container);
    }

    LOG.info("addContainers:" +
        " node=" + getHostName() + 
        " #containers=" + containers.size() + 
        " available=" + getAvailableResource().memory + 
        " used=" + getUsedResource().memory);
  }

  /**
   * Status update from the NodeManager
   * @param nodeStatus node status
   * @return the set of containers no longer should be used by the
   * node manager.
   */
  public synchronized NodeResponse 
    statusUpdate(Map<CharSequence,List<Container>> allContainers) {

    if (allContainers == null) {
      return new NodeResponse(EMPTY_APPLICATION_LIST, EMPTY_CONTAINER_LIST,
          EMPTY_CONTAINER_LIST);
    }
       
    List<Container> listContainers = new ArrayList<Container>();
    // Iterate through the running containers and update their status
    for (Map.Entry<CharSequence, List<Container>> e : 
      allContainers.entrySet()) {
      listContainers.addAll(e.getValue());
    }
    NodeResponse statusCheck = update(listContainers);
    return statusCheck;
  }
  
  /**
   * Status update for an application running on a given node
   * @param node node
   * @param containers containers update.
   * @return containers that are completed or need to be preempted.
   */
  private synchronized NodeResponse update(List<Container> containers) {
    List<Container> completedContainers = new ArrayList<Container>();
    List<Container> containersToCleanUp = new ArrayList<Container>();
    List<ApplicationID> lastfinishedApplications = new ArrayList<ApplicationID>();
    
    for (Container container : containers) {
      if (allocatedContainers.remove(container.id) != null) {
        activeContainers.put(container.id, container);
        LOG.info("Activated container " + container.id + " on node " + 
         getHostName());
      }

      if (container.state == ContainerState.COMPLETE) {
        if (activeContainers.remove(container.id) != null) {
          updateResource(container);
          LOG.info("Completed container " + container);
        }
        completedContainers.add(container);
        LOG.info("Removed completed container " + container.id + " on node " + 
            getHostName());
      }
      else if (container.state != ContainerState.COMPLETE && 
          (!allocatedContainers.containsKey(container.id)) && 
          !activeContainers.containsKey(container.id)) {
        containersToCleanUp.add(container);
      }
    }
    containersToCleanUp.addAll(containersToClean);
    /* clear out containers to clean */
    containersToClean.clear();
    lastfinishedApplications.addAll(finishedApplications);
    return new NodeResponse(lastfinishedApplications, completedContainers, 
        containersToCleanUp);
  }
  
  private synchronized void allocateContainer(Container container) {
    deductAvailableResource(container.resource);
    ++numContainers;
    
    allocatedContainers.put(container.id, container);
    LOG.info("Allocated container " + container.id + 
        " to node " + getHostName());
    
    LOG.info("Assigned container " + container.id + 
        " of capacity " + container.resource + " on host " + getHostName() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + 
        getAvailableResource() + " available");
  }

  private synchronized boolean isValidContainer(Container c) {    
    if (activeContainers.containsKey(c.id) || allocatedContainers.containsKey(c.id))
      return true;
    return false;
  }

  private synchronized void updateResource(Container container) {
    addAvailableResource(container.resource);
    --numContainers;
  }
  
  /**
   * Release an allocated container on this node.
   * @param container container to be released
   * @return <code>true</code> iff the container was unused, 
   *         <code>false</code> otherwise
   */
  public synchronized boolean releaseContainer(Container container) {
    if (!isValidContainer(container)) {
      LOG.error("Invalid container released " + container);
      return false;
    }
    
    /* remove the containers from the nodemanger */
    
    // Was this container launched?
    activeContainers.remove(container.id);
    allocatedContainers.remove(container.id);
    containersToClean.add(container);
    updateResource(container);

    LOG.info("Released container " + container.id + 
        " of capacity " + container.resource + " on host " + getHostName() + 
        ", which currently has " + numContainers + " containers, " + 
        getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
    return true;
  }

  @Override
  public NodeID getNodeID() {
    return this.nodeId;
  }

  @Override
  public String getHostName() {
    return this.hostName;
  }

  @Override
  public Resource getTotalCapability() {
   return this.totalCapability;
  }

  @Override
  public String getRackName() {
    return node.getNetworkLocation();
  }

  @Override
  public Node getNode() {
    return this.node;
  }

  @Override
  public synchronized Resource getAvailableResource() {
    return this.availableResource;
  }

  @Override
  public synchronized Resource getUsedResource() {
    return this.usedResource;
  }

  public synchronized void addAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for " + this.hostName);
      return;
    }
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        availableResource, resource);
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        usedResource, resource);
  }

  public synchronized void deductAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "+ this.hostName);
    }
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        availableResource, resource);
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
        usedResource, resource);
  }

  public synchronized void notifyFinishedApplication(ApplicationID applicationId) {  
    finishedApplications.add(applicationId);
    /* make sure to iterate through the list and remove all the containers that 
     * belong to this application.
     */
  }

  @Override
  public int getNumContainers() {
    return numContainers;
  }
  
  @Override
  public String toString() {
    return "host: " + getHostName() + " #containers=" + getNumContainers() +  
      " available=" + getAvailableResource().memory + 
      " used=" + getUsedResource().memory;
  }
 }
