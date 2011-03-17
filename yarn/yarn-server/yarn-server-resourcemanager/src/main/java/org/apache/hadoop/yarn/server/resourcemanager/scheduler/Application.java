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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceRequest;

/**
 * This class keeps track of all the consumption of an application.
 * This also keeps track of current running/completed
 *  containers for the application.
 */
@LimitedPrivate("yarn")
@Evolving
public class Application {
  private static final Log LOG = LogFactory.getLog(Application.class);

  private AtomicInteger containerCtr = new AtomicInteger(0);

  final ApplicationID applicationId;
  final Queue queue;
  final String user;

  final Set<Priority> priorities = 
    new TreeSet<Priority>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  final Map<Priority, Map<String, ResourceRequest>> requests = 
    new HashMap<Priority, Map<String, ResourceRequest>>();
  final Resource currentConsumption = new Resource();
  final Resource overallConsumption = new Resource();

  /* Current consumption */
  List<Container> acquired = new ArrayList<Container>();
  List<Container> completedContainers = new ArrayList<Container>();
  /* Allocated by scheduler */
  List<Container> allocated = new ArrayList<Container>(); 
  Set<NodeInfo> applicationOnNodes = new HashSet<NodeInfo>();
  
  public Application(ApplicationID applicationId, Queue queue, String user) {
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user; 
  }

  public ApplicationID getApplicationId() {
    return applicationId;
  }

  public Queue getQueue() {
    return queue;
  }
  
  public String getUser() {
    return user;
  }

  public synchronized Map<Priority, Map<String, ResourceRequest>> getRequests() {
    return requests;
  }

  public int getNewContainerId() {
    return containerCtr.incrementAndGet();
  }

  /**
   * the currently acquired/allocated containers  by the application masters.
   * @return the current containers being used by the application masters.
   */
  public synchronized List<Container> getCurrentContainers() {
    List<Container> currentContainers =  new ArrayList<Container>(acquired);
    currentContainers.addAll(allocated);
    return currentContainers;
  }

  /**
   * The ApplicationMaster is acquiring the allocated/completed resources.
   * @return allocated resources
   */
  synchronized public List<Container> acquire() {
    // Return allocated containers
    acquired.addAll(allocated);
    List<Container> heartbeatContainers = allocated;
    allocated = new ArrayList<Container>();

    // Metrics
    for (Container container : heartbeatContainers) {
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
          overallConsumption, container.resource);
    }

    LOG.debug("acquire:" +
        " application=" + applicationId + 
        " #acquired=" + heartbeatContainers.size());
    heartbeatContainers =  (heartbeatContainers == null) ? 
        new ArrayList<Container>() : heartbeatContainers;

        heartbeatContainers.addAll(completedContainers);
        completedContainers.clear();
        return heartbeatContainers;
  }

  /**
   * The ApplicationMaster is updating resource requirements for the 
   * application, by asking for more resources and releasing resources 
   * acquired by the application.
   * @param requests resources to be acquired
   * @param release resources being released
   */
  synchronized public void updateResourceRequests(List<ResourceRequest> requests) {
    // Update resource requests
    for (ResourceRequest request : requests) {
      Priority priority = request.priority;
      String hostName = request.hostName.toString();

      Map<String, ResourceRequest> asks = this.requests.get(priority);

      if (asks == null) {
        asks = new HashMap<String, ResourceRequest>();
        this.requests.put(priority, asks);
        this.priorities.add(priority);
      }

      asks.put(hostName, request);

      if (hostName.equals(NodeManager.ANY)) {
        LOG.debug("update:" +
            " application=" + applicationId + 
            " request=" + request);
      }
    }
  }

  public synchronized void releaseContainers(List<Container> release) {
    // Release containers and update consumption 
    for (Container container : release) {
      LOG.debug("update: " +
          "application=" + applicationId + " released=" + container);
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
          currentConsumption, container.resource);
      for (Iterator<Container> i=acquired.iterator(); i.hasNext();) {
        Container c = i.next();
        if (c.id.equals(container.id)) {
          i.remove();
          LOG.info("Removed acquired container: " + container.id);
        }
      }
    }
  }

  synchronized public Collection<Priority> getPriorities() {
    return priorities;
  }

  synchronized public Map<String, ResourceRequest> 
  getResourceRequests(Priority priority) {
    return requests.get(priority);
  }

  synchronized public ResourceRequest getResourceRequest(Priority priority, 
      String node) {
    Map<String, ResourceRequest> nodeRequests = requests.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(node);
  }

  synchronized public void completedContainer(Container container) {
    LOG.info("Completed container: " + container);
    completedContainers.add(container);
  }

  synchronized public void completedContainers(List<Container> containers) {
    completedContainers.addAll(containers);
  }

  /**
   * Resources have been allocated to this application by the resource scheduler.
   * Track them.
   * @param type the type of the node
   * @param node the nodeinfo of the node
   * @param priority the priority of the request.
   * @param request the request
   * @param containers the containers allocated.
   */
  synchronized public void allocate(NodeType type, NodeInfo node, 
      Priority priority, ResourceRequest request, List<Container> containers) {
    applicationOnNodes.add(node);
    if (type == NodeType.DATA_LOCAL) {
      allocateNodeLocal(node, priority, request, containers);
    } else if (type == NodeType.RACK_LOCAL) {
      allocateRackLocal(node, priority, request, containers);
    } else {
      allocateOffSwitch(node, priority, request, containers);
    }
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources 
   * to the application.
   * @param allocatedContainers resources allocated to the application
   */
  synchronized private void allocateNodeLocal(NodeInfo node, 
      Priority priority, ResourceRequest nodeLocalRequest, 
      List<Container> containers) {
    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements
    nodeLocalRequest.numContainers -= containers.size();
    ResourceRequest rackLocalRequest = 
      requests.get(priority).get(node.getRackName());
    rackLocalRequest.numContainers -= containers.size();
    ResourceRequest offSwitchRequest = 
      requests.get(priority).get(NodeManager.ANY);
    offSwitchRequest.numContainers -= containers.size();
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources 
   * to the application.
   * @param allocatedContainers resources allocated to the application
   */
  synchronized private void allocateRackLocal(NodeInfo node, 
      Priority priority, ResourceRequest rackLocalRequest, 
      List<Container> containers) {

    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements
    rackLocalRequest.numContainers -= containers.size();
    ResourceRequest offSwitchRequest = 
      requests.get(priority).get(NodeManager.ANY);
    offSwitchRequest.numContainers -= containers.size();
  } 

  /**
   * The {@link ResourceScheduler} is allocating data-local resources 
   * to the application.
   * @param allocatedContainers resources allocated to the application
   */
  synchronized private void allocateOffSwitch(NodeInfo node, 
      Priority priority, ResourceRequest offSwitchRequest, 
      List<Container> containers) {

    // Update consumption and track allocations
    allocate(containers);

    // Update future requirements
    offSwitchRequest.numContainers -= containers.size();
  }

  synchronized private void allocate(List<Container> containers) {
    // Update consumption and track allocations
    for (Container container : containers) {
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
          currentConsumption, container.resource);

      allocated.add(container);

      LOG.debug("allocate: applicationId=" + applicationId + 
          " container=" + container.id + " host=" + container.hostName);
    }
  }

  synchronized public void showRequests() {
    for (Priority priority : getPriorities()) {
      Map<String, ResourceRequest> requests = getResourceRequests(priority);
      if (requests != null) {
        for (ResourceRequest request : requests.values()) {
          LOG.debug("showRequests:" +
              " application=" + applicationId + 
              " request=" + request);
        }
      }
    }
  }
  
  synchronized public List<NodeInfo> getAllNodesForApplication() {
    return new ArrayList<NodeInfo>(applicationOnNodes);
  }
}
