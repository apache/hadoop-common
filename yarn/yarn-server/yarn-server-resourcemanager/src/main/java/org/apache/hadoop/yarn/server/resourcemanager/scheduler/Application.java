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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;

/**
 * This class keeps track of all the consumption of an application.
 * This also keeps track of current running/completed
 *  containers for the application.
 */
@LimitedPrivate("yarn")
@Evolving
public class Application {
  private static final Log LOG = LogFactory.getLog(Application.class);
  final ApplicationId applicationId;
  final Queue queue;
  final String user;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  final Set<Priority> priorities = 
    new TreeSet<Priority>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  final Map<Priority, Map<String, ResourceRequest>> requests = 
    new HashMap<Priority, Map<String, ResourceRequest>>();
  final Resource currentConsumption = recordFactory.newRecordInstance(Resource.class);
  final Resource overallConsumption = recordFactory.newRecordInstance(Resource.class);

  /* Current consumption */
  List<Container> acquired = new ArrayList<Container>();
  List<Container> completedContainers = new ArrayList<Container>();
  /* Allocated by scheduler */
  List<Container> allocated = new ArrayList<Container>(); 
  Set<NodeInfo> applicationOnNodes = new HashSet<NodeInfo>();
  ApplicationMaster master;
  
  public Application(ApplicationId applicationId, ApplicationMaster master,
      Queue queue, String user) {
    this.applicationId = applicationId;
    this.queue = queue;
    this.user = user; 
    this.master = master;
  }

  public ApplicationId getApplicationId() {
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
    int i = master.getContainerCount();
    master.setContainerCount(++i);
    return master.getContainerCount();
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
          overallConsumption, container.getResource());
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
      Priority priority = request.getPriority();
      String hostName = request.getHostName();

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
          currentConsumption, container.getResource());
      for (Iterator<Container> i=acquired.iterator(); i.hasNext();) {
        Container c = i.next();
        if (c.getId().equals(container.getId())) {
          i.remove();
          LOG.info("Removed acquired container: " + container.getId());
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
      String nodeAddress) {
    Map<String, ResourceRequest> nodeRequests = requests.get(priority);
    return (nodeRequests == null) ? null : nodeRequests.get(nodeAddress);
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
    nodeLocalRequest.setNumContainers(nodeLocalRequest.getNumContainers() - containers.size());
    ResourceRequest rackLocalRequest = 
      requests.get(priority).get(node.getRackName());
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - containers.size());
    ResourceRequest offSwitchRequest = 
      requests.get(priority).get(NodeManager.ANY);
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers() - containers.size());
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
    rackLocalRequest.setNumContainers(rackLocalRequest.getNumContainers() - containers.size());
    ResourceRequest offSwitchRequest = 
      requests.get(priority).get(NodeManager.ANY);
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers() - containers.size());
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
    offSwitchRequest.setNumContainers(offSwitchRequest.getNumContainers() - containers.size());
  }

  synchronized private void allocate(List<Container> containers) {
    // Update consumption and track allocations
    for (Container container : containers) {
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
          currentConsumption, container.getResource());

      allocated.add(container);

      LOG.debug("allocate: applicationId=" + applicationId + 
          " container=" + container.getId() + " host=" + container.getContainerManagerAddress());
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


  synchronized public org.apache.hadoop.yarn.api.records.Application 
  getApplicationInfo() {
    org.apache.hadoop.yarn.api.records.Application application = 
      recordFactory.newRecordInstance(
          org.apache.hadoop.yarn.api.records.Application.class);
    application.setApplicationId(applicationId);
    application.setMasterHost("");
    application.setName("");
    application.setQueue(queue.getQueueName());
    application.setState(ApplicationState.RUNNING);
    application.setUser(user);

    ApplicationStatus status = 
      recordFactory.newRecordInstance(ApplicationStatus.class);
    status.setApplicationId(applicationId);
    application.setStatus(status);

    return application;
  }
}
