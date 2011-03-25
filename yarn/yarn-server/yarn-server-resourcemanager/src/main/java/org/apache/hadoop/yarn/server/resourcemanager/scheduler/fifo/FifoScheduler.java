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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerToken;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceRequest;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;

@LimitedPrivate("yarn")
@Evolving
public class FifoScheduler implements ResourceScheduler {
  
  private static final Log LOG = LogFactory.getLog(FifoScheduler.class);
  
  Configuration conf;
  private ContainerTokenSecretManager containerTokenSecretManager;
  
  // TODO: The memory-block size should be site-configurable?
  public static final int MINIMUM_MEMORY = 1024;
  private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
  private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
  
  public static final Resource MINIMUM_ALLOCATION = 
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(
        MINIMUM_MEMORY);
    
  Map<ApplicationID, Application> applications = 
    new TreeMap<ApplicationID, Application>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.ApplicationID.Comparator());

  private static final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return "default";
    }
  };
  
  public FifoScheduler() {}
  
  public FifoScheduler(Configuration conf,
      ContainerTokenSecretManager containerTokenSecretManager) 
  {
    reinitialize(conf, containerTokenSecretManager);
  }
  
  
  @Override
  public void reinitialize(Configuration conf,
      ContainerTokenSecretManager containerTokenSecretManager) 
  {
    this.conf = conf;
    this.containerTokenSecretManager = containerTokenSecretManager;
  }
  
  @Override
  public synchronized List<Container> allocate(ApplicationID applicationId,
      List<ResourceRequest> ask, List<Container> release) 
      throws IOException {
    Application application = getApplication(applicationId);
    if (application == null) {
      LOG.error("Calling allocate on removed " +
      		"or non existant application " + applicationId);
      return EMPTY_CONTAINER_LIST; 
    }
    normalizeRequests(ask);
    
    LOG.debug("allocate: pre-update" +
    		" applicationId=" + applicationId + 
        " application=" + application);
    application.showRequests();
    
    // Update application requests
    application.updateResourceRequests(ask);
    
    // Release containers
    releaseContainers(application, release);
    
    application.showRequests();
    
    List<Container> allContainers = application.acquire();
    LOG.debug("allocate:" +
    		" applicationId=" + applicationId + 
    		" #ask=" + ask.size() + 
    		" #release=" + release.size() +
    		" #allContainers=" + allContainers.size());
    return allContainers;
  }

  private void releaseContainers(Application application, List<Container> release) {
    application.releaseContainers(release);
    for (Container container : release) {
      releaseContainer(application.getApplicationId(), container);
    }
  }
  
  private void normalizeRequests(List<ResourceRequest> asks) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(ask);
    }
  }
  
  private void normalizeRequest(ResourceRequest ask) {
    int memory = ask.capability.memory;
    memory = 
      MINIMUM_MEMORY * ((memory/MINIMUM_MEMORY) + (memory%MINIMUM_MEMORY)); 
  }
  
  private synchronized Application getApplication(ApplicationID applicationId) {
    return applications.get(applicationId);
  }

  public synchronized void addApplication(ApplicationID applicationId, 
      String user, String unusedQueue, Priority unusedPriority) 
  throws IOException {
    applications.put(applicationId, 
        new Application(applicationId, DEFAULT_QUEUE, user));
    LOG.info("Application Submission: " + applicationId.id + " from " + user + 
        ", currently active: " + applications.size());
  }

  public synchronized void removeApplication(ApplicationID applicationId)
  throws IOException {
    Application application = getApplication(applicationId);
    if (application == null) {
      throw new IOException("Unknown application " + applicationId + 
          " has completed!");
    }
    
    // Release current containers
    releaseContainers(application, application.getCurrentContainers());
    
    // Let the cluster know that the applications are done
    finishedApplication(applicationId, 
        application.getAllNodesForApplication());
    
    // Remove the application
    applications.remove(applicationId);
  }
  
  /**
   * Heart of the scheduler...
   * 
   * @param node node on which resources are available to be allocated
   */
  private synchronized void assignContainers(NodeInfo node) {
    LOG.debug("assignContainers:" +
    		" node=" + node.getHostName() + 
        " #applications=" + applications.size());
    
    // Try to assign containers to applications in fifo order
    for (Map.Entry<ApplicationID, Application> e : applications.entrySet()) {
      Application application = e.getValue();
      LOG.debug("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        for (Priority priority : application.getPriorities()) {
          int maxContainers = 
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.OFF_SWITCH); 
          // Ensure the application needs containers of this priority
          if (maxContainers > 0) {
            int assignedContainers = 
              assignContainersOnNode(node, application, priority);
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0) {
              break;
            }
          }
        }
      }
      LOG.debug("post-assignContainers");
      application.showRequests();
      
      // Done
      if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.lessThan(
          node.getAvailableResource(), MINIMUM_ALLOCATION)) {
        return;
      }
    }
  }
  
  private int getMaxAllocatableContainers(Application application,
      Priority priority, NodeInfo node, NodeType type) {
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, NodeManager.ANY);
    int maxContainers = offSwitchRequest.numContainers;
    
    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }
    
    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = 
        application.getResourceRequest(priority, node.getRackName());
      if (rackLocalRequest == null) {
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers, rackLocalRequest.numContainers);
    }
    
    if (type == NodeType.DATA_LOCAL) {
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getHostName());
      if (nodeLocalRequest != null) {
        maxContainers = Math.min(maxContainers, nodeLocalRequest.numContainers);
      }
    }
    
    return maxContainers;
  }
  

  private int assignContainersOnNode(NodeInfo node, 
      Application application, Priority priority 
      ) {
    // Data-local
    int nodeLocalContainers = 
      assignNodeLocalContainers(node, application, priority); 

    // Rack-local
    int rackLocalContainers = 
      assignRackLocalContainers(node, application, priority);
    
    // Off-switch
    int offSwitchContainers =
      assignOffSwitchContainers(node, application, priority);
    

    LOG.debug("assignContainersOnNode:" +
        " node=" + node.getHostName() + 
        " application=" + application.getApplicationId().id +
        " priority=" + priority.priority + 
        " #assigned=" + 
          (nodeLocalContainers + rackLocalContainers + offSwitchContainers));
    

    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }
  
  private int assignNodeLocalContainers(NodeInfo node, 
      Application application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getHostName());
    if (request != null) {
      int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.DATA_LOCAL), 
            request.numContainers);
      assignedContainers = 
        assignContainers(node, application, priority, 
            assignableContainers, request, NodeType.DATA_LOCAL);
    }
    return assignedContainers;
  }
  
  private int assignRackLocalContainers(NodeInfo node, 
      Application application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRackName());
    if (request != null) {
      int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.RACK_LOCAL), 
            request.numContainers);
      assignedContainers = 
        assignContainers(node, application, priority, 
            assignableContainers, request, NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(NodeInfo node, 
      Application application, Priority priority) {
    int assignedContainers = 0;
    ResourceRequest request = 
      application.getResourceRequest(priority, NodeManager.ANY);
    if (request != null) {
      assignedContainers = 
        assignContainers(node, application, priority, 
            request.numContainers, request, NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }
  
  private int assignContainers(NodeInfo node, Application application, 
      Priority priority, int assignableContainers, 
      ResourceRequest request, NodeType type) {
    LOG.debug("assignContainers:" +
    		" node=" + node.getHostName() + 
    		" application=" + application.getApplicationId().id + 
        " priority=" + priority.priority + 
        " assignableContainers=" + assignableContainers +
        " request=" + request + " type=" + type);
    Resource capability = request.capability;
    
    int availableContainers = 
        node.getAvailableResource().memory / capability.memory; // TODO: A buggy
                                                                // application
                                                                // with this
                                                                // zero would
                                                                // crash the
                                                                // scheduler.
    int assignedContainers = 
      Math.min(assignableContainers, availableContainers);
    
    if (assignedContainers > 0) {
      List<Container> containers =
          new ArrayList<Container>(assignedContainers);
      for (int i=0; i < assignedContainers; ++i) {
        Container container =
            org.apache.hadoop.yarn.server.resourcemanager.resource.Container
                .create(application.getApplicationId(), 
                    application.getNewContainerId(),
                    node.getHostName(), capability);
        // If security is enabled, send the container-tokens too.
        if (UserGroupInformation.isSecurityEnabled()) {
          ContainerToken containerToken = new ContainerToken();
          ContainerTokenIdentifier tokenidentifier =
              new ContainerTokenIdentifier(container.id,
                  container.hostName.toString(), container.resource);
          containerToken.identifier =
              ByteBuffer.wrap(tokenidentifier.getBytes());
          containerToken.kind = ContainerTokenIdentifier.KIND.toString();
          containerToken.password =
              ByteBuffer.wrap(containerTokenSecretManager
                  .createPassword(tokenidentifier));
          containerToken.service = container.hostName; // TODO: port
          container.containerToken = containerToken;
        }
        containers.add(container);
      }
      application.allocate(type, node, priority, request, containers);
      addAllocatedContainers(node, application.getApplicationId(), containers);
    }
    return assignedContainers;
  }

  private synchronized void applicationCompletedContainers(
     List<Container> completedContainers) {
    for (Container c: completedContainers) {
      Application app = applications.get(c.id.appID);
      /** this is possible, since an application can be removed from scheduler but
       * the nodemanger is just updating about a completed container.
       */
      if (app != null) {
        app.completedContainer(c);
      }
    }
  }
  
  @Override
  public synchronized NodeResponse nodeUpdate(NodeInfo node, 
      Map<CharSequence,List<Container>> containers ) {
   
    NodeResponse nodeResponse = nodeUpdateInternal(node, containers);
    applicationCompletedContainers(nodeResponse.getCompletedContainers());
    LOG.info("Node heartbeat " + node.getNodeID() + " resource = " + node.getAvailableResource());
    if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.
        greaterThanOrEqual(node.getAvailableResource(), MINIMUM_ALLOCATION)) {
      assignContainers(node);
    }
    LOG.info("Node after allocation " + node.getNodeID() + " resource = "
      + node.getAvailableResource());

    // TODO: Add the list of containers to be preempted when we support
    // preemption.
    return nodeResponse;
  }  

  @Override
  public synchronized void handle(ASMEvent<ApplicationTrackerEventType> event) {
    switch(event.getType()) {
    case ADD:
      try {
        addApplication(event.getAppContext().getApplicationID(), event.getAppContext().getUser(),
            event.getAppContext().getQueue(), event.getAppContext().getSubmissionContext().priority);
      } catch(IOException ie) {
        LOG.error("Unable to add application " + event.getAppContext().getApplicationID(), ie);
        /** this is fatal we are not able to add applications for scheduling **/
        //TODO handle it later.
      }
      break;
    case REMOVE:
      try {
        
        removeApplication(event.getAppContext().getApplicationID());
      } catch(IOException ie) {
        LOG.error("Unable to remove application " + event.getAppContext().getApplicationID(), ie);
      }
      break;  
    }
  }
  
  private Map<String, NodeManager> nodes = new HashMap<String, NodeManager>();
  private Resource clusterResource = new Resource();
 
  public synchronized Resource getClusterResource() {
    return clusterResource;
  }

  @Override
  public synchronized void removeNode(NodeInfo nodeInfo) {
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        clusterResource, nodeInfo.getTotalCapability());
    //TODO inform the the applications that the containers are completed/failed
    nodes.remove(nodeInfo.getHostName());
  }
  
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

  public synchronized boolean releaseContainer(ApplicationID applicationId, 
      Container container) {
    // Reap containers
    LOG.info("Application " + applicationId + " released container " + container);
    NodeManager nodeManager = nodes.get(container.hostName.toString());
    return nodeManager.releaseContainer(container);
  }
  
  private synchronized NodeResponse nodeUpdateInternal(NodeInfo nodeInfo, 
      Map<CharSequence,List<Container>> containers) {
    NodeManager node = nodes.get(nodeInfo.getHostName());
    LOG.debug("nodeUpdate: node=" + nodeInfo.getHostName() + 
        " available=" + nodeInfo.getAvailableResource().memory);
    return node.statusUpdate(containers);
    
  }

  public synchronized void addAllocatedContainers(NodeInfo nodeInfo, 
      ApplicationID applicationId, List<Container> containers) {
    NodeManager node = nodes.get(nodeInfo.getHostName());
    node.allocateContainer(applicationId, containers);
  }

  public synchronized void finishedApplication(ApplicationID applicationId,
      List<NodeInfo> nodesToNotify) {
    for (NodeInfo node: nodesToNotify) {
      NodeManager nodeManager = nodes.get(node.getHostName());
      nodeManager.notifyFinishedApplication(applicationId);
    }
  }
}
