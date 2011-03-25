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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;

@LimitedPrivate("yarn")
@Evolving
public class CapacityScheduler 
implements ResourceScheduler, CapacitySchedulerContext {

  private static final Log LOG = LogFactory.getLog(CapacityScheduler.class);

  private Queue root;

  private final static List<Container> EMPTY_CONTAINER_LIST = 
    new ArrayList<Container>();

  private final Comparator<Queue> queueComparator = new Comparator<Queue>() {
    @Override
    public int compare(Queue q1, Queue q2) {
      if (q1.getUtilization() < q2.getUtilization()) {
        return -1;
      } else if (q1.getUtilization() > q2.getUtilization()) {
        return 1;
      }

      return q1.getQueuePath().compareTo(q2.getQueuePath());
    }
  };

  private final Comparator<Application> applicationComparator = 
    new Comparator<Application>() {
    @Override
    public int compare(Application a1, Application a2) {
      return a1.getApplicationId().id - a2.getApplicationId().id;
    }
  };

  private CapacitySchedulerConfiguration conf;
  private ContainerTokenSecretManager containerTokenSecretManager;

  private Map<String, Queue> queues = new ConcurrentHashMap<String, Queue>();


  private Resource minimumAllocation;

  private Map<ApplicationID, Application> applications = 
    new TreeMap<ApplicationID, Application>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.ApplicationID.Comparator());


  public Queue getRootQueue() {
    return root;
  }

  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public ContainerTokenSecretManager getContainerTokenSecretManager() {
    return containerTokenSecretManager;
  }

  @Override
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  @Override
  public void reinitialize(Configuration conf,
      ContainerTokenSecretManager containerTokenSecretManager) {
    this.conf = new CapacitySchedulerConfiguration(conf);
    this.minimumAllocation = this.conf.getMinimumAllocation();
    this.containerTokenSecretManager = containerTokenSecretManager;

    initializeQueues(this.conf);
  }

  @Private
  public static final String ROOT = "root";

  @Private
  public static final String ROOT_QUEUE = 
    CapacitySchedulerConfiguration.PREFIX + ROOT;

  private void initializeQueues(CapacitySchedulerConfiguration conf) {
    root = parseQueue(conf, null, ROOT);
    LOG.info("Initialized root queue " + root);
  }

  private Queue parseQueue(CapacitySchedulerConfiguration conf, 
      Queue parent, String queueName) {
    Queue queue;
    String[] childQueueNames = 
      conf.getQueues((parent == null) ? 
          queueName : (parent.getQueuePath()+"."+queueName));
    if (childQueueNames == null || childQueueNames.length == 0) {
      queue = new LeafQueue(this, queueName, parent, applicationComparator);
    } else {
      ParentQueue parentQueue = 
        new ParentQueue(this, queueName, queueComparator, parent);
      List<Queue> childQueues = new ArrayList<Queue>();
      for (String childQueueName : childQueueNames) {
        Queue childQueue = 
          parseQueue(
              conf, 
              parentQueue, 
              childQueueName);
        childQueues.add(childQueue);

        queues.put(childQueueName, childQueue);
      }
      parentQueue.setChildQueues(childQueues);

      queue = parentQueue;
    }

    LOG.info("Initialized queue: " + queue);
    return queue;
  }
  
  /**
   * Add an application to the capacity scheduler. This application needs to be 
   * tracked. 
   * @param applicationId the application id of this application
   * @param user the user who owns the application
   * @param queueName the queue which the application belongs to
   * @param priority the priority of the application
   * @throws IOException
   */
  public void addApplication(ApplicationID applicationId, 
      String user, String queueName, Priority priority)
  throws IOException {
    Queue queue = queues.get(queueName);

    if (queue == null) {
      throw new IOException("Application " + applicationId + 
          " submitted by user " + user + " to unknown queue: " + queueName);
    }

    if (!(queue instanceof LeafQueue)) {
      throw new IOException("Application " + applicationId + 
          " submitted by user " + user + " to non-leaf queue: " + queueName);
    }

    Application application = new Application(applicationId, queue, user); 
    try {
      queue.submitApplication(application, user, queueName, priority);
    } catch (AccessControlException ace) {
      throw new IOException(ace);
    }

    applications.put(applicationId, application);

    LOG.info("Application Submission: " + applicationId.id + 
        ", user: " + user +
        " queue: " + queue +
        ", currently active: " + applications.size());
  }

  /**
   * Remove an application. Releases the resources of the application and 
   * then makes sure its removed from data structures of the scheduler.
   * @param applicationId the applicationId of the application
   * @throws IOException
   */
  public void removeApplication(ApplicationID applicationId)
  throws IOException {
    Application application = getApplication(applicationId);

    if (application == null) {
      //      throw new IOException("Unknown application " + applicationId + 
      //          " has completed!");
      LOG.info("Unknown application " + applicationId + " has completed!");
      return;
    }

    // Inform the queue
    Queue queue = queues.get(application.getQueue().getQueueName());
    LOG.info("DEBUG --- removeApplication - appId: " + applicationId + " queue: " + queue);
    queue.finishApplication(application, queue.getQueueName());

    // Release containers and update queue capacities
    processReleasedContainers(application, application.getCurrentContainers());

    // Inform all NodeManagers about completion of application
    finishedApplication(applicationId, 
        application.getAllNodesForApplication());

    // Remove from our data-structure
    applications.remove(applicationId);
  }

  @Override
  public List<Container> allocate(ApplicationID applicationId,
      List<ResourceRequest> ask, List<Container> release)
      throws IOException {

    Application application = getApplication(applicationId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + applicationId);
      return EMPTY_CONTAINER_LIST; 
    }
    normalizeRequests(ask);

    LOG.info("DEBUG --- allocate: pre-update" +
        " applicationId=" + applicationId + 
        " application=" + application);
    application.showRequests();

    // Update application requests
    application.updateResourceRequests(ask);

    // Release ununsed containers and update queue capacities
    processReleasedContainers(application, release);

    LOG.info("DEBUG --- allocate: post-update");
    application.showRequests();

    List<Container> allContainers = application.acquire();
    LOG.info("DEBUG --- allocate:" +
        " applicationId=" + applicationId + 
        " #ask=" + ask.size() + 
        " #release=" + release.size() +
        " #allContainers=" + allContainers.size());
    return allContainers;
  }

  private void normalizeRequests(List<ResourceRequest> asks) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(ask);
    }
  }

  private void normalizeRequest(ResourceRequest ask) {
    int memory = ask.capability.memory;
    int minMemory = minimumAllocation.memory;
    ask.capability.memory =
      minMemory * ((memory/minMemory) + (memory%minMemory > 0 ? 1 : 0));
  }


  @Override
  public synchronized NodeResponse nodeUpdate(NodeInfo node, 
      Map<CharSequence,List<Container>> containers ) {

    LOG.info("nodeUpdate: " + node);

    NodeResponse nodeResponse = nodeUpdateInternal(node, containers);

    // Completed containers
    processCompletedContainers(nodeResponse.getCompletedContainers());
    NodeManager nm = nodes.get(node.getHostName());
    // Assign new containers
    root.assignContainers(clusterResource, nm);

    return nodeResponse;
  }

  private synchronized void processCompletedContainers(
      List<Container> completedContainers) {
    for (Container container: completedContainers) {
      Application application = getApplication(container.id.appID);

      // this is possible, since an application can be removed from scheduler 
      // but the nodemanger is just updating about a completed container.
      if (application != null) {

        // Inform the queue
        LeafQueue queue = (LeafQueue)application.getQueue();
        queue.completedContainer(clusterResource, container, application);
      }
    }
  }

  private synchronized void processReleasedContainers(Application application,
      List<Container> releasedContainers) {
    // Inform the application
    application.releaseContainers(releasedContainers);

    // Inform clusterTracker
    List<Container> unusedContainers = new ArrayList<Container>();
    for (Container container : releasedContainers) {
      if (releaseContainer(
          application.getApplicationId(), 
          container)) {
        unusedContainers.add(container);
      }
    }

    // Update queue capacities
    processCompletedContainers(unusedContainers);
  }

  private synchronized Application getApplication(ApplicationID applicationId) {
    return applications.get(applicationId);
  }

  @Override
  public synchronized void handle(ASMEvent<ApplicationTrackerEventType> event) {
    switch(event.getType()) {
    case ADD:
      try {
        addApplication(event.getAppContext().getApplicationID(), 
            event.getAppContext().getUser(), event.getAppContext().getQueue(),
            event.getAppContext().getSubmissionContext().priority);
      } catch(IOException ie) {
        LOG.error("Error in adding an application to the scheduler", ie);
        //TODO do proper error handling to shutdown the Resource Manager is we 
        // are not able to handle this.
      }
      break;
    case REMOVE:
      try {
        removeApplication(event.getAppContext().getApplicationID());
      } catch(IOException ie) {
        LOG.error("Error in removing application", ie);
        //TODO have to be shutdown the RM in case of this.
        // do a graceful shutdown.
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
    //TODO inform the applications that the containers are completed/failed
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
  
  public synchronized NodeResponse nodeUpdateInternal(NodeInfo nodeInfo, 
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
