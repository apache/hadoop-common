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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;

@Private
@Unstable
public class LeafQueue implements Queue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);
  
  private final String queueName;
  private final Queue parent;
  private float capacity;
  private float absoluteCapacity;
  private float maximumCapacity;
  private float absoluteMaxCapacity;
  private int userLimit;
  private float userLimitFactor;
  
  private int maxApplications;
  private int maxApplicationsPerUser;
  
  private Resource usedResources = 
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(0);
  private float utilization = 0.0f;
  private float usedCapacity = 0.0f;
  private volatile int numContainers;
  
  Set<Application> applications;
  
  public final Resource minimumAllocation;

  private ContainerTokenSecretManager containerTokenSecretManager;

  private Map<String, User> users = new HashMap<String, User>();
  
  private QueueInfo queueInfo; 
  private Map<ApplicationId, org.apache.hadoop.yarn.api.records.Application> 
  applicationInfos;
  
  private QueueState state;
  
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, Queue parent, 
      Comparator<Application> applicationComparator) {
    this.queueName = queueName;
    this.parent = parent;
    
    this.minimumAllocation = cs.getMinimumAllocation();
    this.containerTokenSecretManager = cs.getContainerTokenSecretManager();

    float capacity = 
      (float)cs.getConfiguration().getCapacity(getQueuePath()) / 100;
    float absoluteCapacity = parent.getAbsoluteCapacity() * capacity;
    
    float maximumCapacity = cs.getConfiguration().getMaximumCapacity(getQueuePath());
    float absoluteMaxCapacity = 
      (maximumCapacity == CapacitySchedulerConfiguration.UNDEFINED) ? 
          Float.MAX_VALUE : (parent.getAbsoluteCapacity() * maximumCapacity) / 100;

    int userLimit = cs.getConfiguration().getUserLimit(getQueuePath());
    float userLimitFactor = 
      cs.getConfiguration().getUserLimitFactor(getQueuePath());
    
    int maxSystemJobs = cs.getConfiguration().getMaximumSystemApplications();
    int maxApplications = (int)(maxSystemJobs * absoluteCapacity);
    int maxApplicationsPerUser = 
      (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor);

    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setQueueName(queueName);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());
    
    this.applicationInfos = 
      new HashMap<ApplicationId, 
      org.apache.hadoop.yarn.api.records.Application>();

    QueueState state = cs.getConfiguration().getState(getQueuePath());
    
    setupQueueConfigs(capacity, absoluteCapacity, 
        maximumCapacity, absoluteMaxCapacity, 
        userLimit, userLimitFactor, 
        maxApplications, maxApplicationsPerUser,
        state);

    LOG.info("DEBUG --- LeafQueue:" +
    		" name=" + queueName + 
    		", fullname=" + getQueuePath());
    
    this.applications = new TreeSet<Application>(applicationComparator);
  }
  
  private synchronized void setupQueueConfigs(
          float capacity, float absoluteCapacity, 
          float maxCapacity, float absoluteMaxCapacity,
          int userLimit, float userLimitFactor,
          int maxApplications, int maxApplicationsPerUser,
          QueueState state)
  {
    this.capacity = capacity; 
    this.absoluteCapacity = parent.getAbsoluteCapacity() * capacity;

    this.maximumCapacity = maxCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;
    
    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;

    this.maxApplications = maxApplications;
    this.maxApplicationsPerUser = maxApplicationsPerUser;

    this.state = state;
    
    this.queueInfo.setCapacity(capacity);
    this.queueInfo.setMaximumCapacity(maximumCapacity);
    this.queueInfo.setQueueState(state);
    
    LOG.info(queueName +
        ", capacity=" + capacity + 
        ", asboluteCapacity=" + absoluteCapacity + 
        ", maxCapacity=" + maxCapacity +
        ", asboluteMaxCapacity=" + absoluteMaxCapacity +
        ", userLimit=" + userLimit + ", userLimitFactor=" + userLimitFactor + 
        ", maxApplications=" + maxApplications + 
        ", maxApplicationsPerUser=" + maxApplicationsPerUser + 
        ", state=" + state);
  }
      

  @Override
  public float getCapacity() {
    return capacity;
  }

  @Override
  public float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public float getMaximumCapacity() {
    return maximumCapacity;
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return absoluteMaxCapacity;
  }

  @Override
  public Queue getParent() {
    return parent;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public String getQueuePath() {
    return parent.getQueuePath() + "." + getQueueName();
  }

  @Override
  public float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
  }

  @Override
  public synchronized float getUtilization() {
    return utilization;
  }

  @Override
  public synchronized List<Application> getApplications() {
    return new ArrayList<Application>(applications);
  }

  @Override
  public List<Queue> getChildQueues() {
    return null;
  }

  synchronized void setUtilization(float utilization) {
    this.utilization = utilization;
  }

  synchronized void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }
  
  public synchronized int getNumApplications() {
    return applications.size();
  }
  
  public int getNumContainers() {
    return numContainers;
  }

  @Override
  public QueueState getState() {
    return state;
  }

  @Override
  public synchronized QueueInfo getQueueInfo(boolean includeApplications, 
      boolean includeChildQueues, boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);
    
    if (includeApplications) {
      queueInfo.setApplications( 
        new ArrayList<org.apache.hadoop.yarn.api.records.Application>(
            applicationInfos.values()));
    } else {
      queueInfo.setApplications(
          new ArrayList<org.apache.hadoop.yarn.api.records.Application>());
    }
    
    return queueInfo;
  }

  public String toString() {
    return queueName + ":" + capacity + ":" + absoluteCapacity + ":" + 
      getUsedCapacity() + ":" + getUtilization() + ":" + 
      getNumApplications() + ":" + getNumContainers();
  }

  private synchronized User getUser(String userName) {
    User user = users.get(userName);
    if (user == null) {
      user = new User();
      users.put(userName, user);
    }
    return user;
  }
  
  @Override
  public synchronized void reinitialize(Queue queue, Resource clusterResource) 
  throws IOException {
    // Sanity check
    if (!(queue instanceof LeafQueue) || 
        !queue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() + 
          " from " + queue.getQueuePath());
    }
    
    LeafQueue leafQueue = (LeafQueue)queue;
    setupQueueConfigs(leafQueue.capacity, leafQueue.absoluteCapacity, 
        leafQueue.maximumCapacity, leafQueue.absoluteMaxCapacity, 
        leafQueue.userLimit, leafQueue.userLimitFactor, 
        leafQueue.maxApplications, leafQueue.maxApplicationsPerUser,
        leafQueue.state);
    
    update(clusterResource);
  }

  @Override
  public void submitApplication(Application application, String userName,
      String queue, Priority priority) 
  throws AccessControlException {
    // Careful! Locking order is important!
    User user = null;
    
    synchronized (this) {
      
      if (state != QueueState.RUNNING) {
        throw new AccessControlException("Queue " + getQueuePath() +
            " is STOPPED. Cannot accept submission of application: " +
            application.getApplicationId());
      }
      
      // Check submission limits for queues
      if (getNumApplications() >= maxApplications) {
        throw new AccessControlException("Queue " + getQueuePath() + 
            " already has " + getNumApplications() + " applications," +
            " cannot accept submission of application: " + 
            application.getApplicationId());
      }

      // Check submission limits for the user on this queue
      user = getUser(userName);
      if (user.getApplications() >= maxApplicationsPerUser) {
        throw new AccessControlException("Queue " + getQueuePath() + 
            " already has " + user.getApplications() + 
            " applications from user " + userName + 
            " cannot accept submission of application: " + 
            application.getApplicationId());
      }

      // Add the application to our data-structures
      addApplication(application, user);
    }

    // Inform the parent queue
    try {
      parent.submitApplication(application, userName, queue, priority);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application to parent-queue: " + 
          parent.getQueuePath(), ace);
      removeApplication(application, user);
      throw ace;
    }
  }

  private synchronized void addApplication(Application application, User user) {
    // Accept 
    user.submitApplication();
    applications.add(application);
    applicationInfos.put(application.getApplicationId(), 
        application.getApplicationInfo());

    LOG.info("Application added -" +
        " appId: " + application.getApplicationId() +
        " user: " + user + "," + " leaf-queue: " + getQueueName() +
        " #user-applications: " + user.getApplications() + 
        " #queue-applications: " + getNumApplications());

  }
  
  @Override
  public void finishApplication(Application application, String queue) 
  throws AccessControlException {
    // Careful! Locking order is important!
    synchronized (this) {
      removeApplication(application, getUser(application.getUser()));
    }
    
    // Inform the parent queue
    parent.finishApplication(application, queue);
  }
  
  public synchronized void removeApplication(Application application, User user) {
    applications.remove(application);
    
    user.finishApplication();
    if (user.getApplications() == 0) {
      users.remove(application.getUser());
    }
    
    applicationInfos.remove(application.getApplicationId());

    LOG.info("Application removed -" +
        " appId: " + application.getApplicationId() + 
        " user: " + application.getUser() + 
        " queue: " + getQueueName() +
        " #user-applications: " + user.getApplications() + 
        " #queue-applications: " + getNumApplications());
  }
  
  @Override
  public synchronized Resource 
  assignContainers(Resource clusterResource, NodeManager node) {
  
    LOG.info("DEBUG --- assignContainers:" +
        " node=" + node.getNodeAddress() + 
        " #applications=" + applications.size());
    
    // Try to assign containers to applications in fifo order
    for (Application application : applications) {
  
      LOG.info("DEBUG --- pre-assignContainers");
      application.showRequests();
      
      synchronized (application) {
        for (Priority priority : application.getPriorities()) {

          // Do we need containers at this 'priority'?
          if (!needContainers(application, priority)) {
            continue;
          }
          
          // Are we going over limits by allocating to this application?
          ResourceRequest required = 
            application.getResourceRequest(priority, NodeManager.ANY);
          if (required != null && required.getNumContainers() > 0) {
            
            // Maximum Capacity of the queue
            if (!assignToQueue(clusterResource, required.getCapability())) {
              return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
            }
            
            // User limits
            if (!assignToUser(application.getUser(), clusterResource, required.getCapability())) {
              return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
            }
            
          }
          
          Resource assigned = 
            assignContainersOnNode(clusterResource, node, application, priority);
  
          if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThan(
                assigned, 
                org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE)) {
            Resource assignedResource = 
              application.getResourceRequest(priority, NodeManager.ANY).getCapability();
            
            // Book-keeping
            allocateResource(clusterResource, 
                application.getUser(), assignedResource);
            
            // Done
            return assignedResource; 
          } else {
            // Do not assign out of order w.r.t priorities
            break;
          }
        }
      }
      
      LOG.info("DEBUG --- post-assignContainers");
      application.showRequests();
    }
  
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
  }

  private synchronized boolean assignToQueue(Resource clusterResource, 
      Resource required) {
    float newUtilization = 
      (float)(usedResources.getMemory() + required.getMemory()) / 
        (clusterResource.getMemory() * absoluteCapacity);
    if (newUtilization > absoluteMaxCapacity) {
      LOG.info(getQueueName() + 
          " current-capacity (" + getUtilization() + ") +" +
          " required (" + required.getMemory() + ")" +
          " > max-capacity (" + absoluteMaxCapacity + ")");
      return false;
    }
    return true;
  }
  
  private synchronized boolean assignToUser(String userName, Resource clusterResource,
      Resource required) {
    // What is our current capacity? 
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)

    // Allow progress for queues with miniscule capacity
    final int queueCapacity = 
      Math.max(
          divideAndCeil(
              (int)(absoluteCapacity * clusterResource.getMemory()), 
              minimumAllocation.getMemory()) 
              * minimumAllocation.getMemory(),           // round up 
          required.getMemory());

    final int consumed = usedResources.getMemory();
    final int currentCapacity = 
      (consumed < queueCapacity) ? queueCapacity : (consumed + required.getMemory());
    
    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    final int activeUsers = users.size();  
    User user = getUser(userName);
    
    int limit = 
      Math.min(
          Math.max(divideAndCeil(currentCapacity, activeUsers), 
                   divideAndCeil((int)userLimit*currentCapacity, 100)),
          (int)(queueCapacity * userLimitFactor)
          );

    // Note: We aren't considering the current request since there is a fixed
    // overhead of the AM, so... 
    if ((user.getConsumedResources().getMemory()) > limit) {
      LOG.info("User " + userName + " in queue " + getQueueName() + 
          " will exceed limit, required: " + required + 
          " consumed: " + user.getConsumedResources() + " limit: " + limit +
          " queueCapacity: " + queueCapacity + 
          " qconsumed: " + consumed +
          " currentCapacity: " + currentCapacity +
          " activeUsers: " + activeUsers +
          " clusterCapacity: " + clusterResource.getMemory()
          );
      return false;
    }

    return true;
  }
  
  private static int divideAndCeil(int a, int b) {
    if (b == 0) {
      LOG.info("divideAndCeil called with a=" + a + " b=" + b);
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  boolean needContainers(Application application, Priority priority) {
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, NodeManager.ANY);

    return (offSwitchRequest.getNumContainers() > 0);
  }

  Resource assignContainersOnNode(Resource clusterResource, NodeManager node, 
      Application application, Priority priority) {

    Resource assigned = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;

    // Data-local
    assigned = assignNodeLocalContainers(clusterResource, node, application, priority); 
    if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThan(
          assigned, 
          org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE)) {
      return assigned;
    }

    // Rack-local
    assigned = assignRackLocalContainers(clusterResource, node, application, priority);
    if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThan(
        assigned, 
        org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE)) {
    return assigned;
  }
    
    // Off-switch
    return assignOffSwitchContainers(clusterResource, node, application, priority);
  }

  Resource assignNodeLocalContainers(Resource clusterResource, NodeManager node, 
      Application application, Priority priority) {
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getNodeAddress());
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.DATA_LOCAL)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.DATA_LOCAL);
      }
    }
    
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
  }

  Resource assignRackLocalContainers(Resource clusterResource, NodeManager node, 
      Application application, Priority priority) {
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRackName());
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.RACK_LOCAL)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.RACK_LOCAL);
      }
    }
    
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
  }

  Resource assignOffSwitchContainers(Resource clusterResource, NodeManager node, 
      Application application, Priority priority) {
    ResourceRequest request = 
      application.getResourceRequest(priority, NodeManager.ANY);
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.OFF_SWITCH)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.OFF_SWITCH);
      }
    }
    
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
  }

  boolean canAssign(Application application, Priority priority, 
      NodeInfo node, NodeType type) {

    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, NodeManager.ANY);
    
    if (offSwitchRequest.getNumContainers() == 0) {
      return false;
    }
    
    if (type == NodeType.OFF_SWITCH) {
      return offSwitchRequest.getNumContainers() > 0;
    }
    
    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = 
        application.getResourceRequest(priority, node.getRackName());
      if (rackLocalRequest == null) {
        // No point waiting for rack-locality if we don't need this rack
        return offSwitchRequest.getNumContainers() > 0;
      } else {
        return rackLocalRequest.getNumContainers() > 0;
      }
    }
    
    if (type == NodeType.DATA_LOCAL) {
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getNodeAddress());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }
    
    return false;
  }
  
  private Resource assignContainer(Resource clusterResource, NodeManager node, 
      Application application, 
      Priority priority, ResourceRequest request, NodeType type) {
    LOG.info("DEBUG --- assignContainers:" +
        " node=" + node.getNodeAddress() + 
        " application=" + application.getApplicationId().getId() + 
        " priority=" + priority.getPriority() + 
        " request=" + request + " type=" + type);
    Resource capability = request.getCapability();
    
    int availableContainers = 
        node.getAvailableResource().getMemory() / capability.getMemory(); // TODO: A buggy
                                                                // application
                                                                // with this
                                                                // zero would
                                                                // crash the
                                                                // scheduler.
    
    if (availableContainers > 0) {
      List<Container> containers =
        new ArrayList<Container>();
      Container container =
        org.apache.hadoop.yarn.server.resourcemanager.resource.Container
        .create(recordFactory, application.getApplicationId(), 
            application.getNewContainerId(),
            node.getNodeAddress(), capability);
      
      // If security is enabled, send the container-tokens too.
      if (UserGroupInformation.isSecurityEnabled()) {
        ContainerToken containerToken = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ContainerToken.class);
        ContainerTokenIdentifier tokenidentifier =
          new ContainerTokenIdentifier(container.getId(),
              container.getHostName(), container.getResource());
        containerToken.setIdentifier(ByteBuffer.wrap(tokenidentifier.getBytes()));
        containerToken.setKind(ContainerTokenIdentifier.KIND.toString());
        containerToken.setPassword(ByteBuffer.wrap(containerTokenSecretManager
              .createPassword(tokenidentifier)));
        containerToken.setService(container.getHostName()); // TODO: port
        container.setContainerToken(containerToken);
      }
      
      containers.add(container);

      // Allocate container to the application
      application.allocate(type, node, priority, request, containers);
      
      node.allocateContainer(application.getApplicationId(), containers);
      
      LOG.info("allocatedContainer" +
          " container=" + container + 
          " queue=" + this.toString() + 
          " util=" + getUtilization() + 
          " used=" + usedResources + 
          " cluster=" + clusterResource);

      return container.getResource();
    }
    
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
  }

  @Override
  public void completedContainer(Resource clusterResource, 
      Container container, Application application) {
    if (application != null) {
      // Careful! Locking order is important!
      synchronized (this) {
        // Inform the application
        application.completedContainer(container);
        
        // Book-keeping
        releaseResource(clusterResource, 
            application.getUser(), container.getResource());
        
        LOG.info("completedContainer" +
            " container=" + container +
        		" queue=" + this + 
            " util=" + getUtilization() + 
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }
      
      // Inform the parent queue
      parent.completedContainer(clusterResource, container, application);
    }
  }

  private synchronized void allocateResource(Resource clusterResource, 
      String userName, Resource resource) {
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.
      addResource(usedResources, resource);
    update(clusterResource);
    ++numContainers;
    
    User user = getUser(userName);
    user.assignContainer(resource);
  }

  private synchronized void releaseResource(Resource clusterResource, 
      String userName, Resource resource) {
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.
      subtractResource(usedResources, resource);
    update(clusterResource);
    --numContainers;
    
    User user = getUser(userName);
    user.releaseContainer(resource);
  }

  private synchronized void update(Resource clusterResource) {
    setUtilization(usedResources.getMemory() / (clusterResource.getMemory() * absoluteCapacity));
    setUsedCapacity(usedResources.getMemory() / (clusterResource.getMemory() * capacity));
  }
  
  static class User {
    Resource consumed = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(0);
    int applications = 0;
    
    public Resource getConsumedResources() {
      return consumed;
    }
  
    public int getApplications() {
      return applications;
    }
  
    public synchronized void submitApplication() {
      ++applications;
    }
    
    public synchronized void finishApplication() {
      --applications;
    }
    
    public synchronized void assignContainer(Resource resource) {
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
          consumed, resource);
    }
    
    public synchronized void releaseContainer(Resource resource) {
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
          consumed, resource);
    }
  }
}
