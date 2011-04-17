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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;

@Private
@Evolving
public class ParentQueue implements Queue {

  private static final Log LOG = LogFactory.getLog(ParentQueue.class);

  private final Queue parent;
  private final String queueName;
  private final float capacity;
  private final float maximumCapacity;
  private final float absoluteCapacity;
  private final float absoluteMaxCapacity;

  private float usedCapacity = 0.0f;
  private float utilization = 0.0f;

  private final Set<Queue> childQueues;
  
  private Resource usedResources = 
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(0);
  
  private final boolean rootQueue;
  
  private final Resource minimumAllocation;

  private volatile int numApplications;
  private volatile int numContainers;

  private QueueInfo queueInfo; 
  private Map<ApplicationId, org.apache.hadoop.yarn.api.records.Application> 
  applicationInfos;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public ParentQueue(CapacitySchedulerContext cs, 
      String queueName, Comparator<Queue> comparator, Queue parent) {
    minimumAllocation = cs.getMinimumAllocation();
    
    this.parent = parent;
    this.queueName = queueName;
    this.rootQueue = (parent == null);
    
    LOG.info("PQ: parent=" + parent + ", qName=" + queueName + 
        " qPath=" + getQueuePath() + ", root=" + rootQueue);
    this.capacity = 
      (float)cs.getConfiguration().getCapacity(getQueuePath()) / 100;

    float parentAbsoluteCapacity = 
      (parent == null) ? 1.0f : parent.getAbsoluteCapacity();
    this.absoluteCapacity = parentAbsoluteCapacity * capacity; 

    this.maximumCapacity = 
      cs.getConfiguration().getMaximumCapacity(getQueuePath());
    this.absoluteMaxCapacity = 
      (maximumCapacity == CapacitySchedulerConfiguration.UNDEFINED) ? 
          Float.MAX_VALUE :  (parentAbsoluteCapacity * maximumCapacity) / 100;
    
    this.childQueues = new TreeSet<Queue>(comparator);
    
    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setCapacity(capacity);
    this.queueInfo.setMaximumCapacity(maximumCapacity);
    this.queueInfo.setQueueName(queueName);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());
    
    this.applicationInfos = 
      new HashMap<ApplicationId, 
      org.apache.hadoop.yarn.api.records.Application>();

    LOG.info("Initialized parent-queue " + queueName + 
        " name=" + queueName + 
        ", fullname=" + getQueuePath() + 
        ", capacity=" + capacity + 
        ", asboluteCapacity=" + absoluteCapacity + 
        ", maxCapacity=" + maximumCapacity +
        ", asboluteMaxCapacity=" + absoluteMaxCapacity);
  }

  public void setChildQueues(Collection<Queue> childQueues) {
    
    // Validate
    float childCapacities = 0;
    for (Queue queue : childQueues) {
      childCapacities += queue.getCapacity();
    }
    if (childCapacities != 1.0f) {
      throw new IllegalArgumentException("Illegal" +
      		" capacity of " + childCapacities + 
      		" for children of queue " + queueName);
    }
    
    this.childQueues.addAll(childQueues);
    LOG.info("DEBUG --- setChildQueues: " + getChildQueuesToPrint());
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
    String parentPath = ((parent == null) ? "" : (parent.getQueuePath() + "."));
    return parentPath + getQueueName();
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
  public float getAbsoluteMaximumCapacity() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getMaximumCapacity() {
    // TODO Auto-generated method stub
    return 0;
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
  public List<Application> getApplications() {
    return null;
  }

  @Override
  public synchronized List<Queue> getChildQueues() {
    return new ArrayList<Queue>(childQueues);
  }

  public int getNumContainers() {
    return numContainers;
  }
  
  public int getNumApplications() {
    return numApplications;
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

    List<QueueInfo> childQueuesInfo = new ArrayList<QueueInfo>();
    if (includeChildQueues) {
      for (Queue child : childQueues) {
        // Get queue information recursively?
        childQueuesInfo.add(
            child.getQueueInfo(includeApplications, recursive, recursive));
      }
    }
    queueInfo.setChildQueues(childQueuesInfo);
    
    return queueInfo;
}

  public String toString() {
    return queueName + ":" + capacity + ":" + absoluteCapacity + ":" + 
      getUsedCapacity() + ":" + getUtilization() + ":" + 
      getNumApplications() + ":" + getNumContainers() + ":" + 
      childQueues.size() + " child-queues";
  }
  
  @Override
  public void submitApplication(Application application, String user,
      String queue, Priority priority) 
  throws AccessControlException {
    // Sanity check
    if (queue.equals(queueName)) {
      throw new AccessControlException("Cannot submit application " +
          "to non-leaf queue: " + queueName);
    }
    
    ++numApplications;
   
    applicationInfos.put(application.getApplicationId(), 
        application.getApplicationInfo());

    LOG.info("Application submission -" +
    		" appId: " + application.getApplicationId() + 
        " user: " + user + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());

    // Inform the parent queue
    if (parent != null) {
      parent.submitApplication(application, user, queue, priority);
    }
  }

  @Override
  public void finishApplication(Application application, String queue) 
  throws AccessControlException {
    // Sanity check
    if (queue.equals(queueName)) {
      throw new AccessControlException("Cannot finish application " +
          "from non-leaf queue: " + queueName);
    }
    
    --numApplications;
    applicationInfos.remove(application.getApplicationId());

    LOG.info("Application completion -" +
        " appId: " + application.getApplicationId() + 
        " user: " + application.getUser() + 
        " leaf-queue of parent: " + getQueueName() + 
        " #applications: " + getNumApplications());

    // Inform the parent queue
    if (parent != null) {
      parent.finishApplication(application, queue);
    }
  }

  synchronized void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }
  
  synchronized void setUtilization(float utilization) {
    this.utilization = utilization;
  }

  @Override
  public synchronized Resource assignContainers(
      Resource clusterResource, NodeManager node) {
    Resource assigned = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(0);

    while (canAssign(node)) {
      LOG.info("DEBUG --- Trying to assign containers to child-queue of " + 
          getQueueName());
      
      // Are we over maximum-capacity for this queue?
      if (!assignToQueue()) {
        LOG.info(getQueueName() + 
            " current-capacity (" + getUtilization() + ") > max-capacity (" + 
            absoluteMaxCapacity + ")");
        break;
      }
      
      // Schedule
      Resource assignedToChild = assignContainersToChildQueues(clusterResource, node);

      // Done if no child-queue assigned anything
      if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThan(
          assignedToChild, 
          org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE)) {
        // Track resource utilization for the parent-queue
        allocateResource(clusterResource, assignedToChild);
        
        // Track resource utilization in this pass of the scheduler
        org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
            assigned, assignedToChild);
        
        LOG.info("completedContainer" +
            " queue=" + getQueueName() + 
            " util=" + getUtilization() + 
            " used=" + usedResources + 
            " cluster=" + clusterResource);

      } else {
        break;
      }

      LOG.info("DEBUG ---" +
      		" parentQ=" + getQueueName() + 
      		" assigned=" + assigned + 
      		" utilization=" + getUtilization());
      
      // Do not assign more than one container if this isn't the root queue
      if (!rootQueue) {
        break;
      }
    } 
    
    return assigned;
  }
  
  private synchronized boolean assignToQueue() {
    return (getUtilization() < absoluteMaxCapacity);
  }
  
  private boolean canAssign(NodeInfo node) {
    return 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThanOrEqual(
        node.getAvailableResource(), 
        minimumAllocation);
  }
  
  synchronized Resource assignContainersToChildQueues(Resource cluster, 
      NodeManager node) {
    Resource assigned = 
      org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.createResource(0);
    
    printChildQueues();

    // Try to assign to most 'under-served' sub-queue
    for (Iterator<Queue> iter=childQueues.iterator(); iter.hasNext();) {
      Queue childQueue = iter.next();
      LOG.info("DEBUG --- Trying to assign to" +
      		" queue: " + childQueue.getQueuePath() + 
      		" stats: " + childQueue);
      assigned = childQueue.assignContainers(cluster, node);

      // If we do assign, remove the queue and re-insert in-order to re-sort
      if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.greaterThan(
            assigned, 
            org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE)) {
        // Remove and re-insert to sort
        iter.remove();
        LOG.info("Re-sorting queues since queue: " + childQueue.getQueuePath() + 
            " stats: " + childQueue);
        childQueues.add(childQueue);
        printChildQueues();
        break;
      }
    }
    
    return assigned;
  }

  String getChildQueuesToPrint() {
    StringBuilder sb = new StringBuilder();
    for (Queue q : childQueues) {
      sb.append(q.getQueuePath() + "(" + q.getUtilization() + "), ");
    }
    return sb.toString();
  }
  void printChildQueues() {
    LOG.info("DEBUG --- printChildQueues - queue: " + getQueuePath() + 
        " child-queues: " + getChildQueuesToPrint());
  }
  
  @Override
  public void completedContainer(Resource clusterResource,
      Container container, Application application) {
    if (application != null) {
      // Careful! Locking order is important!
      // Book keeping
      synchronized (this) {
        releaseResource(clusterResource, container.getResource());

        LOG.info("completedContainer" +
            " queue=" + getQueueName() + 
            " util=" + getUtilization() + 
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }

      // Inform the parent
      if (parent != null) {
        parent.completedContainer(clusterResource, container, application);
      }    
    }
  }
  
  private synchronized void allocateResource(Resource clusterResource, 
      Resource resource) {
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.
      addResource(usedResources, resource);
    update(clusterResource);
    ++numContainers;
  }
  
  private synchronized void releaseResource(Resource clusterResource, 
      Resource resource) {
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.
      subtractResource(usedResources, resource);
    update(clusterResource);
    --numContainers;
  }

  private synchronized void update(Resource clusterResource) {
    setUtilization(usedResources.getMemory() / (clusterResource.getMemory() * absoluteCapacity));
    setUsedCapacity(usedResources.getMemory() / (clusterResource.getMemory() * capacity));
  }
  
}
