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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task.State;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceRequest;

@Private
public class Application {
  private static final Log LOG = LogFactory.getLog(Application.class);
  
  private AtomicInteger taskCounter = new AtomicInteger(0);

  final private String user;
  final private String queue;
  final private ApplicationID applicationId;
  final private ResourceManager resourceManager;
  
  final private Map<Priority, Resource> requestSpec = 
    new TreeMap<Priority, Resource>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  
  final private Map<Priority, Map<String, ResourceRequest>> requests = 
    new TreeMap<Priority, Map<String, ResourceRequest>>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  
  final Map<Priority, Set<Task>> tasks = 
    new TreeMap<Priority, Set<Task>>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.Comparator());
  
  final private Set<ResourceRequest> ask = 
    new TreeSet<ResourceRequest>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceRequest.Comparator());
  final private Set<Container> release = 
    new TreeSet<Container>(
        new org.apache.hadoop.yarn.server.resourcemanager.resource.Container.Comparator());

  final private Map<String, NodeManager> nodes = 
    new HashMap<String, NodeManager>();
  
  Resource used = new Resource();
  
  public Application(String user, ResourceManager resourceManager)
      throws AvroRemoteException {
    this(user, "default", resourceManager);
  }
  
  public Application(String user, String queue, ResourceManager resourceManager)
  throws AvroRemoteException {
    this.user = user;
    this.queue = queue;
    this.resourceManager = resourceManager;
    this.applicationId =
      this.resourceManager.getApplicationsManager().getNewApplicationID();
  }

  public String getUser() {
    return user;
  }

  public String getQueue() {
    return queue;
  }

  public ApplicationID getApplicationId() {
    return applicationId;
  }

  public static String resolve(String hostName) {
    return NetworkTopology.DEFAULT_RACK;
  }
  
  public int getNextTaskId() {
    return taskCounter.incrementAndGet();
  }
  
  public Resource getUsedResources() {
    return used;
  }
  
  public synchronized void submit() throws IOException {
    ApplicationSubmissionContext context = new ApplicationSubmissionContext();
    context.applicationId = applicationId;
    context.user = this.user;
    context.queue = this.queue;
    resourceManager.getApplicationsManager().submitApplication(context);
  }
  
  public synchronized void addResourceRequestSpec(
      Priority priority, Resource capability) {
    Resource currentSpec = requestSpec.put(priority, capability);
    if (currentSpec != null) {
      throw new IllegalStateException("Resource spec already exists for " +
      		"priority " + priority.priority + " - " + currentSpec.memory);
    }
  }
  
  public synchronized void addNodeManager(String host, NodeManager nodeManager) {
    nodes.put(host, nodeManager);
  }
  
  private synchronized NodeManager getNodeManager(String host) {
    return nodes.get(host);
  }
  
  public synchronized void addTask(Task task) {
    Priority priority = task.getPriority();
    Map<String, ResourceRequest> requests = this.requests.get(priority);
    if (requests == null) {
      requests = new HashMap<String, ResourceRequest>();
      this.requests.put(priority, requests);
      LOG.info("DEBUG --- Added" +
      		" priority=" + priority + 
      		" application=" + applicationId);
    }
    
    final Resource capability = requestSpec.get(priority);
    
    // Note down the task
    Set<Task> tasks = this.tasks.get(priority);
    if (tasks == null) {
      tasks = new HashSet<Task>();
      this.tasks.put(priority, tasks);
    }
    tasks.add(task);
    
    LOG.info("Added task " + task.getTaskId() + " to application " + 
        applicationId + " at priority " + priority);
    
    LOG.info("DEBUG --- addTask:" +
    		" application=" + applicationId + 
    		" #asks=" + ask.size());
    
    // Create resource requests
    for (String host : task.getHosts()) {
      // Data-local
      addResourceRequest(priority, requests, host, capability);
    }
        
    // Rack-local
    for (String rack : task.getRacks()) {
      addResourceRequest(priority, requests, rack, capability);
    }
      
    // Off-switch
    addResourceRequest(priority, requests, 
        org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager.ANY, 
        capability);
  }
  
  public synchronized void finishTask(Task task) throws IOException {
    Set<Task> tasks = this.tasks.get(task.getPriority());
    if (!tasks.remove(task)) {
      throw new IllegalStateException(
          "Finishing unknown task " + task.getTaskId() + 
          " from application " + applicationId);
    }
    
    NodeManager nodeManager = task.getNodeManager();
    ContainerID containerId = task.getContainerId();
    task.stop();
    nodeManager.stopContainer(containerId);
    
    org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.subtractResource(
        used, requestSpec.get(task.getPriority()));
    
    LOG.info("Finished task " + task.getTaskId() + 
        " of application " + applicationId + 
        " on node " + nodeManager.getHostName() + 
        ", currently using " + used + " resources");
  }
  
  private synchronized void addResourceRequest(
      Priority priority, Map<String, ResourceRequest> requests, 
      String resourceName, Resource capability) {
    ResourceRequest request = requests.get(resourceName);
    if (request == null) {
      request = 
        org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceRequest.create(
            priority, resourceName, capability, 1);
      requests.put(resourceName, request);
    } else {
      ++request.numContainers;
    }
    
    // Note this down for next interaction with ResourceManager
    ask.remove(request);
    ask.add(
        org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceRequest.create(
            request)); // clone to ensure the RM doesn't manipulate the same obj
    
    LOG.info("DEBUG --- addResourceRequest:" +
    		" applicationId=" + applicationId.id +
    		" priority=" + priority.priority + 
        " resourceName=" + resourceName + 
        " capability=" + capability +
        " numContainers=" + request.numContainers + 
        " #asks=" + ask.size());
  }
  
  public synchronized List<Container> getResources() throws IOException {
    LOG.info("DEBUG --- getResources begin:" +
        " application=" + applicationId + 
        " #ask=" + ask.size() +
        " #release=" + release.size());
    for (ResourceRequest request : ask) {
      LOG.info("DEBUG --- getResources:" +
          " application=" + applicationId + 
          " ask-request=" + request);
    }
    for (Container c : release) {
      LOG.info("DEBUG --- getResources:" +
          " application=" + applicationId + 
          " release=" + c);
    }
    
    // Get resources from the ResourceManager
    List<Container> response = 
      resourceManager.getResourceScheduler().allocate(applicationId, 
          new ArrayList<ResourceRequest>(ask), 
          new ArrayList<Container>(release));
    
    List<Container> containers = new ArrayList<Container>(response.size());
    for (Container container : response) {
      if (container.state != ContainerState.COMPLETE) {
        containers.add(
            org.apache.hadoop.yarn.server.resourcemanager.resource.Container.create(
                container));
      }
    }
    // Clear state for next interaction with ResourceManager
    ask.clear();
    release.clear();
    
    LOG.info("DEBUG --- getResources() for " + applicationId + ":" +
    		" ask=" + ask.size() + 
    		" release= "+ release.size() + 
    		" recieved=" + containers.size());
    
    return containers;
  }
  
  public synchronized void assign(List<Container> containers) 
  throws IOException {
    
    int numContainers = containers.size();
    // Schedule in priority order
    for (Priority priority : requests.keySet()) {
      assign(priority, NodeType.DATA_LOCAL, containers);
      assign(priority, NodeType.RACK_LOCAL, containers);
      assign(priority, NodeType.OFF_SWITCH, containers);

      if (containers.isEmpty()) { 
        break;
      }
    }
    
    int assignedContainers = numContainers - containers.size();
    LOG.info("Application " + applicationId + " assigned " + 
        assignedContainers + "/" + numContainers);
    if (assignedContainers < numContainers) {
      // Release
      release.addAll(containers);
    }
  }
  
  public synchronized void schedule() throws IOException {
    assign(getResources());
  }
  
  private synchronized void assign(Priority priority, NodeType type, 
      List<Container> containers) throws IOException {
    for (Iterator<Container> i=containers.iterator(); i.hasNext();) {
      Container container = i.next();
      String host = container.hostName.toString();
      
      if (org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.equals(
          requestSpec.get(priority), container.resource)) { 
        // See which task can use this container
        for (Iterator<Task> t=tasks.get(priority).iterator(); t.hasNext();) {
          Task task = t.next();
          if (task.getState() == State.PENDING && task.canSchedule(type, host)) {
            NodeManager nodeManager = getNodeManager(host);
            
            task.start(nodeManager, container.id);
            i.remove();
            
            // Track application resource usage
            org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.addResource(
                used, container.resource);
            
            LOG.info("Assigned container (" + container + ") of type " + type +
                " to task " + task.getTaskId() + " at priority " + priority + 
                " on node " + nodeManager.getHostName() +
                ", currently using " + used + " resources");

            // Update resource requests
            updateResourceRequests(requests.get(priority), type, task);

            // Launch the container
            nodeManager.startContainer(createCLC(container));
            break;
          }
        }
      }
    }
  }

  private void updateResourceRequests(Map<String, ResourceRequest> requests, 
      NodeType type, Task task) {
    if (type == NodeType.DATA_LOCAL) {
      for (String host : task.getHosts()) {
        LOG.info("DEBUG --- updateResourceRequests:" +
            " application=" + applicationId +
        		" type=" + type + 
        		" host=" + host + 
        		" request=" + ((requests == null) ? "null" : requests.get(host)));
        updateResourceRequest(requests.get(host));
      }
    }
    
    if (type == NodeType.DATA_LOCAL || type == NodeType.RACK_LOCAL) {
      for (String rack : task.getRacks()) {
        LOG.info("DEBUG --- updateResourceRequests:" +
            " application=" + applicationId +
            " type=" + type + 
            " rack=" + rack + 
            " request=" + ((requests == null) ? "null" : requests.get(rack)));
        updateResourceRequest(requests.get(rack));
      }
    }
    
    updateResourceRequest(
        requests.get(
            org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager.ANY)
            );
    
    LOG.info("DEBUG --- updateResourceRequests:" +
        " application=" + applicationId +
    		" #asks=" + ask.size());
  }
  
  private void updateResourceRequest(ResourceRequest request) {
    --request.numContainers;

    // Note this for next interaction with ResourceManager
    ask.remove(request);
    ask.add(
        org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceRequest.create(
        request)); // clone to ensure the RM doesn't manipulate the same obj

    LOG.info("DEBUG --- updateResourceRequest:" +
        " application=" + applicationId +
    		" request=" + request);
  }

  private ContainerLaunchContext createCLC(Container container) {
    ContainerLaunchContext clc = new ContainerLaunchContext();
    clc.id = container.id;
    clc.user = this.user;
    clc.resource = container.resource;
    return clc;
  }
}
