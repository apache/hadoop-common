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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.service.AbstractService;



/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends AbstractService 
implements ContainerAllocator {
  private static final Log LOG = 
    LogFactory.getLog(RMContainerAllocator.class);
  private static final String ANY = "*";
  private static int rmPollInterval;//millis
  private ApplicationId applicationId;
  private EventHandler eventHandler;
  private volatile boolean stopped;
  protected Thread allocatorThread;
  private ApplicationMaster applicationMaster;
  private AMRMProtocol scheduler;
  private final ClientService clientService;
  private int lastResponseID = 0;
  
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  //mapping for assigned containers
  private final Map<ContainerId, TaskAttemptId> assignedMap = 
    new HashMap<ContainerId, TaskAttemptId>();

  private final Map<Priority, 
  Map<Resource,LinkedList<ContainerRequestEvent>>> localRequestsQueue = 
    new HashMap<Priority, Map<Resource,LinkedList<ContainerRequestEvent>>>();

  //Key -> Priority
  //Value -> Map
  //Key->ResourceName (eg. hostname, rackname, *)
  //Value->Map
  //Key->Resource Capability
  //Value->ResourceReqeust
  private final Map<Priority, Map<String, Map<Resource, ResourceRequest>>> 
  remoteRequestsTable = 
    new TreeMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();


  private final Set<ResourceRequest> ask =new TreeSet<ResourceRequest>();
  private final Set<Container> release = new TreeSet<Container>();

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super("RMContainerAllocator");
    this.clientService = clientService;
    this.applicationId = context.getApplicationID();
    this.eventHandler = context.getEventHandler();
    this.applicationMaster = recordFactory.newRecordInstance(ApplicationMaster.class);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    rmPollInterval = conf.getInt(YarnConfiguration.AM_EXPIRY_INTERVAL, 10000)/3;
  }

  @Override
  public void start() {
    scheduler= createSchedulerProxy();
    //LOG.info("Scheduler is " + scheduler);
    register();
    startAllocatorThread();
    super.start();
  }

  protected void register() {
    //Register
    applicationMaster.setApplicationId(applicationId);
    applicationMaster.setHost(clientService.getBindAddress().getAddress().getHostAddress());
    applicationMaster.setRpcPort(clientService.getBindAddress().getPort());
    applicationMaster.setState(ApplicationState.RUNNING);
    applicationMaster.setHttpPort(clientService.getHttpPort());
    applicationMaster.setStatus(recordFactory.newRecordInstance(ApplicationStatus.class));
    applicationMaster.getStatus().setApplicationId(applicationId);
    applicationMaster.getStatus().setProgress(0.0f);
    try {
      RegisterApplicationMasterRequest request = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      request.setApplicationMaster(applicationMaster);
      scheduler.registerApplicationMaster(request);
    } catch(Exception are) {
      LOG.info("Exception while registering", are);
      throw new YarnException(are);
    }
  }

  protected void unregister() {
    try {
      applicationMaster.setState(ApplicationState.COMPLETED);
      FinishApplicationMasterRequest request = recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
      request.setApplicationMaster(applicationMaster);
      scheduler.finishApplicationMaster(request);
    } catch(Exception are) {
      LOG.info("Error while unregistering ", are);
    }
  }

  @Override
  public void stop() {
    stopped = true;
    allocatorThread.interrupt();
    try {
      allocatorThread.join();
    } catch (InterruptedException ie) {
      LOG.info("Interruped Exception while stopping", ie);
    }
    unregister();
    super.stop();
  }

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            try {
              allocate();
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM.", e);
            }
          } catch (InterruptedException e) {
            LOG.info("Allocated thread interrupted. Returning");
            return;
          }
        }
      }
    });
    allocatorThread.start();
  }

  protected AMRMProtocol createSchedulerProxy() {
    final YarnRPC rpc = YarnRPC.create(getConfig());
    final Configuration conf = new Configuration(getConfig());
    final String serviceAddr = conf.get(
        YarnConfiguration.SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_SCHEDULER_BIND_ADDRESS);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
          SchedulerSecurityInfo.class, SecurityInfo.class);

      String tokenURLEncodedStr =
        System.getenv().get(
            YarnConfiguration.APPLICATION_MASTER_TOKEN_ENV_NAME);
      LOG.debug("AppMasterToken is " + tokenURLEncodedStr);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
      }
    });       
  }

  // TODO: Need finer synchronization.
  protected synchronized void allocate() throws Exception {
    assign(getResources());
  }

  @Override
  public synchronized void handle(ContainerAllocatorEvent event) {
    LOG.info("Processing the event " + event.toString());
    //TODO: can be replaced by switch instead of if-else
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      requestContainer((ContainerRequestEvent) event);
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
      //TODO: handle deallocation
    }
  }

  protected synchronized void requestContainer(ContainerRequestEvent event) {
    //add to the localRequestsQueue
    //localRequests Queue is hashed by Resource and Priority for easy lookups
    Map<Resource, LinkedList<ContainerRequestEvent>> eventMap =
      this.localRequestsQueue.get(event.getPriority());
    if (eventMap == null) {
      eventMap = new HashMap<Resource, LinkedList<ContainerRequestEvent>>();
      this.localRequestsQueue.put(event.getPriority(), eventMap);
    }

    LinkedList<ContainerRequestEvent> eventList =
      eventMap.get(event.getCapability());
    if (eventList == null) {
      eventList = new LinkedList<ContainerRequestEvent>();
      eventMap.put(event.getCapability(), eventList);
    }
    eventList.add(event);

    // Create resource requests
    for (String host : event.getHosts()) {
      // Data-local
      addResourceRequest(event.getPriority(), host, event.getCapability());
    }

    // Nothing Rack-local for now
    for (String rack : event.getRacks()) {
      addResourceRequest(event.getPriority(), rack, event.getCapability());
    }

    // Off-switch
    addResourceRequest(event.getPriority(), ANY, event.getCapability());

  }

  private void addResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests = 
      this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, Map<Resource, ResourceRequest>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      LOG.info("Added priority=" + priority);
    }
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new HashMap<Resource, ResourceRequest>();
      remoteRequests.put(resourceName, reqMap);
    }
    ResourceRequest remoteRequest = reqMap.get(capability);
    if (remoteRequest == null) {
      remoteRequest = recordFactory.newRecordInstance(ResourceRequest.class);
      remoteRequest.setPriority(priority);
      remoteRequest.setHostName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      reqMap.put(capability, remoteRequest);
    }
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);

    // Note this down for next interaction with ResourceManager
    ask.add(remoteRequest);
    LOG.info("addResourceRequest:" + " applicationId=" + applicationId.getId()
        + " priority=" + priority.getPriority() + " resourceName=" + resourceName
        + " numContainers=" + remoteRequest.getNumContainers() + " #asks="
        + ask.size());
  }

  private void decResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests = 
      this.remoteRequestsTable.get(priority);
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    ResourceRequest remoteRequest = reqMap.get(capability);

    LOG.info("BEFORE decResourceRequest:" + " applicationId=" + applicationId.getId()
        + " priority=" + priority.getPriority() + " resourceName=" + resourceName
        + " numContainers=" + remoteRequest.getNumContainers() + " #asks="
        + ask.size());

    remoteRequest.setNumContainers(remoteRequest.getNumContainers() -1);
    if (remoteRequest.getNumContainers() == 0) {
      reqMap.remove(capability);
      if (reqMap.size() == 0) {
        remoteRequests.remove(resourceName);
      }
      if (remoteRequests.size() == 0) {
        remoteRequestsTable.remove(priority);
      }
      //remove from ask if it may have
      ask.remove(remoteRequest); 
    } else {
      ask.add(remoteRequest);//this will override the request if ask doesn't
      //already have it.
    }

    LOG.info("AFTER decResourceRequest:" + " applicationId=" + applicationId.getId()
        + " priority=" + priority.getPriority() + " resourceName=" + resourceName
        + " numContainers=" + remoteRequest.getNumContainers() + " #asks="
        + ask.size());
  }

  private List<Container> getResources() throws Exception {
    ApplicationStatus status = recordFactory.newRecordInstance(ApplicationStatus.class);
    status.setApplicationId(applicationId);
    status.setResponseId(lastResponseID);
    
    AllocateRequest allocateRequest = recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationStatus(status);
    allocateRequest.addAllAsks(new ArrayList<ResourceRequest>(ask));
    allocateRequest.addAllReleases(new ArrayList<Container>(release));
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    AMResponse response = allocateResponse.getAMResponse(); 
    lastResponseID = response.getResponseId();
    List<Container> allContainers = response.getContainerList();
    ask.clear();
    release.clear();

    LOG.info("getResources() for " + applicationId + ":" +
        " ask=" + ask.size() + 
        " release= "+ release.size() + 
        " recieved=" + allContainers.size());
    List<Container> allocatedContainers = new ArrayList<Container>();
    for (Container cont : allContainers) {
      if (cont.getState() != ContainerState.COMPLETE) {
        allocatedContainers.add(cont);
        LOG.debug("Received Container :" + cont);
      } else {
        LOG.info("Received completed container " + cont);
        TaskAttemptId attemptID = assignedMap.remove(cont.getId());
        if (attemptID == null) {
          LOG.error("Container complete event for unknown container id " + 
              cont.getId());
        } else {
          //send the container completed event to Task attempt
          eventHandler.handle(new TaskAttemptEvent(attemptID, 
              TaskAttemptEventType.TA_CONTAINER_COMPLETED));
        }
      }
      LOG.debug("Received Container :" + cont);
    }
    return allocatedContainers;
  }

  private void assign(List<Container> allocatedContainers) {
    // Schedule in priority order
    for (Priority priority : localRequestsQueue.keySet()) {
      LOG.info("Assigning for priority " + priority); 
      assign(priority, allocatedContainers);
      if (allocatedContainers.isEmpty()) { 
        break;
      }
    }

    if (!allocatedContainers.isEmpty()) {
      //TODO
      //after the assigment, still containers are left
      //This can happen if container requests are cancelled by AM, currently
      //not there. release the unassigned containers??

      //LOG.info("Releasing container " + allocatedContainer);
      //release.add(allocatedContainer);
    }
  }

  private void assign(Priority priority, List<Container> allocatedContainers) {
    for (Iterator<Container> i=allocatedContainers.iterator(); i.hasNext();) {
      Container allocatedContainer = i.next();
      String host = allocatedContainer.getHostName();
      Resource capability = allocatedContainer.getResource();

      LinkedList<ContainerRequestEvent> requestList = 
        localRequestsQueue.get(priority).get(capability);

      if (requestList == null) {
        LOG.info("No request match at priority " + priority);
        return;
      }

      ContainerRequestEvent assigned = null;
      //walk thru the requestList to see if in any host matches
      Iterator<ContainerRequestEvent> it = requestList.iterator();
      while (it.hasNext()) {
        ContainerRequestEvent event = it.next();
        if (Arrays.asList(event.getHosts()).contains(host)) { // TODO: Fix
          assigned = event;
          it.remove();
          // Update resource requests
          for (String hostName : event.getHosts()) {
            decResourceRequest(priority, hostName, capability);
          }
          break;
        }
      }
      if (assigned == null) {//host didn't match
        if (requestList.size() > 0) {
          //choose the first one in queue
          assigned = requestList.remove();
        }
      }

      if (assigned != null) {

        i.remove(); // Remove from allocated Containers list also.

        // Update resource requests
        decResourceRequest(priority, ANY, capability);

        //send the container assigned event to Task attempt
        eventHandler.handle(new TaskAttemptContainerAssignedEvent(assigned
            .getAttemptID(), allocatedContainer.getId(),
            allocatedContainer.getHostName(),
            allocatedContainer.getContainerToken()));

        assignedMap.put(allocatedContainer.getId(), assigned.getAttemptID());

        LOG.info("Assigned container (" + allocatedContainer + ") " +
            " to task " + assigned.getAttemptID() + " at priority " + priority + 
            " on node " + allocatedContainer.getHostName());
      }
    }
  }

}
