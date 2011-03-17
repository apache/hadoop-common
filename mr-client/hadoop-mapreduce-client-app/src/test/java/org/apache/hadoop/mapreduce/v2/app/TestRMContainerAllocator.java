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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.AMRMProtocol;
import org.apache.hadoop.yarn.AMResponse;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceRequest;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.junit.Test;

public class TestRMContainerAllocator {
  private static final Log LOG = LogFactory.getLog(TestRMContainerAllocator.class);

  @Test
  public void testSimple() throws Exception {
    FifoScheduler scheduler = createScheduler();
    LocalRMContainerAllocator allocator = new LocalRMContainerAllocator(
        scheduler);

    //add resources to scheduler
    NodeInfo nodeManager1 = addNode(scheduler, "h1", 10240);
    NodeInfo nodeManager2 = addNode(scheduler, "h2", 10240);
    NodeInfo nodeManager3 = addNode(scheduler, "h3", 10240);

    //create the container request
    ContainerRequestEvent event1 = 
      createReq(1, 1024, 1, new String[]{"h1"});
    allocator.sendRequest(event1);

    //send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(2, 1024, 1, new String[]{"h2"});
    allocator.sendRequest(event2);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(3, 1024, 1, new String[]{"h3"});
    allocator.sendRequest(event3);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //update resources in scheduler
    scheduler.nodeUpdate(nodeManager1, null); // Node heartbeat
    scheduler.nodeUpdate(nodeManager2, null); // Node heartbeat
    scheduler.nodeUpdate(nodeManager3, null); // Node heartbeat


    assigned = allocator.schedule();
    checkAssignments(
        new ContainerRequestEvent[]{event1, event2, event3}, assigned, false);
  }

  //TODO: Currently Scheduler seems to have bug where it does not work
  //for Application asking for containers with different capabilities.
  //@Test
  public void testResource() throws Exception {
    FifoScheduler scheduler = createScheduler();
    LocalRMContainerAllocator allocator = new LocalRMContainerAllocator(
        scheduler);

    //add resources to scheduler
    NodeInfo nodeManager1 = addNode(scheduler, "h1", 10240);
    NodeInfo nodeManager2 = addNode(scheduler, "h2", 10240);
    NodeInfo nodeManager3 = addNode(scheduler, "h3", 10240);

    //create the container request
    ContainerRequestEvent event1 = 
      createReq(1, 1024, 1, new String[]{"h1"});
    allocator.sendRequest(event1);

    //send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(2, 2048, 1, new String[]{"h2"});
    allocator.sendRequest(event2);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //update resources in scheduler
    scheduler.nodeUpdate(nodeManager1, null); // Node heartbeat
    scheduler.nodeUpdate(nodeManager2, null); // Node heartbeat
    scheduler.nodeUpdate(nodeManager3, null); // Node heartbeat

    assigned = allocator.schedule();
    checkAssignments(
        new ContainerRequestEvent[]{event1, event2}, assigned, false);
  }

  @Test
  public void testPriority() throws Exception {
    FifoScheduler scheduler = createScheduler();
    LocalRMContainerAllocator allocator = new LocalRMContainerAllocator(
        scheduler);

    //add resources to scheduler
    NodeInfo nodeManager1 = addNode(scheduler, "h1", 1024);
    NodeInfo nodeManager2 = addNode(scheduler, "h2", 10240);
    NodeInfo nodeManager3 = addNode(scheduler, "h3", 10240);

    //create the container request
    ContainerRequestEvent event1 = 
      createReq(1, 2048, 1, new String[]{"h1", "h2"});
    allocator.sendRequest(event1);

    //send 1 more request with different priority
    ContainerRequestEvent event2 = createReq(2, 2048, 2, new String[]{"h1"});
    allocator.sendRequest(event2);

    //send 1 more request with different priority
    ContainerRequestEvent event3 = createReq(3, 2048, 3, new String[]{"h3"});
    allocator.sendRequest(event3);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //update resources in scheduler
    scheduler.nodeUpdate(nodeManager1, null); // Node heartbeat
    scheduler.nodeUpdate(nodeManager2, null); // Node heartbeat
    scheduler.nodeUpdate(nodeManager3, null); // Node heartbeat

    assigned = allocator.schedule();
    checkAssignments(
        new ContainerRequestEvent[]{event1, event2, event3}, assigned, false);

    //validate that no container is assigned to h1 as it doesn't have 2048
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertFalse("Assigned count not correct", 
          "h1".equals(assig.getContainerManagerAddress()));
    }
  }



  private NodeInfo addNode(FifoScheduler scheduler, 
      String nodeName, int memory) {
    NodeID nodeId = new NodeID();
    nodeId.id = 0;
    Resource resource = new Resource();
    resource.memory = memory;
    NodeInfo nodeManager = scheduler.addNode(nodeId, nodeName,
        RMResourceTrackerImpl.resolve(nodeName), resource); // Node registration
    return nodeManager;
  }

  private FifoScheduler createScheduler() throws AvroRemoteException {
    FifoScheduler fsc = new FifoScheduler(new Configuration(),
        new ContainerTokenSecretManager()) {
      //override this to copy the objects
      //otherwise FifoScheduler updates the numContainers in same objects as kept by
      //RMContainerAllocator
      @Override
      public synchronized List<Container> allocate(ApplicationID applicationId,
          List<ResourceRequest> ask, List<Container> release) 
          throws IOException {
        List<ResourceRequest> askCopy = new ArrayList<ResourceRequest>();
        for (ResourceRequest req : ask) {
          ResourceRequest reqCopy = new ResourceRequest();
          reqCopy.priority = req.priority;
          reqCopy.hostName = req.hostName;
          reqCopy.capability = req.capability;
          reqCopy.numContainers = req.numContainers;
          askCopy.add(reqCopy);
        }
        //no need to copy release
        return super.allocate(applicationId, askCopy, release);
      }
    };
    try {
      fsc.addApplication(new ApplicationID(), "test", null, null);
    } catch(IOException ie) {
      LOG.info("add application failed with ", ie);
      assert(false);
    }
    return fsc;
  }

  private ContainerRequestEvent createReq(
      int attemptid, int memory, int priority, String[] hosts) {
    TaskAttemptID attemptId = new TaskAttemptID();
    attemptId.id = attemptid;
    Resource containerNeed = new Resource();
    containerNeed.memory = memory;
    return new ContainerRequestEvent(attemptId, 
        containerNeed, priority,
        hosts, new String[] {NetworkTopology.DEFAULT_RACK});
  }

  private void checkAssignments(ContainerRequestEvent[] requests, 
      List<TaskAttemptContainerAssignedEvent> assignments, 
      boolean checkHostMatch) {
    Assert.assertNotNull("Container not assigned", assignments);
    Assert.assertEquals("Assigned count not correct", 
        requests.length, assignments.size());

    //check for uniqueness of containerIDs
    Set<ContainerID> containerIds = new HashSet<ContainerID>();
    for (TaskAttemptContainerAssignedEvent assigned : assignments) {
      containerIds.add(assigned.getContainerID());
    }
    Assert.assertEquals("Assigned containers must be different", 
        assignments.size(), containerIds.size());

    //check for all assignment
    for (ContainerRequestEvent req : requests) {
      TaskAttemptContainerAssignedEvent assigned = null;
      for (TaskAttemptContainerAssignedEvent ass : assignments) {
        if (ass.getTaskAttemptID().equals(req.getAttemptID())){
          assigned = ass;
          break;
        }
      }
      checkAssignment(req, assigned, checkHostMatch);
    }
  }

  private void checkAssignment(ContainerRequestEvent request, 
      TaskAttemptContainerAssignedEvent assigned, boolean checkHostMatch) {
    Assert.assertNotNull("Nothing assigned to attempt " + request.getAttemptID(),
        assigned);
    Assert.assertEquals("assigned to wrong attempt", request.getAttemptID(),
        assigned.getTaskAttemptID());
    if (checkHostMatch) {
      Assert.assertTrue("Not assigned to requested host", Arrays.asList
          (request.getHosts()).contains(assigned.getContainerManagerAddress()));
    }

  }

  //Mock RMContainerAllocator
  //Instead of talking to remote Scheduler,uses the local Scheduler
  public static class LocalRMContainerAllocator extends RMContainerAllocator {
    private static final List<TaskAttemptContainerAssignedEvent> events = 
      new ArrayList<TaskAttemptContainerAssignedEvent>();

    public static class AMRMProtocolImpl implements AMRMProtocol {

      private ResourceScheduler resourceScheduler;

      public AMRMProtocolImpl(ResourceScheduler resourceScheduler) {
        this.resourceScheduler = resourceScheduler;
      }

      @Override
      public Void registerApplicationMaster(
          ApplicationMaster applicationMaster) throws AvroRemoteException {
        return null;
      }

      @Override
      public AMResponse allocate(ApplicationStatus status,
          List<ResourceRequest> ask, List<Container> release)
          throws AvroRemoteException {
        try {
          AMResponse response = new AMResponse();
          response.containers = resourceScheduler.allocate(status.applicationId, ask, release);
          return response;
        } catch(IOException ie) {
          throw RPCUtil.getRemoteException(ie);
        }
      }

      @Override
      public Void finishApplicationMaster(ApplicationMaster applicationMaster)
      throws AvroRemoteException {
        // TODO Auto-generated method stub
        return null;
      }

    }

    private ResourceScheduler scheduler;
    LocalRMContainerAllocator(ResourceScheduler scheduler) {
      super(null, new TestContext(events));
      this.scheduler = scheduler;
      super.init(new Configuration());
      super.start();
    }

    protected AMRMProtocol createSchedulerProxy() {
      return new AMRMProtocolImpl(scheduler);
    }

    @Override
    protected void register() {}
    @Override
    protected void unregister() {}

    public void sendRequest(ContainerRequestEvent req) {
      sendRequests(Arrays.asList(new ContainerRequestEvent[]{req}));
    }

    public void sendRequests(List<ContainerRequestEvent> reqs) {
      for (ContainerRequestEvent req : reqs) {
        handle(req);
      }
    }

    //API to be used by tests
    public List<TaskAttemptContainerAssignedEvent> schedule() {
      //run the scheduler
      try {
        allocate();
      } catch (Exception e) {

      }

      List<TaskAttemptContainerAssignedEvent> result = new ArrayList(events);
      events.clear();
      return result;
    }

    protected void startAllocatorThread() {
      //override to NOT start thread
    }

    static class TestContext implements AppContext {
      private List<TaskAttemptContainerAssignedEvent> events;
      TestContext(List<TaskAttemptContainerAssignedEvent> events) {
        this.events = events;
      }
      @Override
      public Map<JobID, Job> getAllJobs() {
        return null;
      }
      @Override
      public ApplicationID getApplicationID() {
        return new ApplicationID();
      }
      @Override
      public EventHandler getEventHandler() {
        return new EventHandler() {
          @Override
          public void handle(Event event) {
            events.add((TaskAttemptContainerAssignedEvent) event);
          }
        };
      }
      @Override
      public Job getJob(JobID jobID) {
        return null;
      }

      @Override
      public CharSequence getUser() {
        return null;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    TestRMContainerAllocator t = new TestRMContainerAllocator();
    t.testSimple();
    //t.testResource();
    t.testPriority();
  }
}
