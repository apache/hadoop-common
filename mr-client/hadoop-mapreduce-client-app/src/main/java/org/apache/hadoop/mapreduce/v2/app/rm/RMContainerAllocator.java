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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);

  //holds information about the assigned containers to task attempts
  private final AssignedRequests assignedRequests = new AssignedRequests();
  
  //holds pending requests to be fulfilled by RM
  private final PendingRequests pendingRequests = new PendingRequests();
  
  private int containersAllocated = 0;
  private int mapsAssigned = 0;
  private int reducesAssigned = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    List<Container> allocatedContainers = getResources();
    if (allocatedContainers.size() > 0) {
      LOG.info("Before Assign: " + getStat());
      pendingRequests.assign(allocatedContainers);
      LOG.info("After Assign: " + getStat());
    }
  }

  @Override
  public void stop() {
    super.stop();
    LOG.info("Final Stats: " + getStat());
  }

  @Override
  public synchronized void handle(ContainerAllocatorEvent event) {
    LOG.info("Processing the event " + event.toString());
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      pendingRequests.add((ContainerRequestEvent) event);
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
      TaskAttemptId aId = event.getAttemptID();
      
      boolean removed = pendingRequests.remove(aId);
      if (!removed) {
        Container container = assignedRequests.get(aId);
        if (container != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          release(container);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + 
            aId);
      }
    }
  }

  private String getStat() {
    return "PendingMaps:" + pendingRequests.maps.size() +
        " PendingReduces:" + pendingRequests.reduces.size() +
        " containersAllocated:" + containersAllocated +
        " mapsAssigned:" + mapsAssigned + 
        " reducesAssigned:" + reducesAssigned + 
        " containersReleased:" + containersReleased +
        " hostLocalAssigned:" + hostLocalAssigned + 
        " rackLocalAssigned:" + rackLocalAssigned;
  }
  
  private List<Container> getResources() throws Exception {
    List<Container> allContainers = makeRemoteRequest();
    List<Container> allocatedContainers = new ArrayList<Container>();
    for (Container cont : allContainers) {
      if (cont.getState() != ContainerState.COMPLETE) {
        allocatedContainers.add(cont);
        LOG.debug("Received Container :" + cont);
      } else {
        LOG.info("Received completed container " + cont);
        TaskAttemptId attemptID = assignedRequests.get(cont.getId());
        if (attemptID == null) {
          LOG.error("Container complete event for unknown container id " +
              cont.getId());
        } else {
          assignedRequests.remove(attemptID);
          //send the container completed event to Task attempt
          eventHandler.handle(new TaskAttemptEvent(attemptID,
              TaskAttemptEventType.TA_CONTAINER_COMPLETED));
        }
      }
      LOG.debug("Received Container :" + cont);
    }
    return allocatedContainers;
  }

  private class PendingRequests {
    
    private Resource mapResourceReqt;
    private Resource reduceResourceReqt;
    
    private final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();
    private final LinkedList<TaskAttemptId> earlierFailedReduces = 
      new LinkedList<TaskAttemptId>();
    
    private final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<TaskAttemptId, ContainerRequestEvent> maps = 
      new LinkedHashMap<TaskAttemptId, ContainerRequestEvent>();
    
    private final Map<TaskAttemptId, ContainerRequestEvent> reduces = 
      new LinkedHashMap<TaskAttemptId, ContainerRequestEvent>();
    
    boolean remove(TaskAttemptId tId) {
      ContainerRequestEvent req = maps.remove(tId);
      if (req == null) {
        req = reduces.remove(tId);
      }
      if (req == null) {
        return false;
      } else {
        decContainerReq(req);
        return true;
      }
    }
    
    void add(ContainerRequestEvent event) {
      
      if (event.getAttemptID().getTaskId().getTaskType() == TaskType.MAP) {
        if (mapResourceReqt == null) {
          mapResourceReqt = event.getCapability();
        }
        maps.put(event.getAttemptID(), event);
        
        if (event.getEarlierAttemptFailed()) {
          earlierFailedMaps.add(event.getAttemptID());
        } else {
          for (String host : event.getHosts()) {
            LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
            if (list == null) {
              list = new LinkedList<TaskAttemptId>();
              mapsHostMapping.put(host, list);
            }
            list.add(event.getAttemptID());
            LOG.info("Added attempt req to host " + host);
         }
         for (String rack: event.getRacks()) {
           LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
           if (list == null) {
             list = new LinkedList<TaskAttemptId>();
             mapsRackMapping.put(rack, list);
           }
           list.add(event.getAttemptID());
           LOG.info("Added attempt req to rack " + rack);
         }
        }
      } else {//reduce
        if (reduceResourceReqt == null) {
          reduceResourceReqt = event.getCapability();
        }
        
        if (event.getEarlierAttemptFailed()) {
          earlierFailedReduces.add(event.getAttemptID());
        }
        reduces.put(event.getAttemptID(), event);
      }
      
      addContainerReq(event);
    }
    
    private void assign(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        ContainerRequestEvent assigned = null;
        LOG.info("Assiging container " + allocated);
        
        //try to assign to earlierFailedMaps if present
        while (assigned == null && earlierFailedMaps.size() > 0 && 
            allocated.getResource().getMemory() >= mapResourceReqt.getMemory()) {
          TaskAttemptId tId = earlierFailedMaps.removeFirst();
          if (maps.containsKey(tId)) {
            assigned = maps.remove(tId);
            mapsAssigned++;
            LOG.info("Assigned from earlierFailedMaps");
            break;
          }
        }
        
        //try to assign to earlierFailedReduces if present
        while (assigned == null && earlierFailedReduces.size() > 0 && 
            allocated.getResource().getMemory() >= reduceResourceReqt.getMemory()) {
          TaskAttemptId tId = earlierFailedReduces.removeFirst();
          if (reduces.containsKey(tId)) {
            assigned = reduces.remove(tId);
            reducesAssigned++;
            LOG.info("Assigned from earlierFailedReduces");
            break;
          }
        }
        
        //try to assign to maps if present 
        //first by host, then by rack, followed by *
        while (assigned == null && maps.size() > 0
            && allocated.getResource().getMemory() >= mapResourceReqt.getMemory()) {
          String host = allocated.getContainerManagerAddress();
          String[] hostport = host.split(":");
          if (hostport.length == 2) {
            host = hostport[0];
          }
          LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
          while (list != null && list.size() > 0) {
            LOG.info("Host matched to the request list " + host);
            TaskAttemptId tId = list.removeFirst();
            if (maps.containsKey(tId)) {
              assigned = maps.remove(tId);
              mapsAssigned++;
              hostLocalAssigned++;
              LOG.info("Assigned based on host match " + host);
              break;
            }
          }
          if (assigned == null) {
            // TODO: get rack
            String rack = "";
            list = mapsRackMapping.get(rack);
            while (list != null && list.size() > 0) {
              TaskAttemptId tId = list.removeFirst();
              if (maps.containsKey(tId)) {
                assigned = maps.remove(tId);
                mapsAssigned++;
                rackLocalAssigned++;
                LOG.info("Assigned based on rack match " + rack);
                break;
              }
            }
            if (assigned == null && maps.size() > 0) {
              TaskAttemptId tId = maps.keySet().iterator().next();
              assigned = maps.remove(tId);
              mapsAssigned++;
              LOG.info("Assigned based on * match");
              break;
            }
          }
        }
        
        //try to assign to reduces if present
        if (assigned == null && reduces.size() > 0
            && allocated.getResource().getMemory() >= reduceResourceReqt.getMemory()) {
          TaskAttemptId tId = reduces.keySet().iterator().next();
          assigned = reduces.remove(tId);
          reducesAssigned++;
          LOG.info("Assigned to reduce");
        }
        
        if (assigned != null) {
          
          // Update resource requests
          decContainerReq(assigned);

          // send the container-assigned event to task attempt
          eventHandler.handle(new TaskAttemptContainerAssignedEvent(
              assigned.getAttemptID(), allocated.getId(),
              allocated.getContainerManagerAddress(),
              allocated.getNodeHttpAddress(),
              allocated.getContainerToken()));

          assignedRequests.add(allocated, assigned.getAttemptID());
          
          LOG.info("Assigned container (" + allocated + ") " +
              " to task " + assigned.getAttemptID() +
              " on node " + allocated.getContainerManagerAddress());
        } else {
          //not assigned to any request, release the container
          LOG.info("Releasing unassigned container " + allocated);
          containersReleased++;
          release(allocated);
        }
      }
    }
  }

  private static class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    private final Map<TaskAttemptId, Container> attemptToContainerMap = 
      new HashMap<TaskAttemptId, Container>();
    
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getContainerManagerAddress() 
          + " to " + tId);
      containerToAttemptMap.put(container.getId(), tId);
      attemptToContainerMap.put(tId, container);
    }

    boolean remove(TaskAttemptId tId) {
      Container container = attemptToContainerMap.remove(tId);
      if (container != null) {
        containerToAttemptMap.remove(container.getId());
        return true;
      }
      return false;
    }
    
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }

    Container get(TaskAttemptId tId) {
      return attemptToContainerMap.get(tId);
    }
  }
}
