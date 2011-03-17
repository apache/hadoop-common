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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;

/**
 * This interface defines tracking for all the node managers for the scheduler 
 * and all the containers running/allocated 
 * on the nodemanagers.
 */
@Evolving
@Private
public interface ClusterTracker {

  /**
   * The class that encapsulates response from clusterinfo for 
   * updates from the node managers.
   */
  public class NodeResponse {
    private final List<Container> completed;
    private final List<Container> toCleanUp;
    private final List<ApplicationID> finishedApplications;
    
    public NodeResponse(List<ApplicationID> finishedApplications,
        List<Container> completed, List<Container> toKill) {
      this.finishedApplications = finishedApplications;
      this.completed = completed;
      this.toCleanUp = toKill;
    }
    public List<ApplicationID> getFinishedApplications() {
      return this.finishedApplications;
    }
    public List<Container> getCompletedContainers() {
      return this.completed;
    }
    public List<Container> getContainersToCleanUp() {
      return this.toCleanUp;
    }
  }
  
  /** 
   * the current cluster resource.
   * @return the current cluster resource.
   */
  public Resource getClusterResource();
  
  /**
   * Remove the node from this cluster.
   * @param nodeInfo the nodemanager to be removed from tracking.
   */
  public void removeNode(NodeInfo nodeInfo);
  

  /**
   * Add a node for tracking
   * @param nodeId the nodeid of the node
   * @param hostName the hostname of the node
   * @param node the node info of the node
   * @param capability the total capability of the node.
   * @return the {@link NodeInfo} that tracks this node.
   */
  public NodeInfo addNode(NodeID nodeId, String hostName, Node node,
      Resource capability);
  
  /**
   * An application has released a container
   * @param applicationId the application that is releasing the container
   * @param container the container that is being released 
   */
  public boolean releaseContainer(ApplicationID applicationId, 
      Container container);
  
  /**
   * Update the cluster with the node update.
   * @param nodeInfo the node for which the update is
   * @param containers the containers update for the node.
   * @return the containers that are completed or need to be prempted.
   */
  public NodeResponse nodeUpdate(NodeInfo nodeInfo, 
      Map<CharSequence, List<Container>> containers);

  /**
   * check to see if this node is being tracked for resources and allocations.
   * @param node the node to check for.
   * @return true if this node is being tracked, false else.
   */
  public boolean isTracked(NodeInfo node);
  
  /**
   * Update the cluster with allocated containers on a node.
   * @param nodeInfo the nodeinfo for the node that containers are allocated on.
   * @param applicationId the application id of the application that containers
   * are allocated to
   * @param containers the list of containers allocated.
   */
  public void addAllocatedContainers(NodeInfo nodeInfo, ApplicationID applicationId,
      List<Container> containers);
  
  /**
   * Notify each of the node data structures that the application has finished.
   * @param applicationId the application id of the application that finished.
   * @param nodes the list of nodes that need to be notified of application completion.
   */
  public void finishedApplication(ApplicationID applicationId, List<NodeInfo> nodes);
}