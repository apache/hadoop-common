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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterTracker.NodeResponse;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.Resource;

/**
 * This interface is implemented by services which want to get notified
 * by the resource tracker with resource tracking information.
 */
@Evolving
@Private
public interface ResourceListener {
 
  /**
   * add a node to the resource listener.
   * @param nodeId the nodeid of the node
   * @param hostName the hostname of this node.
   * @param node the topology information.
   * @param capability the resource  capability of the node.
   * @return the {@link NodeInfo} object that tracks this nodemanager.
   */
  public NodeInfo addNode(NodeID nodeId,String hostName,
      Node node, Resource capability);
  
  /**
   * A node has been removed from the cluster.
   * @param node the node to remove.
   */
  public void removeNode(NodeInfo node);
  
  /**
   * A status update from a NodeManager
   * @param nodeInfo NodeManager info
   * @param containers the containers completed/running/failed on this node.
   * @return response information for the node, which containers to kill and 
   * applications to clean.
   */
  public NodeResponse nodeUpdate(NodeInfo nodeInfo, 
      Map<CharSequence,List<Container>> containers);
}