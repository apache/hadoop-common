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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;

/**
 * Track the node info and heart beat responses for this node.
 * This should be package private. It does not need to be public.
 *
 */
class NodeInfoTracker {
  private final NodeInfo node;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  HeartbeatResponse lastHeartBeatResponse;
  private org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = recordFactory.newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);

  public NodeInfoTracker(NodeInfo node, HeartbeatResponse lastHeartBeatResponse) {
    this.node = node;
    this.lastHeartBeatResponse = lastHeartBeatResponse;
    this.nodeStatus.setNodeId(node.getNodeID());
    this.nodeStatus.setLastSeen(System.currentTimeMillis());
  }

  public synchronized NodeInfo getNodeInfo() {
    return this.node;
  }

  public synchronized HeartbeatResponse getLastHeartBeatResponse() {
    return this.lastHeartBeatResponse;
  }

  public synchronized void refreshHeartBeatResponse(HeartbeatResponse heartBeatResponse) {
    this.lastHeartBeatResponse = heartBeatResponse;
  }

  public synchronized void updateNodeStatus(org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus) {
    this.nodeStatus = nodeStatus;
  }

  public synchronized org.apache.hadoop.yarn.server.api.records.NodeStatus getNodeStatus() {
    return this.nodeStatus;
  }
}