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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * This class is responsible for the interaction with the NodeManagers.
 * All the interactions with the NodeManagers happens via this interface.
 *`
 */
public class RMResourceTrackerImpl extends AbstractService implements 
ResourceTracker, ResourceContext {
  private static final Log LOG = LogFactory.getLog(RMResourceTrackerImpl.class);
  /* we dont garbage collect on nodes. A node can come back up again and re register,
   * so no use garbage collecting. Though admin can break the RM by bouncing 
   * nodemanagers on different ports again and again.
   */
  private Map<String, NodeId> nodes = new ConcurrentHashMap<String, NodeId>();
  private final Map<NodeId, NodeInfoTracker> nodeManagers = 
    new ConcurrentHashMap<NodeId, NodeInfoTracker>();
  private final HeartBeatThread heartbeatThread;
  
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  private final TreeSet<org.apache.hadoop.yarn.server.api.records.NodeStatus> nmExpiryQueue =
      new TreeSet<org.apache.hadoop.yarn.server.api.records.NodeStatus>(
          new Comparator<org.apache.hadoop.yarn.server.api.records.NodeStatus>() {
            public int compare(org.apache.hadoop.yarn.server.api.records.NodeStatus p1, org.apache.hadoop.yarn.server.api.records.NodeStatus p2) {
              if (p1.getLastSeen() < p2.getLastSeen()) {
                return -1;
              } else if (p1.getLastSeen() > p2.getLastSeen()) {
                return 1;
              } else {
                return (p1.getNodeId().getId() -
                    p2.getNodeId().getId());
              }
            }
          }
      );
  
  private ResourceListener resourceListener;
  private InetSocketAddress resourceTrackerAddress;
  private Server server;
  private final ContainerTokenSecretManager containerTokenSecretManager;
  private final AtomicInteger nodeCounter = new AtomicInteger(0);
  private static final HeartbeatResponse reboot = recordFactory.newRecordInstance(HeartbeatResponse.class);
  private long nmExpiryInterval;

  public RMResourceTrackerImpl(ContainerTokenSecretManager containerTokenSecretManager,
      ResourceListener listener) {
    super(RMResourceTrackerImpl.class.getName());
    reboot.setReboot(true);
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.heartbeatThread = new HeartBeatThread();
    this.resourceListener = listener;
  }

  @Override
  public void init(Configuration conf) {
    String resourceTrackerBindAddress =
      conf.get(YarnServerConfig.RESOURCETRACKER_ADDRESS,
          YarnServerConfig.DEFAULT_RESOURCETRACKER_BIND_ADDRESS);
    resourceTrackerAddress = NetUtils.createSocketAddr(resourceTrackerBindAddress);
    this.nmExpiryInterval =  conf.getLong(YarnConfiguration.NM_EXPIRY_INTERVAL, 
        YarnConfiguration.DEFAULT_NM_EXPIRY_INTERVAL);
    super.init(conf);
  }

  @Override
  public void start() {
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration rtServerConf = new Configuration(getConfig());
    rtServerConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        RMNMSecurityInfoClass.class, SecurityInfo.class);
    this.server =
      rpc.getServer(ResourceTracker.class, this, resourceTrackerAddress,
          rtServerConf, null);
    this.server.start();
    this.heartbeatThread.start();
    LOG.info("Expiry interval of NodeManagers set to " + nmExpiryInterval);
    super.start();
  }

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  public static Node resolve(String hostName) {
    return new NodeBase(hostName, NetworkTopology.DEFAULT_RACK);
  }

  @Override
  public RegisterNodeManagerResponse registerNodeManager(RegisterNodeManagerRequest request) throws YarnRemoteException {
    String node = request.getNode();
    Resource capability = request.getResource();
  
    NodeId nodeId = getNodeId(node);
    NodeInfoTracker nTracker = null;
    
    synchronized(nodeManagers) {
      if (!nodeManagers.containsKey(nodeId)) {
        /* we do the resolving here, so that scheduler does not have to do it */
        NodeInfo nodeManager = resourceListener.addNode(nodeId, node.toString(),
            resolve(node.toString()),
            capability);
        HeartbeatResponse response = recordFactory.newRecordInstance(HeartbeatResponse.class);
        response.setResponseId(0);
        nTracker = new NodeInfoTracker(nodeManager, response);
        nodeManagers.put(nodeId, nTracker);
      } else {
        nTracker = nodeManagers.get(nodeId);
        org.apache.hadoop.yarn.server.api.records.NodeStatus status = nTracker.getNodeStatus();
        status.setLastSeen(System.currentTimeMillis());
        nTracker.updateNodeStatus(status);
      }
    }
    addForTracking(nTracker.getNodeStatus());
    LOG.info("NodeManager from node " + node + " registered with capability: " + 
        capability.getMemory() + ", assigned nodeId " + nodeId.getId());

    RegistrationResponse regResponse = recordFactory.newRecordInstance(RegistrationResponse.class);
    regResponse.setNodeId(nodeId);
    SecretKey secretKey =
      this.containerTokenSecretManager.createAndGetSecretKey(node);
    regResponse.setSecretKey(ByteBuffer.wrap(secretKey.getEncoded()));
    RegisterNodeManagerResponse response = recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
    response.setRegistrationResponse(regResponse);
    return response;
  }

  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request) throws YarnRemoteException {
    org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = request.getNodeStatus();
    nodeStatus.setLastSeen(System.currentTimeMillis());
    NodeInfoTracker nTracker = null;
    NodeHeartbeatResponse nodeHbResponse = recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    synchronized(nodeManagers) {
      nTracker = nodeManagers.get(nodeStatus.getNodeId());
    }
    if (nTracker == null) {
      /* node does not exist */
      LOG.info("Node not found rebooting " + nodeStatus.getNodeId());
      nodeHbResponse.setHeartbeatResponse(reboot);
      return nodeHbResponse;
    }

    NodeInfo nodeInfo = nTracker.getNodeInfo();
    /* check to see if its an old heartbeat */    
    if (nodeStatus.getResponseId() + 1 == nTracker.getLastHeartBeatResponse().getResponseId()) {
      nodeHbResponse.setHeartbeatResponse(nTracker.getLastHeartBeatResponse());
      return nodeHbResponse;
    } else if (nodeStatus.getResponseId() + 1 < nTracker.getLastHeartBeatResponse().getResponseId()) {
      LOG.info("Too far behind rm response id:" + 
          nTracker.lastHeartBeatResponse.getResponseId() + " nm response id:" + nodeStatus.getResponseId());
      nodeHbResponse.setHeartbeatResponse(reboot);
      return nodeHbResponse;
    }

    /* inform any listeners of node heartbeats */
    NodeResponse nodeResponse = resourceListener.nodeUpdate(
        nodeInfo, nodeStatus.getAllContainers());

    
    HeartbeatResponse response = recordFactory.newRecordInstance(HeartbeatResponse.class);
    response.addAllContainersToCleanup(nodeResponse.getContainersToCleanUp());

    response.addAllApplicationsToCleanup(nodeResponse.getFinishedApplications());
    response.setResponseId(nTracker.getLastHeartBeatResponse().getResponseId() + 1);

    nTracker.refreshHeartBeatResponse(response);
    nTracker.updateNodeStatus(nodeStatus);
    nodeHbResponse.setHeartbeatResponse(response);
    return nodeHbResponse;
  }

  @Private
  public synchronized NodeInfo getNodeManager(NodeId nodeId) {
    NodeInfoTracker ntracker = nodeManagers.get(nodeId);
    return (ntracker == null ? null: ntracker.getNodeInfo());
  }

  private synchronized NodeId getNodeId(String node) {
    NodeId nodeId;
    nodeId = nodes.get(node);
    if (nodeId == null) {
      nodeId = recordFactory.newRecordInstance(NodeId.class);
      nodeId.setId(nodeCounter.getAndIncrement());
      nodes.put(node.toString(), nodeId);
    }
    return nodeId;
  }

  @Override
  public synchronized YarnClusterMetrics getClusterMetrics() {
    YarnClusterMetrics ymetrics = recordFactory.newRecordInstance(YarnClusterMetrics.class);
    ymetrics.setNumNodeManagers(nodeManagers.size());
    return ymetrics;
  }

  @Override
  public void stop() {
    if (this.server != null) {
      this.server.close();
    }
    super.stop();
  }

  @Override
  public List<NodeInfo> getAllNodeInfo() {
    List<NodeInfo> infoList = new ArrayList<NodeInfo>();
    synchronized (nodeManagers) {
      for (NodeInfoTracker t : nodeManagers.values()) {
        infoList.add(t.getNodeInfo());
      }
    }
    return infoList;
  }

  protected void addForTracking(org.apache.hadoop.yarn.server.api.records.NodeStatus status) {
    synchronized(nmExpiryQueue) {
      nmExpiryQueue.add(status);
    }
  }
  
  protected void expireNMs(List<NodeId> nodes) {
    for (NodeId id: nodes) {
      synchronized (nodeManagers) {
        NodeInfo nInfo = nodeManagers.get(id).getNodeInfo();
        nodeManagers.remove(id);
        resourceListener.removeNode(nInfo);
      }
    }
  }

  /*
   * This class runs continuosly to track the nodemanagers
   * that might be dead.
   */
  private class HeartBeatThread extends Thread {
    private volatile boolean stop = false;

    public HeartBeatThread() {
      super("RMResourceTrackerImpl:" + HeartBeatThread.class.getName());
    }

    @Override
    public void run() {
      /* the expiry queue does not need to be in sync with nodeManagers,
       * if a nodemanager in the expiry queue cannot be found in nodemanagers
       * its alright. We do not want to hold a hold on nodeManagers while going
       * through the expiry queue.
       */
      
      List<NodeId> expired = new ArrayList<NodeId>();
      LOG.info("Starting expiring thread with interval " + nmExpiryInterval);
      
      while (!stop) {
        org.apache.hadoop.yarn.server.api.records.NodeStatus leastRecent;
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized(nmExpiryQueue) {
          while ((nmExpiryQueue.size() > 0) &&
              (leastRecent = nmExpiryQueue.first()) != null &&
              ((now - leastRecent.getLastSeen()) > 
              nmExpiryInterval)) {
            nmExpiryQueue.remove(leastRecent);
            NodeInfoTracker info;
            synchronized(nodeManagers) {
              info = nodeManagers.get(leastRecent.getNodeId());
            }
            if (info == null) {
              continue;
            }
            org.apache.hadoop.yarn.server.api.records.NodeStatus status = info.getNodeStatus();
            if ((now - status.getLastSeen()) > nmExpiryInterval) {
              expired.add(status.getNodeId());
            } else {
              nmExpiryQueue.add(status);
            }
          }
        }
        expireNMs(expired);
      }
    }
  }
}
