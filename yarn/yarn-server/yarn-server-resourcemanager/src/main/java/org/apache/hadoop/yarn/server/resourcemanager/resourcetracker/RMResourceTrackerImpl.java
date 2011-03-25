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

import org.apache.avro.ipc.AvroRemoteException;
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
import org.apache.hadoop.yarn.HeartbeatResponse;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.NodeStatus;
import org.apache.hadoop.yarn.RegistrationResponse;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceTracker;
import org.apache.hadoop.yarn.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
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
  private Map<String, NodeID> nodes = new ConcurrentHashMap<String, NodeID>();
  private final Map<NodeID, NodeInfoTracker> nodeManagers = 
    new ConcurrentHashMap<NodeID, NodeInfoTracker>();
  private final HeartBeatThread heartbeatThread;
  private final TreeSet<NodeStatus> nmExpiryQueue =
      new TreeSet<NodeStatus>(
          new Comparator<NodeStatus>() {
            public int compare(NodeStatus p1, NodeStatus p2) {
              if (p1.lastSeen < p2.lastSeen) {
                return -1;
              } else if (p1.lastSeen > p2.lastSeen) {
                return 1;
              } else {
                return (p1.nodeId.id -
                    p2.nodeId.id);
              }
            }
          }
      );
  
  private ResourceListener resourceListener;
  private InetSocketAddress resourceTrackerAddress;
  private Server server;
  private final ContainerTokenSecretManager containerTokenSecretManager;
  private final AtomicInteger nodeCounter = new AtomicInteger(0);
  private static final HeartbeatResponse reboot = new HeartbeatResponse();
  private long nmExpiryInterval;

  public RMResourceTrackerImpl(ContainerTokenSecretManager containerTokenSecretManager,
      ResourceListener listener) {
    super(RMResourceTrackerImpl.class.getName());
    reboot.reboot = true;
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
  public RegistrationResponse registerNodeManager(CharSequence node,
      Resource capability) throws AvroRemoteException {
    NodeID nodeId = getNodeId(node);
    NodeInfoTracker nTracker = null;
    
    synchronized(nodeManagers) {
      if (!nodeManagers.containsKey(nodeId)) {
        /* we do the resolving here, so that scheduler does not have to do it */
        NodeInfo nodeManager = resourceListener.addNode(nodeId, node.toString(),
            resolve(node.toString()),
            capability);
        HeartbeatResponse response = new HeartbeatResponse();
        response.responseId = 0;
        nTracker = new NodeInfoTracker(nodeManager, response);
        nodeManagers.put(nodeId, nTracker);
      } else {
        nTracker = nodeManagers.get(nodeId);
        NodeStatus status = nTracker.getNodeStatus();
        status.lastSeen = System.currentTimeMillis();
        nTracker.updateNodeStatus(status);
      }
    }
    addForTracking(nTracker.getNodeStatus());
    LOG.info("NodeManager from node " + node + " registered with capability: " + 
        capability.memory + ", assigned nodeId " + nodeId.id);

    RegistrationResponse regResponse = new RegistrationResponse();
    regResponse.nodeID = nodeId;
    SecretKey secretKey =
      this.containerTokenSecretManager.createAndGetSecretKey(node);
    regResponse.secretKey = ByteBuffer.wrap(secretKey.getEncoded());
    return regResponse;
  }

  @Override
  public HeartbeatResponse nodeHeartbeat(NodeStatus nodeStatus)
  throws AvroRemoteException {
    nodeStatus.lastSeen = System.currentTimeMillis();
    NodeInfoTracker nTracker = null;
    synchronized(nodeManagers) {
      nTracker = nodeManagers.get(nodeStatus.nodeId);
    }
    if (nTracker == null) {
      /* node does not exist */
      LOG.info("Node not found rebooting " + nodeStatus.nodeId);
      return reboot;
    }

    NodeInfo nodeInfo = nTracker.getNodeInfo();
    /* check to see if its an old heartbeat */    
    if (nodeStatus.responseId + 1 == nTracker.getLastHeartBeatResponse().responseId) {
      return nTracker.getLastHeartBeatResponse();
    } else if (nodeStatus.responseId + 1 < nTracker.getLastHeartBeatResponse().responseId) {
      LOG.info("Too far behind rm response id:" + 
          nTracker.lastHeartBeatResponse.responseId + " nm response id:" + nodeStatus.responseId);
      return reboot;
    }

    /* inform any listeners of node heartbeats */
    NodeResponse nodeResponse = resourceListener.nodeUpdate(
        nodeInfo, nodeStatus.containers);

    
    HeartbeatResponse response = new HeartbeatResponse();
    response.containersToCleanup = nodeResponse.getContainersToCleanUp();

    
    response.appplicationsToCleanup = nodeResponse.getFinishedApplications();
    response.responseId = nTracker.getLastHeartBeatResponse().responseId + 1;

    nTracker.refreshHeartBeatResponse(response);
    nTracker.updateNodeStatus(nodeStatus);
    return response;
  }

  @Private
  public synchronized NodeInfo getNodeManager(NodeID nodeId) {
    NodeInfoTracker ntracker = nodeManagers.get(nodeId);
    return (ntracker == null ? null: ntracker.getNodeInfo());
  }

  private synchronized NodeID getNodeId(CharSequence node) {
    NodeID nodeId;
    nodeId = nodes.get(node);
    if (nodeId == null) {
      nodeId = new NodeID();
      nodeId.id = nodeCounter.getAndIncrement();
      nodes.put(node.toString(), nodeId);
    }
    return nodeId;
  }

  @Override
  public synchronized YarnClusterMetrics getClusterMetrics() {
    YarnClusterMetrics ymetrics = new YarnClusterMetrics();
    ymetrics.numNodeManagers = nodeManagers.size();
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

  protected void addForTracking(NodeStatus status) {
    synchronized(nmExpiryQueue) {
      nmExpiryQueue.add(status);
    }
  }
  
  protected void expireNMs(List<NodeID> nodes) {
    for (NodeID id: nodes) {
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
      
      List<NodeID> expired = new ArrayList<NodeID>();
      LOG.info("Starting expiring thread with interval " + nmExpiryInterval);
      
      while (!stop) {
        NodeStatus leastRecent;
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized(nmExpiryQueue) {
          while ((nmExpiryQueue.size() > 0) &&
              (leastRecent = nmExpiryQueue.first()) != null &&
              ((now - leastRecent.lastSeen) > 
              nmExpiryInterval)) {
            nmExpiryQueue.remove(leastRecent);
            NodeInfoTracker info;
            synchronized(nodeManagers) {
              info = nodeManagers.get(leastRecent.nodeId);
            }
            if (info == null) {
              continue;
            }
            NodeStatus status = info.getNodeStatus();
            if ((now - status.lastSeen) > nmExpiryInterval) {
              expired.add(status.nodeId);
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
