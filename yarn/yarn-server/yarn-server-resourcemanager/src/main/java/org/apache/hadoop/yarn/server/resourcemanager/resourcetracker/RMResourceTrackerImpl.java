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

import java.io.IOException;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NodeStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManagerImpl;
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
ResourceTracker, ClusterTracker {
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

  private final TreeSet<NodeId> nmExpiryQueue =
    new TreeSet<NodeId>(
        new Comparator<NodeId>() {
          public int compare(NodeId n1, NodeId n2) {
            NodeInfoTracker nit1 = nodeManagers.get(n1);
            NodeInfoTracker nit2 = nodeManagers.get(n2);
            long p1LastSeen = nit1.getNodeLastSeen();
            long p2LastSeen = nit2.getNodeLastSeen();
            if (p1LastSeen < p2LastSeen) {
              return -1;
            } else if (p1LastSeen > p2LastSeen) {
              return 1;
            } else {
              return (nit1.getNodeManager().getNodeID().getId() -
                  nit2.getNodeManager().getNodeID().getId());
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
  private final RMContext rmContext;
  private final NodeStore nodeStore;
  
  public RMResourceTrackerImpl(ContainerTokenSecretManager containerTokenSecretManager, RMContext context) {
    super(RMResourceTrackerImpl.class.getName());
    reboot.setReboot(true);
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.heartbeatThread = new HeartBeatThread();
    this.rmContext = context;
    this.nodeStore = context.getNodeStore();
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
  public void addListener(ResourceListener listener) {
    this.resourceListener = listener;
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
  
  protected NodeInfoTracker getAndAddNodeInfoTracker(NodeId nodeId,
      String hostString, String httpAddress, Node node, Resource capability) throws IOException {
    NodeInfoTracker nTracker = null;
    
    synchronized(nodeManagers) {
      if (!nodeManagers.containsKey(nodeId)) {
        LOG.info("DEBUG -- Adding  " + hostString);
        NodeManager nodeManager =
          new NodeManagerImpl(nodeId, hostString, httpAddress,
              node,
              capability);
        nodes.put(nodeManager.getNodeAddress(), nodeId);
        nodeStore.storeNode(nodeManager);
        /* Inform the listeners */
        resourceListener.addNode(nodeManager);
        HeartbeatResponse response = recordFactory.newRecordInstance(HeartbeatResponse.class);
        response.setResponseId(0);
        nTracker = new NodeInfoTracker(nodeManager, response);
        nodeManagers.put(nodeId, nTracker);
      } else {
        nTracker = nodeManagers.get(nodeId);
        nTracker.updateLastSeen(System.currentTimeMillis());
      }
    }
    return nTracker;
  }

  @Override
  public RegisterNodeManagerResponse registerNodeManager(RegisterNodeManagerRequest
      request) throws YarnRemoteException {
    String host = request.getHost();
    int cmPort = request.getContainerManagerPort();
    String node = host + ":" + cmPort;
    String httpAddress = host + ":" + request.getHttpPort();
    Resource capability = request.getResource();

    NodeId nodeId = getNodeId(node);
    
    NodeInfoTracker nTracker = null;
    try {
    nTracker = getAndAddNodeInfoTracker(
      nodeId, node.toString(), httpAddress,
                resolve(node.toString()),
                capability);
          // Inform the scheduler
    } catch(IOException io) {
      throw  RPCUtil.getRemoteException(io);
    }
    addForTracking(nodeId);
    LOG.info("NodeManager from node " + node + "(web-url: " + httpAddress
        + ") registered with capability: " + capability.getMemory()
        + ", assigned nodeId " + nodeId.getId());

    RegistrationResponse regResponse = recordFactory.newRecordInstance(
        RegistrationResponse.class);
    regResponse.setNodeId(nodeId);
    SecretKey secretKey =
      this.containerTokenSecretManager.createAndGetSecretKey(node);
    regResponse.setSecretKey(ByteBuffer.wrap(secretKey.getEncoded()));
    RegisterNodeManagerResponse response = recordFactory.newRecordInstance(
        RegisterNodeManagerResponse.class);
    response.setRegistrationResponse(regResponse);
    return response;
  }

  /**
   * Update the listeners. This method can be inlined but are there for 
   * making testing easier
   * @param nodeManager the {@link NodeInfo} to update.
   * @param containers the containers from the status of the node manager.
   */
  protected void updateListener(NodeInfo nodeManager, Map<String, List<Container>>
    containers) {
  /* inform any listeners of node heartbeats */
    resourceListener.nodeUpdate(
        nodeManager, containers);
  }
  
  
  /**
   * Get a response for the nodemanager heartbeat
   * @param nodeManager the nodemanager to update
   * @param containers the containers from the status update.
   * @return the {@link NodeResponse} for the node manager.
   */
  protected NodeResponse NodeResponse(NodeManager nodeManager, Map<String, 
      List<Container>> containers) {
    return nodeManager.statusUpdate(containers);
  }
  
  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request) throws YarnRemoteException {
    org.apache.hadoop.yarn.server.api.records.NodeStatus remoteNodeStatus = request.getNodeStatus();
    remoteNodeStatus.setLastSeen(System.currentTimeMillis());
    NodeInfoTracker nTracker = null;
    NodeHeartbeatResponse nodeHbResponse = recordFactory.newRecordInstance(NodeHeartbeatResponse.class);
    synchronized(nodeManagers) {
      nTracker = nodeManagers.get(remoteNodeStatus.getNodeId());
    }
    if (nTracker == null) {
      /* node does not exist */
      LOG.info("Node not found rebooting " + remoteNodeStatus.getNodeId());
      nodeHbResponse.setHeartbeatResponse(reboot);
      return nodeHbResponse;
    }

    NodeManager nodeManager = nTracker.getNodeManager();
    /* check to see if its an old heartbeat */    
    if (remoteNodeStatus.getResponseId() + 1 == nTracker
        .getLastHeartBeatResponse().getResponseId()) {
      nodeHbResponse.setHeartbeatResponse(nTracker.getLastHeartBeatResponse());
      return nodeHbResponse;
    } else if (remoteNodeStatus.getResponseId() + 1 < nTracker
        .getLastHeartBeatResponse().getResponseId()) {
      LOG.info("Too far behind rm response id:" +
          nTracker.lastHeartBeatResponse.getResponseId() + " nm response id:"
          + remoteNodeStatus.getResponseId());
      nodeHbResponse.setHeartbeatResponse(reboot);
      return nodeHbResponse;
    }
    
    /** TODO This should be 3 step process.
     * nodemanager.statusupdate
     * listener.update()
     * nodemanager.getNodeResponse()
     * This will allow flexibility in updates/scheduling/premption
     */
    NodeResponse nodeResponse = nodeManager.statusUpdate(remoteNodeStatus.getAllContainers());
    /* inform any listeners of node heartbeats */
    updateListener(
        nodeManager, remoteNodeStatus.getAllContainers());
  

    HeartbeatResponse response = recordFactory.newRecordInstance(HeartbeatResponse.class);
    response.addAllContainersToCleanup(nodeResponse.getContainersToCleanUp());

    response.addAllApplicationsToCleanup(nodeResponse.getFinishedApplications());
    response.setResponseId(nTracker.getLastHeartBeatResponse().getResponseId() + 1);

    nTracker.refreshHeartBeatResponse(response);
    nTracker.updateLastSeen(remoteNodeStatus.getLastSeen());
    boolean prevHealthStatus =
      nTracker.getNodeManager().getNodeHealthStatus().getIsNodeHealthy();
    NodeHealthStatus remoteNodeHealthStatus =
      remoteNodeStatus.getNodeHealthStatus();
    nTracker.getNodeManager().updateHealthStatus(
        remoteNodeHealthStatus);
    //    boolean prevHealthStatus = nodeHbResponse.
    nodeHbResponse.setHeartbeatResponse(response);

    // Take care of node-health
    if (prevHealthStatus != remoteNodeHealthStatus
        .getIsNodeHealthy()) {
      // Node's health-status changed.
      if (!remoteNodeHealthStatus.getIsNodeHealthy()) {
        // TODO: Node has become unhealthy, remove?
        //        LOG.info("Node " + nodeManager.getNodeID()
        //            + " has become unhealthy. Health-check report: "
        //            + remoteNodeStatus.nodeHealthStatus.healthReport
        //            + "Removing it from the scheduler.");
        //        resourceListener.removeNode(nodeManager);
      } else {
        // TODO: Node has become healthy back again, add?
        //        LOG.info("Node " + nodeManager.getNodeID()
        //            + " has become healthy back again. Health-check report: "
        //            + remoteNodeStatus.nodeHealthStatus.healthReport
        //            + " Adding it to the scheduler.");
        //        this.resourceListener.addNode(nodeManager);
      }
    }
    return nodeHbResponse;
  }

  @Private
  public synchronized NodeInfo getNodeManager(NodeId nodeId) {
    NodeInfoTracker ntracker = nodeManagers.get(nodeId);
    return (ntracker == null ? null: ntracker.getNodeManager());
  }

  private  NodeId getNodeId(String node) {
    NodeId nodeId;
    nodeId = nodes.get(node);
    if (nodeId == null) {
      nodeId = recordFactory.newRecordInstance(NodeId.class);
      nodeId.setId(nodeCounter.getAndIncrement());
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
        infoList.add(t.getNodeManager());
      }
    }
    return infoList;
  }

  protected void addForTracking(NodeId nodeID) {
    synchronized(nmExpiryQueue) {
      nmExpiryQueue.add(nodeID);
    }
  }

  protected void expireNMs(List<NodeId> nodes) {
    for (NodeId id: nodes) {
      synchronized (nodeManagers) {
        NodeInfo nInfo = nodeManagers.get(id).getNodeManager();
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
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized(nmExpiryQueue) {
          NodeId leastRecent;
          while ((nmExpiryQueue.size() > 0) &&
              (leastRecent = nmExpiryQueue.first()) != null) {
            nmExpiryQueue.remove(leastRecent);
            NodeInfoTracker info;
            synchronized(nodeManagers) {
              info = nodeManagers.get(leastRecent);
            }
            if (info == null) {
              continue;
            }
            NodeId nodeID = info.getNodeManager().getNodeID();
            if ((now - info.getNodeLastSeen()) > nmExpiryInterval) {
              LOG.info("Going to expire the node-manager " + nodeID
                  + " because of no updates for "
                  + (now - info.getNodeLastSeen())
                  + " seconds ( > expiry interval of " + nmExpiryInterval
                  + ").");
              expired.add(nodeID);
            } else {
              nmExpiryQueue.add(nodeID);
              break;
            }
          }
        }
        expireNMs(expired);
      }
    }
  }

  @Override
  public void finishedApplication(ApplicationId applicationId,
      List<NodeInfo> nodesToNotify) {
    for (NodeInfo info: nodesToNotify) {
      NodeManager node;
      synchronized(nodeManagers) {
        node = nodeManagers.get(info.getNodeID()).getNodeManager();
      }
      node.finishedApplication(applicationId);
    } 
  }

  private NodeManager getNodeManagerForContainer(Container container) {
    NodeManager node;
    synchronized (nodeManagers) {
      LOG.info("DEBUG -- Container manager address " + container.getContainerManagerAddress());
      NodeId nodeId = nodes.get(container.getContainerManagerAddress());
      node = nodeManagers.get(nodeId).getNodeManager();
    }
    return node;
  }
  @Override
  public  boolean releaseContainer(Container container) {
    NodeManager node = getNodeManagerForContainer(container);
    node.releaseContainer(container);
    return false;
  }
  
  @Override
  public void recover(RMState state) {
    List<NodeManager> nodeManagers = state.getStoredNodeManagers();
    for (NodeManager nm: nodeManagers) {
      try {
        getAndAddNodeInfoTracker(nm.getNodeID(), nm.getNodeAddress(), nm.getHttpAddress(), 
          nm.getNode(), nm.getTotalCapability());
      } catch(IOException ie) {
        //ignore
      }
    }
    for (Map.Entry<ApplicationId, ApplicationInfo> entry: state.getStoredApplications().entrySet()) {
      List<Container> containers = entry.getValue().getContainers();
      List<Container> containersToAdd = new ArrayList<Container>();
      for (Container c: containers) {
        NodeManager containerNode = getNodeManagerForContainer(c);
        containersToAdd.add(c);
        containerNode.allocateContainer(entry.getKey(), containersToAdd);
        containersToAdd.clear();
      }
    }
  }

}
