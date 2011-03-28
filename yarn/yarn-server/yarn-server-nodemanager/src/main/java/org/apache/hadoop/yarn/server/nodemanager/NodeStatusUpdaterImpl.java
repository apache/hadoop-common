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

package org.apache.hadoop.yarn.server.nodemanager;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.NodeHealthStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.HeartbeatResponse;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.NodeStatus;
import org.apache.hadoop.yarn.RegistrationResponse;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceTracker;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.service.AbstractService;

public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  private static final Log LOG = LogFactory.getLog(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();

  private Context context;
  private Dispatcher dispatcher;

  private long heartBeatInterval;
  private ResourceTracker resourceTracker;
  private String rmAddress;
  private Resource totalResource;
  private String nodeName;
  private NodeID nodeId;
  private byte[] secretKeyBytes = new byte[0];
  private boolean isStopped;

  private final NodeHealthCheckerService healthChecker;

  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.rmAddress =
        conf.get(YarnServerConfig.RESOURCETRACKER_ADDRESS,
            YarnServerConfig.DEFAULT_RESOURCETRACKER_BIND_ADDRESS);
    this.heartBeatInterval =
        conf.getLong(NMConfig.HEARTBEAT_INTERVAL,
            NMConfig.DEFAULT_HEARTBEAT_INTERVAL);
    int memory = conf.getInt(NMConfig.NM_RESOURCE, 8);
    this.totalResource = new Resource();
    this.totalResource.memory = memory * 1024;
    super.init(conf);
  }

  @Override
  public void start() {
    String bindAddress =
        getConfig().get(NMConfig.NM_BIND_ADDRESS,
            NMConfig.DEFAULT_NM_BIND_ADDRESS);
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddress);
    try {
      this.nodeName =
          addr.getAddress().getLocalHost().getHostAddress() + ":"
              + addr.getPort();
      LOG.info("Configured ContainerManager Address is " + this.nodeName);
      // Registration has to be in start so that ContainerManager can get the
      // perNM tokens needed to authenticate ContainerTokens.
      registerWithRM();
      super.start();
      startStatusUpdater();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public synchronized void stop() {
    // Interrupt the updater.
    this.isStopped = true;
    super.stop();
  }

  protected ResourceTracker getRMClient() {
    YarnRPC rpc = YarnRPC.create(getConfig());
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.rmAddress);
    getConfig().setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        RMNMSecurityInfoClass.class, SecurityInfo.class);
    return (ResourceTracker) rpc.getProxy(ResourceTracker.class, rmAddress,
        getConfig());
  }

  private void registerWithRM() throws AvroRemoteException {
    this.resourceTracker = getRMClient();
    LOG.info("Connected to ResourceManager at " + this.rmAddress);
    RegistrationResponse regResponse =
        this.resourceTracker.registerNodeManager(this.nodeName,
            this.totalResource);
    this.nodeId = regResponse.nodeID;
    if (UserGroupInformation.isSecurityEnabled()) {
      this.secretKeyBytes = regResponse.secretKey.array();
    }

    LOG.info("Registered with ResourceManager as " + this.nodeName
        + " with total resource of " + this.totalResource);
  }

  @Override
  public String getNodeName() {
    return this.nodeName;
  }

  @Override
  public byte[] getRMNMSharedSecret() {
    return this.secretKeyBytes;
  }

  private NodeStatus getNodeStatus() {
    NodeStatus status = new NodeStatus();
    status.nodeId = this.nodeId;
    status.containers =
        new HashMap<CharSequence, List<org.apache.hadoop.yarn.Container>>();
    Map<CharSequence, List<org.apache.hadoop.yarn.Container>> activeContainers =
        status.containers;

    int numActiveContainers = 0;
    synchronized (this.context.getContainers()) {
      for (Iterator<Entry<ContainerID, Container>> i =
          this.context.getContainers().entrySet().iterator(); i.hasNext();) {
        Entry<ContainerID, Container> e = i.next();
        ContainerID containerId = e.getKey();
        Container container = e.getValue();
        CharSequence applicationId = String.valueOf(containerId.appID.id); // TODO: ID? Really?

        List<org.apache.hadoop.yarn.Container> applicationContainers =
            activeContainers.get(applicationId);
        if (applicationContainers == null) {
          applicationContainers = new ArrayList<org.apache.hadoop.yarn.Container>();
          activeContainers.put(applicationId, applicationContainers);
        }

        // Clone the container to send it to the RM
        org.apache.hadoop.yarn.Container c = container.cloneAndGetContainer();
        c.hostName = this.nodeName;
        applicationContainers.add(c);
        ++numActiveContainers;
        LOG.info("Sending out status for container: " + c);

        if (c.state == ContainerState.COMPLETE) {
          // Remove
          i.remove();

          LOG.info("Removed completed container " + containerId);
        }
      }
    }

    LOG.debug(this.nodeName + " sending out status for " + numActiveContainers
        + " containers");

    if (this.healthChecker != null) {
      NodeHealthStatus nodeHealthStatus = this.context.getNodeHealthStatus();
      this.healthChecker.setHealthStatus(nodeHealthStatus);
      status.isNodeHealthy = nodeHealthStatus.isNodeHealthy();
      status.healthReport = nodeHealthStatus.getHealthReport();
      status.lastHealthReport = nodeHealthStatus.getLastReported();
    }

    return status;
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  protected void startStatusUpdater() throws InterruptedException,
      AvroRemoteException {

    new Thread() {
      @Override
      public void run() {
        int lastHeartBeatID = 0;
        while (!isStopped) {
          // Send heartbeat
          try {
            synchronized (heartbeatMonitor) {
              heartbeatMonitor.wait(heartBeatInterval);
            }
            NodeStatus nodeStatus = getNodeStatus();
            nodeStatus.responseId = lastHeartBeatID;
            HeartbeatResponse response =
              resourceTracker.nodeHeartbeat(nodeStatus);
            lastHeartBeatID = response.responseId;
            List<org.apache.hadoop.yarn.Container> containersToCleanup =
                response.containersToCleanup;
            if (containersToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedContainersEvent(containersToCleanup));
            }
            List<ApplicationID> appsToCleanup =
                response.appplicationsToCleanup;
            if (appsToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup));
            }
          } catch (AvroRemoteException e) {
            LOG.error("Caught exception in status-updater", e);
            break;
          } catch (InterruptedException e) {
            LOG.error("Status-updater interrupted", e);
            break;
          }
        }
      }
    }.start();
  }
}
