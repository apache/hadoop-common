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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.junit.Before;
import org.junit.Test;

public class TestNMExpiry extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestNMExpiry.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  RMResourceTrackerImpl resourceTracker;
  ContainerTokenSecretManager containerTokenSecretManager = 
    new ContainerTokenSecretManager();
  AtomicInteger test = new AtomicInteger();
  AtomicInteger notify = new AtomicInteger();

  private static class VoidResourceListener implements ResourceListener {
    @Override
    public NodeInfo addNode(NodeId nodeId, String hostName, Node node,
        Resource capability) {
      return new NodeManager(nodeId, hostName, node, capability);
    }
    @Override
    public void removeNode(NodeInfo node) {
    }
    @Override
    public NodeResponse nodeUpdate(NodeInfo nodeInfo,
        Map<String, List<Container>> containers) {
      return new NodeResponse(new ArrayList<ApplicationId>(),
          new ArrayList<Container>(), new ArrayList<Container>());
    }
  }

  private class TestRMResourceTrackerImpl extends RMResourceTrackerImpl {
    public TestRMResourceTrackerImpl(
        ContainerTokenSecretManager containerTokenSecretManager) {
      super(containerTokenSecretManager, new VoidResourceListener());
    }

    @Override
    protected void expireNMs(List<NodeId> ids) {
      for (NodeId id: ids) {
        LOG.info("Expired  " + id);
        if (test.addAndGet(1) == 2) {
          try {
            /* delay atleast 2 seconds to make sure the 3rd one does not expire
             * 
             */
            Thread.sleep(2000);
          } catch(InterruptedException ie){}
          synchronized(notify) {
            notify.addAndGet(1);
            notify.notifyAll();
          }
        }
      }
    }
  }

  @Before
  public void setUp() {
    resourceTracker = new TestRMResourceTrackerImpl(containerTokenSecretManager);
    Configuration conf = new Configuration();
    conf.setLong(YarnConfiguration.NM_EXPIRY_INTERVAL, 1000);
    resourceTracker.init(conf);
    resourceTracker.start();
  }

  private class TestThread extends Thread {
    public void run() {
      HeartbeatResponse res = recordFactory.newRecordInstance(HeartbeatResponse.class);
      while (!stopT) {
        try {
          org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus = recordFactory.newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
          nodeStatus.setNodeId(response.getNodeId());
          nodeStatus.setResponseId(res.getResponseId());
          NodeHeartbeatRequest request = recordFactory.newRecordInstance(NodeHeartbeatRequest.class);
          request.setNodeStatus(nodeStatus);
          res = resourceTracker.nodeHeartbeat(request).getHeartbeatResponse();
        } catch(Exception e) {
          LOG.info("failed to heartbeat ", e);
        }
      }
    } 
  }

  boolean stopT = false;
  RegistrationResponse response;

  @Test
  public void testNMExpiry() throws Exception {
    String hostname1 = "localhost:1";
    String hostname2 = "localhost:2";
    String hostname3 = "localhost:3";
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    RegisterNodeManagerRequest request1 = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request1.setNode(hostname1);
    request1.setResource(capability);
    RegisterNodeManagerRequest request2 = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request2.setNode(hostname2);
    request2.setResource(capability);
    RegisterNodeManagerRequest request3 = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request3.setNode(hostname3);
    request3.setResource(capability);
    resourceTracker.registerNodeManager(request1);
    resourceTracker.registerNodeManager(request2);
    response = resourceTracker.registerNodeManager(request3).getRegistrationResponse();
    /* test to see if hostanme 3 does not expire */
    stopT = false;
    new TestThread().start();
    synchronized (notify) {
      while (notify.get() == 0) {
        notify.wait();
      }
    }
    if (test.get() != 2) 
      assertTrue(false);

    stopT = true;
  }
}
