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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.HeartbeatResponse;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.RegistrationResponse;
import org.apache.hadoop.yarn.Resource;
import org.junit.Before;
import org.junit.Test;

public class TestNMExpiry extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestNMExpiry.class);

  RMResourceTrackerImpl resourceTracker;
  ContainerTokenSecretManager containerTokenSecretManager = 
    new ContainerTokenSecretManager();
  AtomicInteger test = new AtomicInteger();
  Object notify = new Object();

  private static class VoidResourceListener implements ResourceListener {
    @Override
    public NodeInfo addNode(NodeID nodeId, String hostName, Node node,
        Resource capability) {
      return new NodeManager(nodeId, hostName, node, capability);
    }
    @Override
    public void removeNode(NodeInfo node) {
    }
    @Override
    public NodeResponse nodeUpdate(NodeInfo nodeInfo,
        Map<CharSequence, List<Container>> containers) {
      return new NodeResponse(new ArrayList<ApplicationID>(),
          new ArrayList<Container>(), new ArrayList<Container>());
    }
  }

  private class TestRMResourceTrackerImpl extends RMResourceTrackerImpl {
    public TestRMResourceTrackerImpl(
        ContainerTokenSecretManager containerTokenSecretManager) {
      super(containerTokenSecretManager, new VoidResourceListener());
    }

    @Override
    protected void expireNMs(List<NodeID> ids) {
      for (NodeID id: ids) {
        LOG.info("Expired  " + id);
        if (test.addAndGet(1) == 2) {
          try {
            /* delay atleast 2 seconds to make sure the 3rd one does not expire
             * 
             */
            Thread.sleep(2000);
          } catch(InterruptedException ie){}
          synchronized(notify) {
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
      HeartbeatResponse res = new HeartbeatResponse();
      while (!stopT) {
        try {
          org.apache.hadoop.yarn.NodeStatus nodeStatus = new org.apache.hadoop.yarn.NodeStatus();
          nodeStatus.nodeId = response.nodeID;
          nodeStatus.responseId = res.responseId;
          res = resourceTracker.nodeHeartbeat(nodeStatus);
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
    Resource capability = new Resource();
    resourceTracker.registerNodeManager(hostname1, capability);
    resourceTracker.registerNodeManager(hostname2, capability);
    response = resourceTracker.registerNodeManager(hostname3, capability);
    /* test to see if hostanme 3 does not expire */
    stopT = false;
    new TestThread().start();
    synchronized (notify) {
      notify.wait(10000);
    }
    if (test.get() != 2) 
      assertTrue(false);

    stopT = true;
  }
}
