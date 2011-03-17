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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.HeartbeatResponse;
import org.apache.hadoop.yarn.NodeID;
import org.apache.hadoop.yarn.NodeStatus;
import org.apache.hadoop.yarn.RegistrationResponse;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.ResourceTracker;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNodeStatusUpdater {

  static final Log LOG = LogFactory.getLog(TestNodeStatusUpdater.class);
  static final Path basedir =
      new Path("target", TestNodeStatusUpdater.class.getName());

  int heartBeatID = 0;
  volatile Error nmStartError = null;

  private class MyResourceTracker implements ResourceTracker {

    private Context context;

    public MyResourceTracker(Context context) {
      this.context = context;
    }

    @Override
    public RegistrationResponse registerNodeManager(CharSequence node,
        Resource resource) throws AvroRemoteException {
      LOG.info("Registering " + node);
      try {
        Assert.assertEquals(InetAddress.getLocalHost().getHostAddress()
            + ":12345", node);
      } catch (UnknownHostException e) {
        Assert.fail(e.getMessage());
      }
      Assert.assertEquals(5 * 1024, resource.memory);
      RegistrationResponse regResponse = new RegistrationResponse();
      regResponse.nodeID = new NodeID();
      return regResponse;
    }

    ApplicationID applicationID = new ApplicationID();
    ContainerID firstContainerID = new ContainerID();
    ContainerID secondContainerID = new ContainerID();

    @Override
    public HeartbeatResponse nodeHeartbeat(NodeStatus nodeStatus)
        throws AvroRemoteException {
      LOG.info("Got heartbeat number " + heartBeatID);
      nodeStatus.responseId = heartBeatID++;
      if (heartBeatID == 1) {
        Assert.assertEquals(0, nodeStatus.containers.size());

        // Give a container to the NM.
        applicationID.id = heartBeatID;
        firstContainerID.appID = applicationID;
        firstContainerID.id = heartBeatID;
        ContainerLaunchContext launchContext = new ContainerLaunchContext();
        launchContext.id = firstContainerID;
        launchContext.resource = new Resource();
        launchContext.resource.memory = 2; // 2GB
        Container container = new ContainerImpl(null, launchContext);
        this.context.getContainers().put(firstContainerID, container);
      } else if (heartBeatID == 2) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            nodeStatus.containers.size());
        Assert.assertEquals("Number of container for the app should be one!",
            1, nodeStatus.containers.get(String.valueOf(applicationID.id))
                .size());
        Assert.assertEquals(2,
            nodeStatus.containers.get(String.valueOf(applicationID.id))
                .get(0).resource.memory);

        // Checks on the NM end
        ConcurrentMap<ContainerID, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(1, activeContainers.size());

        // Give another container to the NM.
        applicationID.id = heartBeatID;
        secondContainerID.appID = applicationID;
        secondContainerID.id = heartBeatID;
        ContainerLaunchContext launchContext = new ContainerLaunchContext();
        launchContext.id = secondContainerID;
        launchContext.resource = new Resource();
        launchContext.resource.memory = 3; // 3GB
        Container container = new ContainerImpl(null, launchContext);
        this.context.getContainers().put(secondContainerID, container);
      } else if (heartBeatID == 3) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            nodeStatus.containers.size());
        Assert.assertEquals("Number of container for the app should be two!",
            2, nodeStatus.containers.get(String.valueOf(applicationID.id))
                .size());
        Assert.assertEquals(2,
            nodeStatus.containers.get(String.valueOf(applicationID.id))
                .get(0).resource.memory);
        Assert.assertEquals(3,
            nodeStatus.containers.get(String.valueOf(applicationID.id))
                .get(1).resource.memory);

        // Checks on the NM end
        ConcurrentMap<ContainerID, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(2, activeContainers.size());
      }
      HeartbeatResponse response = new HeartbeatResponse();
      response.responseId = heartBeatID;
      response.containersToCleanup = new ArrayList<org.apache.hadoop.yarn.Container>();
      response.appplicationsToCleanup = new ArrayList<ApplicationID>();
      return response;
    }
  }

  private class MyNodeStatusUpdater extends NodeStatusUpdaterImpl {
    private Context context;

    public MyNodeStatusUpdater(Context context) {
      super(context);
      this.context = context;
    }

    @Override
    protected ResourceTracker getRMClient() {
      return new MyResourceTracker(this.context);
    }
  }

  @Before
  public void clearError() {
    nmStartError = null;
  }

  @After
  public void deleteBaseDir() throws IOException {
    FileContext lfs = FileContext.getLocalFSFileContext();
    lfs.delete(basedir, true);
  }

  @Test
  public void testNMRegistration() throws InterruptedException {
    final NodeManager nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context) {
        return new MyNodeStatusUpdater(context);
      }
    };

    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(NMConfig.NM_RESOURCE, 5); // 5GB
    conf.set(NMConfig.NM_BIND_ADDRESS, "127.0.0.1:12345");
    conf.set(NMConfig.NM_LOCALIZER_BIND_ADDRESS, "127.0.0.1:12346");
    conf.set(NMConfig.NM_LOG_DIR, new Path(basedir, "logs").toUri().getPath());
    conf.set(NMConfig.NM_LOCAL_DIR, new Path(basedir, "nm0").toUri().getPath());
    nm.init(conf);
    new Thread() {
      public void run() {
        try {
          nm.start();
        } catch (Error e) {
          TestNodeStatusUpdater.this.nmStartError = e;
        }
      }
    }.start();

    System.out.println(" ----- thread already started.."
        + nm.getServiceState());

    while (nm.getServiceState() == STATE.INITED) {
      LOG.info("Waiting for NM to start..");
      Thread.sleep(1000);
    }
    if (nmStartError != null) {
      throw nmStartError;
    }
    if (nm.getServiceState() != STATE.STARTED) {
      // NM could have failed.
      Assert.fail("NodeManager failed to start");
    }

    while (heartBeatID <= 3) {
      Thread.sleep(500);
    }

    nm.stop();
  }
}
