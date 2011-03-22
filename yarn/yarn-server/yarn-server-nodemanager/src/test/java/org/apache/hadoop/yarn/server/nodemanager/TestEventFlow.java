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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.junit.Test;

public class TestEventFlow {

  private static final Log LOG = LogFactory.getLog(TestEventFlow.class);

  @Test
  public void testSuccessfulContainerLaunch() throws InterruptedException,
      AvroRemoteException {
    Context context = new NMContext();

    YarnConfiguration conf = new YarnConfiguration();
    ContainerExecutor exec = new DefaultContainerExecutor();
    DeletionService del = new DeletionService(exec);
    Dispatcher dispatcher = new AsyncDispatcher();
    NodeStatusUpdater nodeStatusUpdater =
        new NodeStatusUpdaterImpl(context, dispatcher) {
      @Override
      protected org.apache.hadoop.yarn.ResourceTracker getRMClient() {
        return new LocalRMInterface();
      };

      @Override
      protected void startStatusUpdater() throws InterruptedException,
          AvroRemoteException {
        return; // Don't start any updating thread.
      }
    };

    DummyContainerManager containerManager =
        new DummyContainerManager(context, exec, del, nodeStatusUpdater);
    containerManager.init(new Configuration());
    containerManager.start();

    ContainerLaunchContext launchContext = new ContainerLaunchContext();
    ContainerID cID = new ContainerID();
    cID.appID = new ApplicationID();
    launchContext.id = cID;
    launchContext.user = "testing";
    launchContext.resource = new Resource();
    launchContext.env = new HashMap<CharSequence, CharSequence>();
    launchContext.command = new ArrayList<CharSequence>();
    containerManager.startContainer(launchContext);

    DummyContainerManager.waitForContainerState(containerManager, cID,
        ContainerState.RUNNING);

    containerManager.stopContainer(cID);
    DummyContainerManager.waitForContainerState(containerManager, cID,
        ContainerState.COMPLETE);

    containerManager.stop();
  }
}
