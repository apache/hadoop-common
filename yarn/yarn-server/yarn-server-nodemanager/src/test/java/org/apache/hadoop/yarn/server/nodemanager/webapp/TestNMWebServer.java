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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestNMWebServer {

  @Test
  public void testNMWebApp() throws InterruptedException {
    Context nmContext = new NodeManager.NMContext();
    ResourceView resourceView = new ResourceView() {
      @Override
      public long getVmemAllocatedForContainers() {
        return 0;
      }
      @Override
      public long getPmemAllocatedForContainers() {
        return 0;
      }
    };
    WebServer server = new WebServer(nmContext, resourceView);
    Configuration conf = new Configuration();
    server.init(conf);
    server.start();

    // Add a couple of applications and the corresponding containers
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(conf);
    Dispatcher dispatcher = new AsyncDispatcher();
    String user = "nobody";
    long clusterTimeStamp = 1234;
    Map<String, String> env = new HashMap<String, String>();
    Map<String, LocalResource> resources =
        new HashMap<String, LocalResource>();
    ByteBuffer containerTokens = ByteBuffer.allocate(0);
    ApplicationId appId1 =
        BuilderUtils.newApplicationId(recordFactory, clusterTimeStamp, 1);
    ApplicationId appId2 =
        BuilderUtils.newApplicationId(recordFactory, clusterTimeStamp, 2);
    for (ApplicationId appId : new ApplicationId[] { appId1, appId2 }) {
      Application app =
          new ApplicationImpl(dispatcher, user, appId, env, resources,
              containerTokens);
      nmContext.getApplications().put(appId, app);
    }
    ContainerId container11 =
        BuilderUtils.newContainerId(recordFactory, appId1, 0);
    ContainerId container12 =
        BuilderUtils.newContainerId(recordFactory, appId1, 1);
    ContainerId container21 =
        BuilderUtils.newContainerId(recordFactory, appId2, 0);
    for (ContainerId containerId : new ContainerId[] { container11,
        container12, container21 }) {
      // TODO: Use builder utils
      ContainerLaunchContext launchContext =
          recordFactory.newRecordInstance(ContainerLaunchContext.class);
      launchContext.setContainerId(containerId);
      Container container = new ContainerImpl(dispatcher, launchContext);
      nmContext.getContainers().put(containerId, container);
      //TODO: Gross hack. Fix in code.
      nmContext.getApplications().get(containerId.getAppId()).getContainers()
          .put(containerId, container);

    }
//    Thread.sleep(1000000);
  }
}
