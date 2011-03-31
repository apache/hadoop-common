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

package org.apache.hadoop.yarn.server;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;

public class MiniYARNCluster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(MiniYARNCluster.class);

  private NodeManager nodeManager;
  private ResourceManager resourceManager;

  private ResourceManagerWrapper resourceManagerWrapper;
  private NodeManagerWrapper nodeManagerWrapper;
  
  private File testWorkDir;

  public MiniYARNCluster(String testName) {
    super(testName);
    this.testWorkDir = new File("target", testName);
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(testWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      LOG.warn("COULD NOT CLEANUP", e);
      throw new YarnException("could not cleanup test dir", e);
    } 
    resourceManagerWrapper = new ResourceManagerWrapper();
    addService(resourceManagerWrapper);
    nodeManagerWrapper = new NodeManagerWrapper();
    addService(nodeManagerWrapper);
  }

  public File getTestWorkDir() {
    return testWorkDir;
  }

  public ResourceManager getResourceManager() {
    return this.resourceManager;
  }

  public NodeManager getNodeManager() {
    return this.nodeManager;
  }

  private class ResourceManagerWrapper extends AbstractService {
    public ResourceManagerWrapper() {
      super(ResourceManagerWrapper.class.getName());
    }

    @Override
    public synchronized void start() {
      try {
        resourceManager = new ResourceManager() {
          @Override
          protected void doSecureLogin() throws IOException {
            // Don't try to login using keytab in the testcase.
          };
        };
        resourceManager.init(getConfig());
        new Thread() {
          public void run() {
            resourceManager.start();
          };
        }.start();
        while (resourceManager.getServiceState() == STATE.INITED) {
          LOG.info("Waiting for RM to start...");
          Thread.sleep(1500);
        }
        if (resourceManager.getServiceState() != STATE.STARTED) {
          // RM could have failed.
          throw new IOException("ResourceManager failed to start");
        }
        super.start();
      } catch (Throwable t) {
        throw new YarnException(t);
      }
    }
  }

  private class NodeManagerWrapper extends AbstractService {
    public NodeManagerWrapper() {
      super(NodeManagerWrapper.class.getName());
    }

    public void start() {
      try {
        File localDir =
            new File(testWorkDir, MiniYARNCluster.this.getName() + "-localDir");
        localDir.mkdir();
        LOG.info("Created localDir in " + localDir.getAbsolutePath());
        getConfig().set(NMConfig.NM_LOCAL_DIR, localDir.getAbsolutePath());
        nodeManager = new NodeManager() {

          @Override
          protected void doSecureLogin() throws IOException {
            // Don't try to login using keytab in the testcase.
          };

          @Override
          protected
              org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater
              createNodeStatusUpdater(
                  org.apache.hadoop.yarn.server.nodemanager.Context context,
                  Dispatcher dispatcher,
                  NodeHealthCheckerService healthChecker) {
            return new NodeStatusUpdaterImpl(context, dispatcher,
                healthChecker) {
              @Override
              protected ResourceTracker getRMClient() {
                // For in-process communication without RPC
                return resourceManager.getResourceTracker();
              };
            };
          };
        };
        nodeManager.init(getConfig());
        new Thread() {
          public void run() {
            nodeManager.start();
          };
        }.start();
        while (nodeManager.getServiceState() == STATE.INITED) {
          LOG.info("Waiting for NM to start...");
          Thread.sleep(1000);
        }
        if (nodeManager.getServiceState() != STATE.STARTED) {
          // RM could have failed.
          throw new IOException("NodeManager failed to start");
        }
      } catch (Throwable t) {
        throw new YarnException(t);
      }
    }
  }
}
