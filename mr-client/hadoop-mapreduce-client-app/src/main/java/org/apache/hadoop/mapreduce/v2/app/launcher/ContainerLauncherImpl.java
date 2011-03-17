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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMasterConstants;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerManagerSecurityInfo;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerManager;
import org.apache.hadoop.yarn.ContainerToken;

/**
 * This class is responsible for launching of containers.
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  private static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  private AppContext context;
  private ThreadPoolExecutor launcherPool;
  private Thread eventHandlingThread;
  private BlockingQueue<ContainerLauncherEvent> eventQueue =
      new LinkedBlockingQueue<ContainerLauncherEvent>();

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
  }

  @Override
  public synchronized void init(Configuration conf) {
    // Clone configuration for this component so that the SecurityInfo setting
    // doesn't affect the original configuration
    Configuration myLocalConfig = new Configuration(conf);
    myLocalConfig.setClass(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_INFO_CLASS_NAME,
        ContainerManagerSecurityInfo.class, SecurityInfo.class);
    super.init(myLocalConfig);
  }

  public void start() {
    launcherPool =
        new ThreadPoolExecutor(getConfig().getInt(
            MRAppMasterConstants.CONTAINERLAUNCHER_THREADPOOL_SIZE, 10),
            Integer.MAX_VALUE, 1, TimeUnit.HOURS,
            new LinkedBlockingQueue<Runnable>());
    launcherPool.prestartAllCoreThreads(); // Wait for work.
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        ContainerLauncherEvent event = null;
        while (!Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }
          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(new EventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  public void stop() {
    eventHandlingThread.interrupt();
    launcherPool.shutdown();
    super.stop();
  }

  protected ContainerManager getCMProxy(ContainerID containerID,
      final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {

    UserGroupInformation user = UserGroupInformation.getLoginUser();
    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token =
          new Token<ContainerTokenIdentifier>(
              containerToken.identifier.array(),
              containerToken.password.array(), new Text(
                  containerToken.kind.toString()), new Text(
                  containerToken.service.toString()));
      user.addToken(token);
    }
    ContainerManager proxy =
        user.doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            YarnRPC rpc = YarnRPC.create(getConfig());
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                NetUtils.createSocketAddr(containerManagerBindAddr),
                getConfig());
          }
        });
    return proxy;
  }

  /**
   * Setup and start the container on remote nodemanager.
   */
  private class EventProcessor implements Runnable {
    private ContainerLauncherEvent event;

    EventProcessor(ContainerLauncherEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.
      final String containerManagerBindAddr = event.getContainerMgrAddress();
      ContainerID containerID = event.getContainerID();
      ContainerToken containerToken = event.getContainerToken();

      switch(event.getType()) {

      case CONTAINER_REMOTE_LAUNCH:
        ContainerRemoteLaunchEvent launchEv = (ContainerRemoteLaunchEvent) event;

        try {
          
          ContainerManager proxy = 
            getCMProxy(containerID, containerManagerBindAddr, containerToken);
          
          // Construct the actual Container
          ContainerLaunchContext containerLaunchContext =
              launchEv.getContainer();

          // TODO: Make sure that child's mapred-local-dir is set correctly.

          // Now launch the actual container
          proxy.startContainer(containerLaunchContext);

          // after launching send launched event to taskattempt
          context.getEventHandler().handle(
              new TaskAttemptEvent(launchEv.getTaskAttemptID(),
                  TaskAttemptEventType.TA_CONTAINER_LAUNCHED));
        } catch (Exception e) {
          LOG.error("Container launch failed", e);
          context.getEventHandler().handle(
              new TaskAttemptEvent(launchEv.getTaskAttemptID(),
                  TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
        }

        break;
      case CONTAINER_REMOTE_CLEANUP:

        // We will have to remove the launch event if it is still in eventQueue
        // and not yet processed
        if (eventQueue.contains(event)) {
          eventQueue.remove(event); // TODO: Any synchro needed?
          // TODO: raise any event?
        } else {
          try {
            ContainerManager proxy = 
              getCMProxy(containerID, containerManagerBindAddr, containerToken);
            // TODO:check whether container is launched

            // kill the remote container if already launched
            proxy.stopContainer(event.getContainerID());
            proxy.cleanupContainer(event.getContainerID());
          } catch (Exception e) {
            //ignore the cleanup failure
            LOG.warn("cleanup failed for container " + event.getContainerID() ,
                e);
          }

            // after killing send killed event to taskattempt
            context.getEventHandler().handle(
                new TaskAttemptEvent(event.getTaskAttemptID(),
                    TaskAttemptEventType.TA_CONTAINER_CLEANED));
        }
        break;
      }
    }
    
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }
}
