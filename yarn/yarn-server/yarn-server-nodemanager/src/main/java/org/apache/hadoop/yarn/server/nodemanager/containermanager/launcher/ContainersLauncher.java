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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOG_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOG_DIR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * The launcher for the containers. This service should be started only after
 * the {@link ResourceLocalizationService} is started as it depends on creation
 * of system directories on the local file-system.
 * 
 */
public class ContainersLauncher extends AbstractService
    implements EventHandler<ContainersLauncherEvent> {

  private final Context context;
  private final ContainerExecutor exec;
  private final Dispatcher dispatcher;
  private final ExecutorService containerLauncher =
    Executors.newCachedThreadPool();
  private List<Path> logDirs;
  private List<Path> localDirs;
  private List<Path> sysDirs;
  private final Map<ContainerId,RunningContainer> running =
    Collections.synchronizedMap(new HashMap<ContainerId,RunningContainer>());

  private static final class RunningContainer {
    public RunningContainer(String string, Future<Integer> submit) {
      this.user = string;
      this.runningcontainer = submit;
    }

    String user;
    Future<Integer> runningcontainer;
  }


  public ContainersLauncher(Context context, Dispatcher dispatcher,
      ContainerExecutor exec) {
    super("containers-launcher");
    this.exec = exec;
    this.context = context;
    this.dispatcher = dispatcher;
  }

  @Override
  public void init(Configuration conf) {
    // TODO factor this out of Localizer
    try {
      FileContext lfs = FileContext.getLocalFSFileContext(conf);
      String[] sLocalDirs = conf.getStrings(NM_LOCAL_DIR, DEFAULT_NM_LOCAL_DIR);

      localDirs = new ArrayList<Path>(sLocalDirs.length);
      logDirs = new ArrayList<Path>(sLocalDirs.length);
      sysDirs = new ArrayList<Path>(sLocalDirs.length);
      for (String sLocaldir : sLocalDirs) {
        Path localdir = new Path(sLocaldir);
        localDirs.add(localdir);
        // $local/nmPrivate
        Path sysdir = new Path(localdir, ResourceLocalizationService.NM_PRIVATE_DIR);
        sysDirs.add(sysdir);
      }
      String[] sLogdirs = conf.getStrings(NM_LOG_DIR, DEFAULT_NM_LOG_DIR);
      for (String sLogdir : sLogdirs) {
        Path logdir = new Path(sLogdir);
        logDirs.add(logdir);
      }
    } catch (IOException e) {
      throw new YarnException("Failed to start ContainersLauncher", e);
    }
    localDirs = Collections.unmodifiableList(localDirs);
    logDirs = Collections.unmodifiableList(logDirs);
    sysDirs = Collections.unmodifiableList(sysDirs);
    super.init(conf);
  }

  @Override
  public void stop() {
    containerLauncher.shutdownNow();
    super.stop();
  }

  @Override
  public void handle(ContainersLauncherEvent event) {
    // TODO: ContainersLauncher launches containers one by one!!
    Container container = event.getContainer();
    ContainerId containerId = container.getContainerID();
    String userName = container.getUser();
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        Application app =
          context.getApplications().get(containerId.getAppId());
        List<Path> appDirs = new ArrayList<Path>(localDirs.size());
        for (Path p : localDirs) {
          Path usersdir = new Path(p, ContainerLocalizer.USERCACHE);
          Path userdir = new Path(usersdir, userName);
          Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
          appDirs.add(new Path(appsdir, app.toString()));
        }
        Path appSysDir =
          new Path(sysDirs.get(0), ConverterUtils.toString(app.getAppId()));
        // TODO set in Application
        //Path appLogDir = new Path(logDirs.get(0), app.toString());
        ContainerLaunch launch =
          new ContainerLaunch(dispatcher, exec, app,
              event.getContainer(), appSysDir, appDirs);
        running.put(containerId,
            new RunningContainer(userName,
                containerLauncher.submit(launch)));
        break;
      case CLEANUP_CONTAINER:
        RunningContainer rContainerDatum = running.remove(containerId);
        Future<Integer> rContainer = rContainerDatum.runningcontainer;
        if (rContainer != null) {
  
          if (rContainer.isDone()) {
            // The future is already done by this time.
            break;
          }
  
          // Cancel the future so that it won't be launched if it isn't already.
          rContainer.cancel(false);
  
          // Kill the container
          String processId = exec.getProcessId(containerId);
          if (processId != null) {
            try {
              exec.signalContainer(rContainerDatum.user,
                  processId, Signal.KILL);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        }
        break;
    }
  }

}
