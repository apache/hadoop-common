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
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ApplicationLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.service.AbstractService;

import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.*;

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
  private final Map<ContainerID,Future<Integer>> running =
    Collections.synchronizedMap(new HashMap<ContainerID,Future<Integer>>());

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
    ContainerID containerId = container.getLaunchContext().id;
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        Application app =
          context.getApplications().get(containerId.appID);
        List<Path> appDirs = new ArrayList<Path>(localDirs.size());
        for (Path p : localDirs) {
          Path usersdir = new Path(p, ApplicationLocalizer.USERCACHE);
          Path userdir = new Path(usersdir,
              container.getLaunchContext().user.toString());
          Path appsdir = new Path(userdir, ApplicationLocalizer.APPCACHE);
          appDirs.add(new Path(appsdir, app.toString()));
        }
        Path appSysDir = new Path(sysDirs.get(0), app.toString());
        // TODO set in Application
        //Path appLogDir = new Path(logDirs.get(0), app.toString());
        ContainerLaunch launch =
          new ContainerLaunch(dispatcher, exec, app,
              event.getContainer(), appSysDir, appDirs);
        running.put(containerId, containerLauncher.submit(launch));
        break;
      case CLEANUP_CONTAINER:
        Future<Integer> rContainer = running.remove(containerId);
        if (rContainer != null) {
          // TODO needs to kill the container
          rContainer.cancel(false);
        }
      dispatcher.getEventHandler().handle(
          new ContainerEvent(containerId,
              ContainerEventType.CONTAINER_CLEANEDUP_AFTER_KILL));
        break;
    }
  }

}
