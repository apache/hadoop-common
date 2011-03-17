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

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ApplicationLocalizer;
import org.apache.hadoop.yarn.ContainerLaunchContext;

public class ContainerLaunch implements Callable<Integer> {

  private static final Log LOG = LogFactory.getLog(ContainerLaunch.class);

  public static final String CONTAINER_SCRIPT = "task.sh";

  private final Dispatcher dispatcher;
  private final ContainerExecutor exec;
  private final Application app;
  private final Container container;
  private final Path sysDir;
  private final List<Path> appDirs;

  public ContainerLaunch(Dispatcher dispatcher, ContainerExecutor exec,
      Application app, Container container, Path sysDir, List<Path> appDirs) {
    this.app = app;
    this.exec = exec;
    this.sysDir = sysDir;
    this.appDirs = appDirs;
    this.container = container;
    this.dispatcher = dispatcher;
  }

  @Override
  public Integer call() {
    final ContainerLaunchContext launchContext = container.getLaunchContext();
    final Map<Path,String> localizedResources = app.getLocalizedResources();
    final String user = launchContext.user.toString();
    final Map<CharSequence,CharSequence> env = launchContext.env;
    final List<CharSequence> command = launchContext.command;
    int ret = -1;
    try {
      FileContext lfs = FileContext.getLocalFSFileContext();
      Path launchSysDir = new Path(sysDir, container.toString());
      lfs.mkdir(launchSysDir, null, false);
      Path launchPath = new Path(launchSysDir, CONTAINER_SCRIPT);
      Path tokensPath =
        new Path(launchSysDir, ApplicationLocalizer.APPTOKEN_FILE);
      DataOutputStream launchOut = null;
      DataOutputStream tokensOut = null;
      
      try {
        launchOut = lfs.create(launchPath, EnumSet.of(CREATE, OVERWRITE));
        ApplicationLocalizer.writeLaunchEnv(launchOut, env, localizedResources,
            command, appDirs);
        
        tokensOut = lfs.create(tokensPath, EnumSet.of(CREATE, OVERWRITE));
        Credentials creds = new Credentials();
        if (container.getLaunchContext().containerTokens != null) {
          // TODO: Is the conditional the correct way of checking?
          DataInputByteBuffer buf = new DataInputByteBuffer();
          container.getLaunchContext().containerTokens.rewind();
          buf.reset(container.getLaunchContext().containerTokens);
          creds.readTokenStorageStream(buf);
          for (Token<? extends TokenIdentifier> tk : creds.getAllTokens()) {
            LOG.debug(tk.getService() + " = " + tk.toString());
          }
        }
        creds.writeTokenStorageToStream(tokensOut);
      } finally {
        IOUtils.cleanup(LOG, launchOut, tokensOut);
        if (launchOut != null) {
          launchOut.close();
        }
      }
      dispatcher.getEventHandler().handle(new ContainerEvent(
            container.getLaunchContext().id,
            ContainerEventType.CONTAINER_LAUNCHED));
      ret =
        exec.launchContainer(container, launchSysDir, user, app.toString(),
            appDirs, null, null);
      if (ret != 0) {
        throw new ExitCodeException(ret, "Container failed");
      }
    } catch (Throwable e) {
      LOG.warn("Failed to launch container", e);
      dispatcher.getEventHandler().handle(new ContainerEvent(
            launchContext.id,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE));
      return ret;
    }
    LOG.info("Container " + container + " succeeded " + launchContext.id);
    dispatcher.getEventHandler().handle(new ContainerEvent(
          launchContext.id, ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    return 0;
  }

}
