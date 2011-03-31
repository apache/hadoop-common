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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.PRIVATE;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.PUBLIC;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;

/**
 * A thread that takes care of localization of a single application. This is
 * used by {@link ResourceLocalizationService} to start one thread per app.
 * 
 */
public class AppLocalizationRunnerImpl implements AppLocalizationRunner {

  private static final Log LOG =
    LogFactory.getLog(AppLocalizationRunnerImpl.class);

  final Path sysDir;
  final Path logDir;
  final FileContext lfs;
  final List<Path> localDirs;
  final Application app;
  final Dispatcher dispatcher;
  final ContainerExecutor exec;
  final LocalizationProtocol localization;
  final LocalResourcesTracker publicRsrcsTracker;
  final LocalResourcesTracker privateRsrsTracker;
  final BlockingQueue<Future<Map<LocalResource,Path>>> localizedResources;

  public AppLocalizationRunnerImpl(Dispatcher dispatcher, Application app,
      LocalizationProtocol localization,
      LocalResourcesTracker publicResources,
      LocalResourcesTracker privateResources, ContainerExecutor exec,
      Path logDir,
      List<Path> localDirs, Path sysDir) throws IOException {
    // TODO
    this(dispatcher, app, localization, publicResources, privateResources, exec,
        logDir, localDirs, sysDir, FileContext.getLocalFSFileContext(),
        new LinkedBlockingQueue<Future<Map<LocalResource,Path>>>());
  }

  AppLocalizationRunnerImpl(Dispatcher dispatcher, Application app,
      LocalizationProtocol localization,
      LocalResourcesTracker publicRsrcsTracker,
      LocalResourcesTracker privateRsrcsTracker, ContainerExecutor exec,
      Path logDir, List<Path> localDirs, Path sysDir, FileContext lfs,
      BlockingQueue<Future<Map<LocalResource, Path>>> localizedResources) {
    this.app = app;
    this.lfs = lfs;
    this.exec = exec;
    this.logDir = logDir;
    this.sysDir = sysDir;
    this.localDirs = localDirs;
    this.dispatcher = dispatcher;
    this.localization = localization;
    this.publicRsrcsTracker = publicRsrcsTracker;
    this.privateRsrsTracker = privateRsrcsTracker;
    this.localizedResources = localizedResources;
    LOG.info("Initializing " + app + " for " + app.getUser());
  }

  public void localizedResource(Future<Map<LocalResource,Path>> result) {
    localizedResources.offer(result);
  }

  private Map<LocalResource,String> invertMap(
        Map<String,org.apache.hadoop.yarn.api.records.LocalResource> yrsrc)
      throws URISyntaxException {
    Map<LocalResource,String> ret = new HashMap<LocalResource,String>();
    for (Map.Entry<String,org.apache.hadoop.yarn.api.records.LocalResource> y : yrsrc.entrySet()) {
      ret.put(new LocalResource(y.getValue()), y.getKey());
    }
    return ret;
  }

  void abort() {
    // TODO
  }

  private void writeApplicationLocalizationControlFiles(
      Collection<org.apache.hadoop.yarn.api.records.LocalResource> todo) throws IOException {
    DataOutputStream filesOut = null;
    DataOutputStream tokenOut = null;
    try {
      lfs.mkdir(sysDir, null, false);
      Path appFiles = new Path(sysDir, ApplicationLocalizer.FILECACHE_FILE);
      filesOut = lfs.create(appFiles, EnumSet.of(CREATE, OVERWRITE));
      ApplicationLocalizer.writeResourceDescription(filesOut, todo);
      Path appTokens = new Path(sysDir, ApplicationLocalizer.APPTOKEN_FILE);
      tokenOut = lfs.create(appTokens, EnumSet.of(CREATE, OVERWRITE));
      Credentials appCreds = app.getCredentials();
      LOG.info("Writing credentials again to " + appTokens.toString()
          + ". Credentials list: ");
      for (Token<? extends TokenIdentifier> tk : appCreds.getAllTokens()) {
        LOG.info(tk.getService() + " : " + tk.encodeToUrlString());
      }
      appCreds.writeTokenStorageToStream(tokenOut);
    } finally {
      IOUtils.cleanup(null, filesOut, tokenOut);
    }
  }

  @Override
  public void run() {
    // 0) queue public cache
    Map<String,org.apache.hadoop.yarn.api.records.LocalResource> pub = app.getResources(PUBLIC);
    // 1) wait for completion
    // 1.1) if any failures, do *not* cancel remaining (other jobs may req) but
    //      decr reference counts
    // 3) queue private cache
    // XXX no need to copy w/ working public cache
    Map<String,org.apache.hadoop.yarn.api.records.LocalResource> priv =
      new HashMap(app.getResources(PRIVATE));

    Path workdir = null;
    Map<Path,String> links = new HashMap<Path,String>();
    // TODO avoid sync and wait for all rsrc. This is a high impacting bug.
    synchronized (privateRsrsTracker) {
    Collection<org.apache.hadoop.yarn.api.records.LocalResource> todo;
    try {
      // TODO public rsrc separate
      priv.putAll(pub);
      todo = privateRsrsTracker.register(this, priv.values());
    } catch (URISyntaxException e) {
      // TODO cancel application
      LOG.warn("Bad resource", e);
      return;
    }
    try {

      // TODO: launch process only if todo is non-empty?

      writeApplicationLocalizationControlFiles(todo);
      // 4) start applicationInit via ContainerExecutor
      // 4.1) applicationInit incl application-specific resources, e.g. job.xml
      // TODO
      try {
        // TODO token location should not rely on this convention
        workdir = new Path(localDirs.get(0),
            new Path(ApplicationLocalizer.USERCACHE,
              new Path(app.getUser(),
                new Path(ApplicationLocalizer.APPCACHE, app.toString()))));
        exec.initApplication(sysDir, localization, app.getUser(),
            app.toString(), logDir, localDirs);
        //links.put(new Path(workdir, ApplicationLocalizer.JOBTOKEN_FILE),
        //          ApplicationLocalizer.JOBTOKEN_FILE);
      } catch (IOException e) {
        // TODO cleanup
        LOG.warn("Failed to init application resources", e);
      } catch (InterruptedException e) {
        // TODO cleanup
        LOG.warn("Failed to init application resources", e);
      }
      // 5) Compile rsrc resolution for this job
      Map<LocalResource,String> requested;
      requested = invertMap(priv);
      //for (Future<Map<LocalResource,Path>> rsrc = localizedResources.poll();
      //     rsrc != null; rsrc = localizedResources.poll()) {
      for (int i = 0; i < priv.size(); ++i) {
        Future<Map<LocalResource,Path>> rsrc = localizedResources.poll();
          if (rsrc != null) {
            // TODO: Happens if no priv resources and only app resources?
            Map<LocalResource, Path> resolved = rsrc.get();
            for (Map.Entry<LocalResource, Path> r : resolved.entrySet()) {
              links.put(r.getValue(), requested.get(r.getKey()));
            }
          }
      }
    } catch (Throwable e) {
      // notify/kill containers
      LOG.info("Initialization of " + app + " failed: ", e);
      dispatcher.getEventHandler().handle(new ApplicationEvent(
            app.getAppId(), ApplicationEventType.FINISH_APPLICATION));
      return;
    }
    }
    LOG.info("Initialization of " + app + " complete");
    // 6) signal completion of Application init to Application, which will
    //    signal any waiting containers to start
    dispatcher.getEventHandler().handle(new ApplicationInitedEvent(
          app.getAppId(), links, workdir));
  }

}
