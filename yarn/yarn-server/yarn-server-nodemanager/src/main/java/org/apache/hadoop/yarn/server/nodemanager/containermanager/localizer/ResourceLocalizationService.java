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

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOCALIZER_BIND_ADDRESS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOG_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCALIZER_BIND_ADDRESS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOG_DIR;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.LocalizationProtocol;
import org.apache.hadoop.yarn.URL;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnRemoteException;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerSecurityInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.AvroUtil;

public class ResourceLocalizationService extends AbstractService
    implements EventHandler<LocalizerEvent>, LocalizationProtocol {

  private static final Log LOG = LogFactory.getLog(ResourceLocalizationService.class);
  public static final String NM_PRIVATE_DIR = "nmPrivate";
  public final FsPermission NM_PRIVATE_PERM = new FsPermission((short) 0700);

  private Server server;
  private InetSocketAddress locAddr;
  private List<Path> logDirs;
  private List<Path> localDirs;
  private List<Path> sysDirs;
  private final ContainerExecutor exec;
  protected final Dispatcher dispatcher;
  private final DeletionService delService;
  private final ExecutorService appLocalizerThreadPool =
    Executors.newCachedThreadPool();

  /**
   * Map of private resources of users.
   */
  private final ConcurrentMap<String,LocalResourcesTracker> privateRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();

  /**
   * Map of applications that are in the process of localization.
   * TODO: Why is it needed?
   */
  private final ConcurrentMap<String,AppLocalizationRunner> localizingApps =
    new ConcurrentHashMap<String,AppLocalizationRunner>();

  public ResourceLocalizationService(Dispatcher dispatcher,
      ContainerExecutor exec, DeletionService delService) {
    super(ResourceLocalizationService.class.getName());
    this.exec = exec;
    this.dispatcher = dispatcher;
    this.delService = delService;
  }

  @Override
  public void init(Configuration conf) {
    // TODO limit for public cache, not appLocalizer
    //appLocalizer.setMaximumPoolSize(
    //    conf.getInt(NM_MAX_PUBLIC_FETCH_THREADS,
    //                DEFAULT_MAX_PUBLIC_FETCH_THREADS));
    try {
      // TODO queue deletions here, rather than NM init?
      FileContext lfs = FileContext.getLocalFSFileContext(conf);
      String[] sLocalDirs = conf.getStrings(NM_LOCAL_DIR, DEFAULT_NM_LOCAL_DIR);

      localDirs = new ArrayList<Path>(sLocalDirs.length);
      logDirs = new ArrayList<Path>(sLocalDirs.length);
      sysDirs = new ArrayList<Path>(sLocalDirs.length);
      for (String sLocaldir : sLocalDirs) {
        Path localdir = new Path(sLocaldir);
        localDirs.add(localdir);
        // $local/usercache
        Path userdir = new Path(localdir, ApplicationLocalizer.USERCACHE);
        lfs.mkdir(userdir, null, true);
        // $local/filecache
        Path filedir = new Path(localdir, ApplicationLocalizer.FILECACHE);
        lfs.mkdir(filedir, null, true);
        // $local/nmPrivate
        Path sysdir = new Path(localdir, NM_PRIVATE_DIR);
        lfs.mkdir(sysdir, NM_PRIVATE_PERM, true);
        sysDirs.add(sysdir);
      }
      String[] sLogdirs = conf.getStrings(NM_LOG_DIR, DEFAULT_NM_LOG_DIR);
      for (String sLogdir : sLogdirs) {
        Path logdir = new Path(sLogdir);
        logDirs.add(logdir);
        lfs.mkdir(logdir, null, true);
      }
    } catch (IOException e) {
      throw new YarnException("Failed to start Localizer", e);
    }
    localDirs = Collections.unmodifiableList(localDirs);
    logDirs = Collections.unmodifiableList(logDirs);
    sysDirs = Collections.unmodifiableList(sysDirs);
    locAddr = NetUtils.createSocketAddr(
        conf.get(NM_LOCALIZER_BIND_ADDRESS, DEFAULT_NM_LOCALIZER_BIND_ADDRESS));
    super.init(conf);
  }

  @Override
  public void start() {
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration conf = new Configuration(getConfig()); // Clone to separate
                                                         // sec-info classes
    LocalizerTokenSecretManager secretManager = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
          LocalizerSecurityInfo.class, SecurityInfo.class);
      secretManager = new LocalizerTokenSecretManager();
    }
    server =
        rpc.getServer(LocalizationProtocol.class, this, locAddr, conf,
            secretManager);
    LOG.info("Localizer started at " + locAddr);
    server.start();
    super.start();
  }

  /** LTC communication w/ Localizer */
  public InetSocketAddress getAddress() {
    return locAddr;
  }

  /**
   * Localizer report to NodeManager of localized resource.
   * @param user Owner of the private cache
   * @param resource Resource localized
   * @param path Location on the local filesystem, or null if failed
   */
  @Override
  public Void successfulLocalization(CharSequence user,
      org.apache.hadoop.yarn.LocalResource resource, URL path)
      throws YarnRemoteException {
    // TODO validate request
    LocalResourcesTracker userCache = privateRsrc.get(user.toString());
    if (null == userCache) {
      throw RPCUtil.getRemoteException("Unknown user: " + user);
    }
    try {
      userCache.setSuccess(new LocalResource(resource),
          resource.size, AvroUtil.getPathFromYarnURL(path));
    } catch (Exception e) {
      throw RPCUtil.getRemoteException(e);
    }
    return null;
  }

  @Override
  public Void failedLocalization(CharSequence user,
      org.apache.hadoop.yarn.LocalResource resource, YarnRemoteException cause) 
      throws YarnRemoteException {
    LocalResourcesTracker userCache = privateRsrc.get(user.toString());
    if (null == userCache) {
      throw RPCUtil.getRemoteException("Unknown user: " + user);
    }
    try {
      userCache.removeFailedResource(new LocalResource(resource), cause);
    } catch (Exception e) {
      throw RPCUtil.getRemoteException(e);
    }
    return null;
  }

  @Override
  public void stop() {
    appLocalizerThreadPool.shutdownNow();
    if (server != null) {
      server.close();
    }
    super.stop();
  }

  @Override
  public void handle(LocalizerEvent event) {
    String userName;
    String appIDStr;
    
    switch (event.getType()) {
    case INIT_APPLICATION_RESOURCES:
      Application app = ((ApplicationLocalizerEvent)event).getApplication();
      String user = app.getUser();
      LocalResourcesTracker rsrcTracker = privateRsrc.get(user);
      if (null == rsrcTracker) {
        LocalResourcesTracker perUserPrivateRsrcTracker =
            new LocalResourcesTrackerImpl();
        rsrcTracker =
            privateRsrc.putIfAbsent(user, perUserPrivateRsrcTracker);
        if (null == rsrcTracker) {
          rsrcTracker = perUserPrivateRsrcTracker;
        }
      }

      // Possibility of duplicate app localization is avoided by ApplicationImpl
      // itself, so we are good to go.

      // TODO: use LDA for picking single logdir, sysDir
      // TODO: create log dir as $logdir/$user/$appId
      try {
        AppLocalizationRunner appLocalizationRunner =
          new AppLocalizationRunnerImpl(dispatcher,
              app, this, null, rsrcTracker, exec, logDirs.get(0), localDirs,
              new Path(sysDirs.get(0), app.toString()));
        localizingApps.put(app.toString(), appLocalizationRunner);
        appLocalizerThreadPool.submit(appLocalizationRunner);
      } catch (IOException e) {
        // TODO kill containers
        LOG.info("Failed to submit application", e);
        //dispatcher.getEventHandler().handle(
      }
      break;
    case CLEANUP_CONTAINER_RESOURCES:
      Container container = ((ContainerLocalizerEvent)event).getContainer();

      // Delete the container directories
      userName = container.getUser();;
      String containerIDStr = container.toString();
      appIDStr = AvroUtil.toString(container.getContainerID().appID);
      for (Path localDir : localDirs) {
        Path usersdir = new Path(localDir, ApplicationLocalizer.USERCACHE);
        Path userdir =
            new Path(usersdir, userName);
        Path allAppsdir = new Path(userdir, ApplicationLocalizer.APPCACHE);
        Path appDir = new Path(allAppsdir, appIDStr);
        Path containerDir =
            new Path(appDir, containerIDStr);
        delService.delete(userName,
            containerDir, null);

        Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
        Path appSysDir = new Path(sysDir, appIDStr);
        Path containerSysDir = new Path(appSysDir, containerIDStr);
        delService.delete(null, containerSysDir, null);
      }

      dispatcher.getEventHandler().handle(new ContainerEvent(
            container.getContainerID(),
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
      break;
    case DESTROY_APPLICATION_RESOURCES:

      Application application =
          ((ApplicationLocalizerEvent) event).getApplication();

      // Delete the application directories
      userName = application.getUser();
      appIDStr = application.toString();
      for (Path localDir : localDirs) {
        Path usersdir = new Path(localDir, ApplicationLocalizer.USERCACHE);
        Path userdir =
            new Path(usersdir, userName);
        Path allAppsdir = new Path(userdir, ApplicationLocalizer.APPCACHE);
        Path appDir = new Path(allAppsdir, appIDStr);
        delService.delete(userName,
            appDir, null);

        Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
        Path appSysDir = new Path(sysDir, appIDStr);
        delService.delete(null, appSysDir, null);
      }

      // TODO: decrement reference counts of all resources associated with this
      // app

      dispatcher.getEventHandler().handle(new ApplicationEvent(
            application.getAppId(),
            ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
      break;
    }
  }

}
