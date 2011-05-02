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

import java.io.DataOutputStream;

import java.net.URISyntaxException;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOCALIZER_BIND_ADDRESS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOG_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCALIZER_BIND_ADDRESS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOG_DIR;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerSecurityInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ResourceLocalizationService extends AbstractService
    implements EventHandler<LocalizationEvent>, LocalizationProtocol {

  private static final Log LOG = LogFactory.getLog(ResourceLocalizationService.class);
  public static final String NM_PRIVATE_DIR = "nmPrivate";
  public static final FsPermission NM_PRIVATE_PERM = new FsPermission((short) 0700);

  private Server server;
  private InetSocketAddress locAddr;
  private List<Path> logDirs;
  private List<Path> localDirs;
  private List<Path> sysDirs;
  private final ContainerExecutor exec;
  protected final Dispatcher dispatcher;
  private final DeletionService delService;
  private LocalizerTracker localizers;
  private final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  //private final LocalResourcesTracker publicRsrc;
  private final ConcurrentMap<String,LocalResourcesTracker> privateRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();
  private final ConcurrentMap<String,LocalResourcesTracker> appRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();

  public ResourceLocalizationService(Dispatcher dispatcher,
      ContainerExecutor exec, DeletionService delService) {
    super(ResourceLocalizationService.class.getName());
    this.exec = exec;
    this.dispatcher = dispatcher;
    this.delService = delService;
  }

  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnException("Failed to access local fs");
    }
  }

  @Override
  public void init(Configuration conf) {
    try {
      // TODO queue deletions here, rather than NM init?
      FileContext lfs = getLocalFileContext(conf);
      String[] sLocalDirs =
        conf.getStrings(NM_LOCAL_DIR, DEFAULT_NM_LOCAL_DIR);

      localDirs = new ArrayList<Path>(sLocalDirs.length);
      logDirs = new ArrayList<Path>(sLocalDirs.length);
      sysDirs = new ArrayList<Path>(sLocalDirs.length);
      for (String sLocaldir : sLocalDirs) {
        Path localdir = new Path(sLocaldir);
        localDirs.add(localdir);
        // $local/usercache
        Path userdir = new Path(localdir, ContainerLocalizer.USERCACHE);
        lfs.mkdir(userdir, null, true);
        // $local/filecache
        Path filedir = new Path(localdir, ContainerLocalizer.FILECACHE);
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
      throw new YarnException("Failed to initialize LocalizationService", e);
    }
    localDirs = Collections.unmodifiableList(localDirs);
    logDirs = Collections.unmodifiableList(logDirs);
    sysDirs = Collections.unmodifiableList(sysDirs);
    locAddr = NetUtils.createSocketAddr(
      conf.get(NM_LOCALIZER_BIND_ADDRESS, DEFAULT_NM_LOCALIZER_BIND_ADDRESS));
    localizers = new LocalizerTracker();
    dispatcher.register(LocalizerEventType.class, localizers);
    super.init(conf);
  }

  @Override
  public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status) {
    return localizers.processHeartbeat(status);
  }

  @Override
  public void start() {
    server = createServer();
    LOG.info("Localizer started on port " + server.getPort());
    server.start();
    super.start();
  }

  Server createServer() {
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration conf = new Configuration(getConfig()); // Clone to separate
                                                         // sec-info classes
    LocalizerTokenSecretManager secretManager = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
          LocalizerSecurityInfo.class, SecurityInfo.class);
      secretManager = new LocalizerTokenSecretManager();
    }
    return rpc.getServer(
        LocalizationProtocol.class, this, locAddr, conf, secretManager);
  }

  @Override
  public void stop() {
    if (server != null) {
      server.close();
    }
    if (localizers != null) {
      localizers.stop();
    }
    super.stop();
  }

  @Override
  @SuppressWarnings("unchecked") // dispatcher not typed
  public void handle(LocalizationEvent event) {
    String userName;
    String appIDStr;
    // TODO: create log dir as $logdir/$user/$appId
    switch (event.getType()) {
    case INIT_APPLICATION_RESOURCES:
      Application app =
        ((ApplicationLocalizationEvent)event).getApplication();
      // 0) Create application tracking structs
      privateRsrc.putIfAbsent(app.getUser(),
          new LocalResourcesTrackerImpl(dispatcher));
      if (null != appRsrc.putIfAbsent(ConverterUtils.toString(app.getAppId()),
          new LocalResourcesTrackerImpl(dispatcher))) {
        LOG.warn("Initializing application " + app + " already present");
        assert false;
      }
      // 1) Signal container init
      dispatcher.getEventHandler().handle(new ApplicationInitedEvent(
            app.getAppId(), logDirs.get(0)));
      break;
    case INIT_CONTAINER_RESOURCES:
      ContainerLocalizationRequestEvent rsrcReqs =
        (ContainerLocalizationRequestEvent) event;
      Container c = rsrcReqs.getContainer();
      LocalizerContext ctxt = new LocalizerContext(
          c.getUser(), c.getContainerID(), c.getCredentials());
      final LocalResourcesTracker tracker;
      LocalResourceVisibility vis = rsrcReqs.getVisibility();
      switch (vis) {
      default:
      case PUBLIC:
        // TODO
        tracker = null;
        break;
      case PRIVATE:
        tracker = privateRsrc.get(c.getUser());
        break;
      case APPLICATION:
        tracker =
          appRsrc.get(ConverterUtils.toString(c.getContainerID().getAppId()));
        break;
      }
      for (LocalResourceRequest req : rsrcReqs.getRequestedResources()) {
        tracker.handle(new ResourceRequestEvent(req, vis, ctxt));
      }
      break;
    case CLEANUP_CONTAINER_RESOURCES:
      Container container =
        ((ContainerLocalizationEvent)event).getContainer();

      // Delete the container directories
      userName = container.getUser();;
      String containerIDStr = container.toString();
      appIDStr =
        ConverterUtils.toString(container.getContainerID().getAppId());
      for (Path localDir : localDirs) {
        Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
        Path userdir =
            new Path(usersdir, userName);
        Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
        Path appDir = new Path(allAppsdir, appIDStr);
        Path containerDir =
            new Path(appDir, containerIDStr);
        delService.delete(userName, containerDir, null);

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
          ((ApplicationLocalizationEvent) event).getApplication();
      if (null == appRsrc.remove(application)) {
        LOG.warn("Removing uninitialized application " + application);
      }

      // Delete the application directories
      userName = application.getUser();
      appIDStr = application.toString();
      for (Path localDir : localDirs) {
        Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
        Path userdir =
            new Path(usersdir, userName);
        Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
        Path appDir = new Path(allAppsdir, appIDStr);
        delService.delete(userName, appDir, null);

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

  class LocalizerTracker implements EventHandler<LocalizerEvent> {

    private final Map<String,LocalizerRunner> trackers;

    LocalizerTracker() {
      this(new HashMap<String,LocalizerRunner>());
    }

    LocalizerTracker(Map<String,LocalizerRunner> trackers) {
      this.trackers = trackers;
    }

    public LocalizerHeartbeatResponse processHeartbeat(LocalizerStatus status) {
      String locId = status.getLocalizerId();
      synchronized (trackers) {
        LocalizerRunner localizer = trackers.get(locId);
        if (null == localizer) {
          // TODO process resources anyway
          LocalizerHeartbeatResponse response =
            recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
          response.setLocalizerAction(LocalizerAction.DIE);
          return response;
        }
        return localizer.update(status.getResources());
      }
    }

    public void stop() {
      for (LocalizerRunner localizer : trackers.values()) {
        localizer.interrupt();
      }
    }

    @Override
    public void handle(LocalizerEvent event) {
      synchronized (trackers) {
        String locId = event.getLocalizerId();
        LocalizerRunner localizer = trackers.get(locId);
        switch(event.getType()) {
          case REQUEST_RESOURCE_LOCALIZATION:
            // 0) find running localizer or start new thread
            LocalizerResourceRequestEvent req =
              (LocalizerResourceRequestEvent)event;
            if (null == localizer) {
              LOG.info("Created localizer for " + req.getLocalizerId());
              // TODO: ROUND_ROBIN below.
              localizer = new LocalizerRunner(req.getContext(),
                  sysDirs.get(0), req.getLocalizerId(), logDirs.get(0));
              trackers.put(locId, localizer);
              localizer.start();
            }
            // 1) propagate event
            localizer.addResource(req);
            break;
          case ABORT_LOCALIZATION:
            // 0) find running localizer, interrupt and remove
            if (null == localizer) {
              return; // ignore; already gone
            }
            trackers.remove(locId);
            localizer.interrupt();
            break;
        }
      }
    }
  }

  class LocalizerRunner extends Thread {

    final LocalizerContext context;
    final String localizerId;
    final Path nmPrivate;
    final Path rootLogDir;
    final Map<LocalResourceRequest,LocalizerResourceRequestEvent> scheduled;
    final List<LocalizerResourceRequestEvent> pending;

    private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

    LocalizerRunner(LocalizerContext context, Path nmPrivate,
        String localizerId, Path logDir) {
      this(context, nmPrivate, localizerId, logDir,
          new ArrayList<LocalizerResourceRequestEvent>(),
          new HashMap<LocalResourceRequest,LocalizerResourceRequestEvent>());
    }

    LocalizerRunner(LocalizerContext context, Path nmPrivate,
        String localizerId, Path logDir,
        List<LocalizerResourceRequestEvent> pending,
        Map<LocalResourceRequest,LocalizerResourceRequestEvent> scheduled) {
      this.nmPrivate = nmPrivate;
      this.context = context;
      this.localizerId = localizerId;
      this.rootLogDir = logDir;
      this.pending = pending;
      this.scheduled = scheduled;
    }

    public void addResource(LocalizerResourceRequestEvent request) {
      pending.add(request);
    }

    LocalResource findNextResource() {
      for (Iterator<LocalizerResourceRequestEvent> i = pending.iterator();
           i.hasNext();) {
        LocalizerResourceRequestEvent evt = i.next();
        LocalizedResource nRsrc = evt.getResource();
        if (ResourceState.LOCALIZED.equals(nRsrc.getState())) {
          i.remove();
          continue;
        }
        if (nRsrc.tryAcquire()) {
          LocalResourceRequest nextRsrc = nRsrc.getRequest();
          LocalResource next =
            recordFactory.newRecordInstance(LocalResource.class);
          next.setResource(
              ConverterUtils.getYarnUrlFromPath(nextRsrc.getPath()));
          next.setTimestamp(nextRsrc.getTimestamp());
          next.setType(nextRsrc.getType());
          next.setVisibility(evt.getVisibility());
          scheduled.put(nextRsrc, evt);
          return next;
        }
      }
      return null;
    }

    // TODO this sucks. Fix it later
    LocalizerHeartbeatResponse update(
        List<LocalResourceStatus> stats) {
      LocalizerHeartbeatResponse response =
        recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);

      if (stats.isEmpty()) {
        LocalResource next = findNextResource();
        if (next != null) {
          response.setLocalizerAction(LocalizerAction.LIVE);
          response.addResource(next);
        } else if (pending.isEmpty()) {
          response.setLocalizerAction(LocalizerAction.DIE);
        } else {
          response.setLocalizerAction(LocalizerAction.LIVE);
        }
        return response;
      }

      for (LocalResourceStatus stat : stats) {
        LocalResource rsrc = stat.getResource();
        LocalResourceRequest req = null;
        try {
          req = new LocalResourceRequest(rsrc);
        } catch (URISyntaxException e) {
          // TODO fail? Already translated several times...
        }
        LocalizerResourceRequestEvent assoc = scheduled.get(req);
        if (assoc == null) {
          // internal error
          LOG.error("Unknown resource reported: " + req);
          continue;
        }
        switch (stat.getStatus()) {
          case FETCH_SUCCESS:
            // notify resource
            try {
              assoc.getResource().handle(
                  new ResourceLocalizedEvent(req,
                    ConverterUtils.getPathFromYarnURL(stat.getLocalPath()),
                    stat.getLocalSize()));
            } catch (URISyntaxException e) { }
            if (pending.isEmpty()) {
              response.setLocalizerAction(LocalizerAction.DIE);
              break;
            }
            response.setLocalizerAction(LocalizerAction.LIVE);
            LocalResource next = findNextResource();
            if (next != null) {
              response.addResource(next);
            }
            break;
          case FETCH_PENDING:
            response.setLocalizerAction(LocalizerAction.LIVE);
            break;
          case FETCH_FAILURE:
            LOG.info("DEBUG: FAILED " + req, stat.getException());
            assoc.getResource().unlock();
            response.setLocalizerAction(LocalizerAction.DIE);
            dispatcher.getEventHandler().handle(
                new ContainerResourceFailedEvent(context.getContainerId(),
                  req, stat.getException()));
            break;
          default:
            LOG.info("Unknown status: " + stat.getStatus());
            response.setLocalizerAction(LocalizerAction.DIE);
            dispatcher.getEventHandler().handle(
                new ContainerResourceFailedEvent(context.getContainerId(),
                  req, stat.getException()));
            break;
        }
      }
      return response;
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      try {
        // 0) init queue, etc.
        // 1) write credentials to private dir
        DataOutputStream tokenOut = null;
        try {
          Credentials credentials = context.getCredentials();
          Path cTokens = new Path(nmPrivate, String.format(
                ContainerLocalizer.TOKEN_FILE_FMT, localizerId));
          FileContext lfs = getLocalFileContext(getConfig());
          tokenOut = lfs.create(cTokens, EnumSet.of(CREATE, OVERWRITE));
          LOG.info("Writing credentials to the nmPrivate file "
              + cTokens.toString() + ". Credentials list: ");
          for (Token<? extends TokenIdentifier> tk :
              credentials.getAllTokens()) {
            LOG.info(tk.getService() + " : " + tk.encodeToUrlString());
          }
          credentials.writeTokenStorageToStream(tokenOut);
        } finally {
          if (tokenOut != null) {
            tokenOut.close();
          }
        }
        // 2) exec initApplication and wait
        exec.startLocalizer(nmPrivate, locAddr, context.getUser(),
            ConverterUtils.toString(context.getContainerId().getAppId()),
            localizerId, rootLogDir, localDirs);
      } catch (Exception e) {
        // 3) on error, report failure to Container and signal ABORT
        // 3.1) notify resource of failed localization
        for (LocalizerResourceRequestEvent event : scheduled.values()) {
          event.getResource().unlock();
        }
        //dispatcher.getEventHandler().handle(
        //    new ContainerResourceFailedEvent(current.getContainer(),
        //      current.getResource().getRequest(), e));
      }
    }

  }
}
