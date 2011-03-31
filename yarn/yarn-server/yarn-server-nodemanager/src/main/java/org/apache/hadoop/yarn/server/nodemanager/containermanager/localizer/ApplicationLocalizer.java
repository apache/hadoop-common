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

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCAL_DIR;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerSecurityInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;


/**
 * Internal class responsible for initializing the job, not intended for users.
 * Creates the following hierarchy:
 *   <li>$local.dir/usercache/$user</li>
 *   <li>$local.dir/usercache/$user/appcache</li>
 *   <li>$local.dir/usercache/$user/appcache/$appId/work</li>
 *   <li>$local.dir/usercache/$user/appcache/$appId/filecache</li>
 *   <li>$local.dir/usercache/$user/appcache/$appId/appToken</li>
 *   <li>$local.dir/usercache/$user/appcache/$appId/appFiles</li>
 *   <li>$local.dir/usercache/$user/filecache</li>
 */
public class ApplicationLocalizer {

  static final Log LOG = LogFactory.getLog(ApplicationLocalizer.class);

  public static final String FILECACHE = "filecache";
  public static final String FILECACHE_FILE = "appFiles";
  public static final String APPCACHE = "appcache";
  public static final String USERCACHE = "usercache";
  public static final String APPTOKEN_FILE = "appTokens";
  public static final String WORKDIR = "work";

  private final String user;
  private final String appId;
  private final Path logDir;
  private final FileContext lfs;
  private final Configuration conf;
  private final List<Path> localDirs;
  private final LocalDirAllocator lDirAlloc;
  private final List<org.apache.hadoop.yarn.api.records.LocalResource> privateResources;
  private final List<org.apache.hadoop.yarn.api.records.LocalResource> applicationResources;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  public ApplicationLocalizer(String user, String appId, Path logDir,
      List<Path> localDirs) throws IOException {
    this(FileContext.getLocalFSFileContext(), user, appId, logDir, localDirs);
  }

  public ApplicationLocalizer(FileContext lfs, String user, String appId,
      Path logDir, List<Path> localDirs) throws IOException {
    if (null == user) {
      throw new IOException("Cannot initialize for null user");
    }
    if (null == appId) {
      throw new IOException("Cannot initialize for null appId");
    }
    this.user = user;
    this.appId = appId;
    this.logDir = logDir;
    this.lfs = lfs;
    // TODO fix bug in FileContext requiring Configuration for local fs
    this.conf = new Configuration();
    this.localDirs = setLocalDirs(user, conf, localDirs);
    lDirAlloc = new LocalDirAllocator(NM_LOCAL_DIR);
    privateResources = new ArrayList<org.apache.hadoop.yarn.api.records.LocalResource>();
    applicationResources = new ArrayList<org.apache.hadoop.yarn.api.records.LocalResource>();
  }

  public static void writeLaunchEnv(OutputStream out,
      Map<String,String> environment, Map<Path,String> resources,
      List<String> command, List<Path> appDirs)
      throws IOException {
    ShellScriptBuilder sb = new ShellScriptBuilder();
    if (System.getenv("YARN_HOME") != null) {
      sb.env("YARN_HOME", System.getenv("YARN_HOME"));
    }
    sb.env(YARNApplicationConstants.LOCAL_DIR_ENV,
        StringUtils.join(",", appDirs));
    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        sb.env(env.getKey().toString(), env.getValue().toString());
      }
    }
    if (resources != null) {
      for (Map.Entry<Path,String> link : resources.entrySet()) {
        sb.symlink(link.getKey(), link.getValue());
      }
    }
    ArrayList<String> cmd = new ArrayList<String>(2 * command.size() + 5);
    cmd.add(ContainerExecutor.isSetsidAvailable ? "exec setsid " : "exec ");
    cmd.add("/bin/bash ");
    cmd.add("-c ");
    cmd.add("\"");
    for (String cs : command) {
      cmd.add(cs.toString());
      cmd.add(" ");
    }
    cmd.add("\"");
    sb.line(cmd.toArray(new String[cmd.size()]));
    PrintStream pout = null;
    try {
      pout = new PrintStream(out);
      sb.write(pout);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  static void writeResourceDescription(OutputStream out,
      Collection<org.apache.hadoop.yarn.api.records.LocalResource> rsrc) throws IOException {
    try {
      for (org.apache.hadoop.yarn.api.records.LocalResource r : rsrc) {
        LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) r;
        rsrcPb.getProto().writeDelimitedTo(out);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
  
  //TODO PB This part becomes dependent on the PB implementation.
  //Add an interface which makes this independent of the serialization layer being used.
  
//  static void writeResourceDescription(OutputStream out,
//      Collection<org.apache.hadoop.yarn.api.records.LocalResource> rsrc) throws IOException {
//    try {
//      BinaryEncoder encoder = new BinaryEncoder(out);
//      SpecificDatumWriter writer = new SpecificDatumWriter(org.apache.hadoop.yarn.api.records.LocalResource.class);
//      for (org.apache.hadoop.yarn.api.records.LocalResource r : rsrc) {
//        writer.write(r, encoder);
//      }
//    } finally {
//      if (out != null) {
//        out.close();
//      }
//    }
//  }

  private void readResourceDescription(InputStream in) throws IOException {
    while (in.available() != 0) {
      org.apache.hadoop.yarn.api.records.LocalResource rsrc = new LocalResourcePBImpl(LocalResourceProto.parseDelimitedFrom(in));
      switch (rsrc.getVisibility()) {
        case PRIVATE:
          privateResources.add(rsrc);
          break;
        default:
          privateResources.add(rsrc);
          break;
      }
    }
  }
  
  //TODO PB This part becomes dependent on the PB implementation.
  //Add an interface which makes this independent of the serialization layer being used.
//  private void readResourceDescription(InputStream in) throws IOException {
//    BinaryDecoder decoder =
//      DecoderFactory.defaultFactory().createBinaryDecoder(in, null);
//    SpecificDatumReader<org.apache.hadoop.yarn.api.records.LocalResource> reader =
//      new SpecificDatumReader<org.apache.hadoop.yarn.api.records.LocalResource>(org.apache.hadoop.yarn.api.records.LocalResource.class);
//    while (!decoder.isEnd()) {
//      org.apache.hadoop.yarn.api.records.LocalResource rsrc = reader.read(null, decoder);
//      switch (rsrc.getVisibility()) {
//        case PRIVATE:
//          privateResources.add(rsrc);
//          break;
//        // TODO: Commented to put everything in privateResources for now?
//        //case APPLICATION:
//        //  applicationResources.add(rsrc);
//        //  break;
//        default:
//          privateResources.add(rsrc);
//          break;
//      }
//    }
//  }

  private static List<Path> setLocalDirs(String user, Configuration conf,
      List<Path> localdirs) throws IOException {
    if (null == localdirs || 0 == localdirs.size()) {
      throw new IOException("Cannot initialize without local dirs");
    }
    String[] sLocaldirs = new String[localdirs.size()];
    final List<Path> ret = new ArrayList<Path>(sLocaldirs.length);
    for (int i = 0; i < sLocaldirs.length; ++i) {
      Path p = new Path(localdirs.get(i), new Path(USERCACHE, user));
      ret.add(p);
      sLocaldirs[i] = p.toString();
    }
    conf.setStrings(NM_LOCAL_DIR, sLocaldirs);
    return ret;
  }

  private String[] getSubdirs(String subdir) {
    String[] cacheDirs = new String[localDirs.size()];
    for (int i = 0, n = localDirs.size(); i < n; ++i) {
      Path cacheDir = new Path(localDirs.get(i), subdir);
      cacheDirs[i] = cacheDir.toString();
    }
    return cacheDirs;
  }

  FSDownload download(LocalDirAllocator lda, org.apache.hadoop.yarn.api.records.LocalResource rsrc)
      throws IOException {
    return new FSDownload(conf, lda, rsrc);
  }

  private void localizePrivateFiles(final LocalizationProtocol nodeManager)
      throws IOException, InterruptedException, YarnRemoteException {
    // setup the private distributed cache
    String cacheContext = appId + ".private.cache";
    conf.setStrings(cacheContext, getSubdirs(FILECACHE));
    // TODO: Why different context but same local-dirs?
    LocalDirAllocator privateLDA = new LocalDirAllocator(cacheContext);
    pull(privateLDA, privateResources, nodeManager);
  }

  private void localizeAppFiles(final LocalizationProtocol nodeManager)
      throws IOException, InterruptedException, YarnRemoteException {
    // TODO localize application-scope files, e.g. job.xml, job.jar
    String cacheContext = appId + ".cache";
    //Configuration cacheConf = new Configuration(false);
    conf.setStrings(cacheContext, getSubdirs(FILECACHE));
    // TODO: Why different context but same local-dirs?
    LocalDirAllocator applicationLDA = new LocalDirAllocator(cacheContext);
    pull(applicationLDA, applicationResources, nodeManager);
  }

  private void pull(LocalDirAllocator lda, Collection<org.apache.hadoop.yarn.api.records.LocalResource> resources,
      LocalizationProtocol nodeManager)
      throws IOException, InterruptedException, YarnRemoteException {
    ExecutorService exec = Executors.newSingleThreadExecutor();
    CompletionService<Map<org.apache.hadoop.yarn.api.records.LocalResource,Path>> queue =
      new ExecutorCompletionService<Map<org.apache.hadoop.yarn.api.records.LocalResource,Path>>(exec);
    Map<Future<Map<org.apache.hadoop.yarn.api.records.LocalResource,Path>>, org.apache.hadoop.yarn.api.records.LocalResource> pending =
      new HashMap<Future<Map<org.apache.hadoop.yarn.api.records.LocalResource,Path>>, org.apache.hadoop.yarn.api.records.LocalResource>();
    for (org.apache.hadoop.yarn.api.records.LocalResource rsrc : resources) {
      FSDownload dThread = download(lda, rsrc);
      pending.put(queue.submit(dThread), rsrc);
    }
    try {
      for (int i = 0, n = resources.size(); i < n; ++i) {
        Future<Map<org.apache.hadoop.yarn.api.records.LocalResource,Path>> result = queue.take();
        try {
          Map<org.apache.hadoop.yarn.api.records.LocalResource,Path> localized = result.get();
          for (Map.Entry<org.apache.hadoop.yarn.api.records.LocalResource,Path> local : result.get().entrySet()) {
            SuccessfulLocalizationRequest successfulLocRequest = recordFactory.newRecordInstance(SuccessfulLocalizationRequest.class);
            successfulLocRequest.setUser(user);
            successfulLocRequest.setResource(local.getKey());
            successfulLocRequest.setPath(ConverterUtils.getYarnUrlFromPath(local.getValue()));
            nodeManager.successfulLocalization(successfulLocRequest);
            pending.remove(result);
          }
        } catch (ExecutionException e) {
          // TODO: Shouldn't we continue localizing other paths?
          FailedLocalizationRequest failedLocRequest = recordFactory.newRecordInstance(FailedLocalizationRequest.class);
          failedLocRequest.setUser(user);
          failedLocRequest.setResource(pending.get(result));
          failedLocRequest.setException(RPCUtil.getRemoteException(e.getCause()));
          nodeManager.failedLocalization(failedLocRequest);
          throw new IOException("Failed to localize " +
                                pending.get(result), e);
        }
      }
    } finally {
      YarnRemoteException e = RPCUtil.getRemoteException("Localization failed");
      exec.shutdownNow();
      for (org.apache.hadoop.yarn.api.records.LocalResource rsrc : pending.values()) {
        try {
          FailedLocalizationRequest failedLocRequest = recordFactory.newRecordInstance(FailedLocalizationRequest.class);
          failedLocRequest.setUser(user);
          failedLocRequest.setResource(rsrc);
          failedLocRequest.setException(RPCUtil.getRemoteException(e));
          nodeManager.failedLocalization(failedLocRequest);
        } catch (YarnRemoteException error) {
          LOG.error("Failure cancelling localization", error);
        }
      }
    }
  }

  private void localizeFiles(LocalizationProtocol nodeManager)
      throws IOException, InterruptedException { 
    // load user credentials, configuration
    // ASSUME
    // let $x = $local.dir
    // forall $x, exists $x/$user
    // exists $x/$user/appcache/$appId/appFiles
    // exists $x/$user/appcache/$appId/appToken
    // exists $logdir/userlogs/$appId
    // TODO verify LTC
    //createUserCacheDirs()
    //createAppDirs()
    //createAppLogDir();
    InputStream in = null;
    try {
      in = lfs.open(new Path(FILECACHE_FILE));
      readResourceDescription(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
    localizePrivateFiles(nodeManager);
    localizeAppFiles(nodeManager);
  }

  LocalizationProtocol getProxy(final InetSocketAddress nmAddr) {
//    // TODO: Fix the following
//    UserGroupInformation remoteUser =
//        UserGroupInformation.createRemoteUser(user);
//    Token<LocalizerTokenIdentifier> token =
//        new Token<LocalizerTokenIdentifier>();
//    remoteUser.addToken(token);
//    return remoteUser.doAs(new PrivilegedAction<Localization>() {
//      @Override
//      public Localization run() {
        Configuration localizerConf = new Configuration();
        YarnRPC rpc = YarnRPC.create(localizerConf);
        if (UserGroupInformation.isSecurityEnabled()) {
          localizerConf.setClass(
              CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
              LocalizerSecurityInfo.class, SecurityInfo.class);
        }
        return (LocalizationProtocol)
          rpc.getProxy(LocalizationProtocol.class, nmAddr, localizerConf);
//      }
//    });
  }

  public int runLocalization(final InetSocketAddress nmAddr)
      throws IOException, InterruptedException {
    // Pull in user's tokens to complete setup
    final Credentials creds = new Credentials();
    DataInputStream credFile = null;
    try {
      // assume credentials in cwd
      credFile = lfs.open(new Path(APPTOKEN_FILE));
      creds.readTokenStorageStream(credFile);
    } finally  {
      if (credFile != null) {
        credFile.close();
      }
    }

//    UserGroupInformation ugiJob =
//        UserGroupInformation.createRemoteUser(localizer.getAppId());
//    // TODO
//    //Token<JobTokenIdentifier> jt = TokenCache.getJobToken(creds);
//    //jt.setService(new Text(
//    //      nmAddr.getAddress().getHostAddress() + ":" + nmAddr.getPort()));
//    //ugiJob.addToken(jt);
    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(user);
    LocalizerTokenSecretManager secretManager =
        new LocalizerTokenSecretManager();
    LocalizerTokenIdentifier id =
        secretManager.createIdentifier();
    Token<LocalizerTokenIdentifier> localizerToken =
        new Token<LocalizerTokenIdentifier>(id, secretManager);
//        new Token<LocalizerTokenIdentifier>("testuser".getBytes(),
//            new byte[0], LocalizerTokenIdentifier.KIND, new Text("testing"));
    remoteUser.addToken(localizerToken);
    final LocalizationProtocol nodeManager =
        remoteUser.doAs(new PrivilegedAction<LocalizationProtocol>() {
          @Override
          public LocalizationProtocol run() {
            return getProxy(nmAddr);
          }
        });

    UserGroupInformation ugi =
      UserGroupInformation.createRemoteUser(user);
    for (Token<? extends TokenIdentifier> token : creds.getAllTokens()) {
      ugi.addToken(token);
    }

    return ugi.doAs(new PrivilegedExceptionAction<Integer>() {
      public Integer run() {
        try {
          localizeFiles(nodeManager);
          return 0;
        } catch (Throwable e) {
          e.printStackTrace(System.out);
          return -1;
        }
      }
    });
  }

  public static void main(String[] argv) throws Throwable {
    // usage: $0 user appId host port log_dir user_dir [user_dir]
    // let $x = cwd for $local.dir
    // VERIFY $user_dir, log_dir exists, owned by the user w/ correct perms
    // VERIFY $logdir exists, owned by user w/ correct perms
    // MKDIR $x/$user/appcache
    // MKDIR $x/$user/filecache
    // MKDIR $x/$user/appcache/$appid
    // MKDIR $x/$user/appcache/$appid/output
    // MKDIR $x/$user/appcache/$appid/filecache
    // LOAD $x/$user/appcache/$appid/appTokens
    // LOAD $x/$user/appcache/$appid/appFiles
    // FOREACH file : files.PRIVATE
    // FETCH to $x/$user/filecache
    // FOREACH file : files.JOB
    // FETCH to $x/$user/$appid/filecache
    // WRITE $x/$user/appcache/$appid/privateEnv.sh
    try {
      String user = argv[0];
      String appId = argv[1];
      InetSocketAddress nmAddr =
          new InetSocketAddress(argv[2], Integer.parseInt(argv[3]));
      Path logDir = new Path(argv[4]);
      String[] sLocaldirs = Arrays.copyOfRange(argv, 5, argv.length);
      ArrayList<Path> localDirs = new ArrayList<Path>(sLocaldirs.length);
      for (String sLocaldir : sLocaldirs) {
        localDirs.add(new Path(sLocaldir));
      }

      final String uid =
          UserGroupInformation.getCurrentUser().getShortUserName();
      if (!user.equals(uid)) {
        LOG.warn("Localization running as " + uid + " not " + user); // TODO: throw exception.
      }

      ApplicationLocalizer localizer =
          new ApplicationLocalizer(user, appId, logDir, localDirs);
      System.exit(localizer.runLocalization(nmAddr));
    } catch (Throwable e) {
      // Print error to stdout so that LCE can use it.
      e.printStackTrace(System.out);
      // TODO: Above. Or set log4j props.
      throw e;
    }
  }

  private static class ShellScriptBuilder {

    private final StringBuilder sb;

    public ShellScriptBuilder() {
      this(new StringBuilder("#!/bin/bash\n\n"));
    }

    protected ShellScriptBuilder(StringBuilder sb) {
      this.sb = sb;
    }

    public ShellScriptBuilder env(String key, String value) {
      line("export ", key, "=\"", value, "\"");
      return this;
    }

    public ShellScriptBuilder symlink(Path src, String dst) throws IOException {
      return symlink(src, new Path(dst));
    }

    public ShellScriptBuilder symlink(Path src, Path dst) throws IOException {
      if (!src.isAbsolute()) {
        throw new IOException("Source must be absolute");
      }
      if (dst.isAbsolute()) {
        throw new IOException("Destination must be relative");
      }
      if (dst.toUri().getPath().indexOf('/') != -1) {
        line("mkdir -p ", dst.getParent().toString());
      }
      line("ln -sf ", src.toUri().getPath(), " ", dst.toString());
      return this;
    }

    public void write(PrintStream out) throws IOException {
      out.append(sb);
    }

    public void line(String... command) {
      for (String s : command) {
        sb.append(s);
      }
      sb.append("\n");
    }

    @Override
    public String toString() {
      return sb.toString();
    }

  }

}

