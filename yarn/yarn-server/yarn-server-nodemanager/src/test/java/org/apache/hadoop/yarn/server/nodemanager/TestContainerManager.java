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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ApplicationLocalizer;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.AvroUtil;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.LocalResourceType;
import org.apache.hadoop.yarn.LocalResourceVisibility;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.URL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContainerManager {

  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  private static Log LOG = LogFactory.getLog(TestContainerManager.class);

  private static File localDir = new File("target",
      TestContainerManager.class.getName() + "-localDir").getAbsoluteFile();

  private static File tmpDir = new File("target",
      TestContainerManager.class.getName() + "-tmpDir");

  private Configuration conf = new YarnConfiguration();
  private Context context = new NMContext();
  private ContainerExecutor exec = new DefaultContainerExecutor();
  private DeletionService delSrvc = new DeletionService(exec);
  private NodeStatusUpdater nodeStatusUpdater = new NodeStatusUpdaterImpl(context) {
    @Override
    protected org.apache.hadoop.yarn.ResourceTracker getRMClient() {
      return new LocalRMInterface();
    };

    @Override
    protected void startStatusUpdater() throws InterruptedException,
        AvroRemoteException {
      return; // Don't start any updating thread.
    }
  };

  private ContainerManagerImpl containerManager = null;

  @Before
  public void setup() throws IOException {
    FileContext localFS = FileContext.getLocalFSFileContext();
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(tmpDir.getAbsolutePath()), true);
    localDir.mkdir();
    tmpDir.mkdir();
    LOG.info("Created localDir in " + localDir.getAbsolutePath());
    LOG.info("Created tmpDir in " + tmpDir.getAbsolutePath());

    String bindAddress = "0.0.0.0:5555";
    conf.set(NMConfig.NM_BIND_ADDRESS, bindAddress);
    conf.set(NMConfig.NM_LOCAL_DIR, localDir.getAbsolutePath());
    containerManager =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater);
    containerManager.init(conf);
  }

  @After
  public void tearDown() {
    if (containerManager != null
        && containerManager.getServiceState() == STATE.STARTED) {
      containerManager.stop();
    }
  }

  @Test
  public void testContainerManagerInitialization() throws IOException {

    containerManager.start();

    // Just do a query for a non-existing container.
    boolean throwsException = false;
    try {
      containerManager.getContainerStatus(new ContainerID());
    } catch (AvroRemoteException e) {
      throwsException = true;
    }
    Assert.assertTrue(throwsException);
  }

  @Test
  public void testContainerSetup() throws IOException, InterruptedException {

    containerManager.start();

    // ////// Create the resources for the container
    File dir = new File(tmpDir, "dir");
    dir.mkdirs();
    File file = new File(dir, "file");
    PrintWriter fileWriter = new PrintWriter(file);
    fileWriter.write("Hello World!");
    fileWriter.close();

    ContainerLaunchContext container = new ContainerLaunchContext();

    // ////// Construct the Container-id
    ApplicationID appId = new ApplicationID();
    ContainerID cId = new ContainerID();
    cId.appID = appId;
    container.id = cId;

    String user = "dummy-user";
    container.user = user;

    // ////// Construct the container-spec.
    ContainerLaunchContext containerLaunchContext =
        new ContainerLaunchContext();
    containerLaunchContext.resources =
        new HashMap<CharSequence, LocalResource>();
    URL resource_alpha =
        AvroUtil.getYarnUrlFromPath(FileContext.getLocalFSFileContext()
            .makeQualified(new Path(file.getAbsolutePath())));
    LocalResource rsrc_alpha = new LocalResource();
    rsrc_alpha.resource = resource_alpha;
    rsrc_alpha.size= -1;
    rsrc_alpha.state = LocalResourceVisibility.APPLICATION;
    rsrc_alpha.type = LocalResourceType.FILE;
    rsrc_alpha.timestamp = file.lastModified();
    String destinationFile = "dest_file";
    containerLaunchContext.resources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.user = container.user;
    containerLaunchContext.id = container.id;
    containerLaunchContext.command = new ArrayList<CharSequence>();

    containerManager.startContainer(containerLaunchContext);

    DummyContainerManager.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    // Now ascertain that the resources are localised correctly.
    // TODO: Don't we need clusterStamp in localDir?
    File userCacheDir = new File(localDir, ApplicationLocalizer.USERCACHE);
    File userDir = new File(userCacheDir, user);
    File appCache = new File(userDir, ApplicationLocalizer.APPCACHE);
    File appDir = new File(appCache, AvroUtil.toString(appId));
    File containerDir = new File(appDir, AvroUtil.toString(cId));
    File targetFile = new File(containerDir, destinationFile);
    for (File f : new File[] { localDir, userCacheDir, appDir,
        containerDir }) {
      Assert.assertTrue(f.getAbsolutePath() + " doesn't exist!!", f.exists());
      Assert.assertTrue(f.getAbsolutePath() + " is not a directory!!",
          f.isDirectory());
    }
    Assert.assertTrue(targetFile.getAbsolutePath() + " doesn't exist!!",
        targetFile.exists());

    // Now verify the contents of the file
    BufferedReader reader = new BufferedReader(new FileReader(targetFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    Assert.assertEquals(null, reader.readLine());
  }

  @Test
  public void testContainerLaunchAndStop() throws IOException, InterruptedException {
    containerManager.start();

    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File outputFile = new File(tmpDir, "output.txt").getAbsoluteFile();
    fileWriter.write("echo Hello World! > " + outputFile);
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext = new ContainerLaunchContext();

    // ////// Construct the Container-id
    ApplicationID appId = new ApplicationID();
    ContainerID cId = new ContainerID();
    cId.appID = appId;
    containerLaunchContext.id = cId;

    String user = "dummy-user";
    containerLaunchContext.user = user;

    containerLaunchContext.resources =
        new HashMap<CharSequence, LocalResource>();
    URL resource_alpha =
        AvroUtil.getYarnUrlFromPath(FileContext.getLocalFSFileContext()
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha = new LocalResource();
    rsrc_alpha.resource = resource_alpha;
    rsrc_alpha.size= -1;
    rsrc_alpha.state = LocalResourceVisibility.APPLICATION;
    rsrc_alpha.type = LocalResourceType.FILE;
    rsrc_alpha.timestamp = scriptFile.lastModified();
    String destinationFile = "dest_file";
    containerLaunchContext.resources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.user = containerLaunchContext.user;
    List<CharSequence> commandArgs = new ArrayList<CharSequence>();
    commandArgs.add("/bin/bash");
    commandArgs.add(scriptFile.getAbsolutePath());
    containerLaunchContext.command = commandArgs;
    containerManager.startContainer(containerLaunchContext);

    DummyContainerManager.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    Assert.assertTrue("OutputFile doesn't exist!", outputFile.exists());
    
    // Now verify the contents of the file
    BufferedReader reader = new BufferedReader(new FileReader(outputFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    Assert.assertEquals(null, reader.readLine());

    // TODO: test the stop functionality.
  }

//  @Test
//  public void testCommandPreparation() {
//    ContainerLaunchContext container = new ContainerLaunchContext();
//
//    // ////// Construct the Container-id
//    ApplicationID appId = new ApplicationID();
//    appId.id = 0;
//    appId.clusterTimeStamp = 0;
//    ContainerID containerID = new ContainerID();
//    containerID.appID = appId;
//    containerID.id = 0;
//    container.id = containerID;
//
//    // The actual environment for the container
//    Path containerWorkDir =
//        NodeManager.getContainerWorkDir(new Path(localDir.getAbsolutePath()),
//            containerID);
//    final Map<String, String> ENVS = new HashMap<String, String>();
//    ENVS.put("JAVA_HOME", "/my/path/to/java-home");
//    ENVS.put("LD_LIBRARY_PATH", "/my/path/to/libraries");
//
//    File workDir = new File(ContainerBuilderHelper.getWorkDir());
//    File logDir = new File(workDir, "logs");
//    File stdout = new File(logDir, "stdout");
//    File stderr = new File(logDir, "stderr");
//    File tmpDir = new File(workDir, "tmp");
//    File javaHome = new File(ContainerBuilderHelper.getEnvVar("JAVA_HOME"));
//    String ldLibraryPath =
//        ContainerBuilderHelper.getEnvVar("LD_LIBRARY_PATH");
//    List<String> classPaths = new ArrayList<String>();
//    File someJar = new File(workDir, "jar-name.jar");
//    classPaths.add(someJar.toString());
//    classPaths.add(workDir.toString());
//    String PATH_SEPARATOR = System.getProperty("path.separator");
//    String classPath = StringUtils.join(PATH_SEPARATOR, classPaths);
//    File someFile = new File(workDir, "someFileNeededinEnv");
//
//    NMContainer nmContainer = new NMContainer(container, containerWorkDir) {
//      @Override
//      protected String checkAndGetEnvValue(String envVar) {
//        return ENVS.get(envVar);
//      }
//    };
//    List<CharSequence> command = new ArrayList<CharSequence>();
//    command.add(javaHome + "/bin/java");
//    command.add("-Djava.library.path=" + ldLibraryPath);
//    command.add("-Djava.io.tmpdir=" + tmpDir);
//    command.add("-classpath");
//    command.add(classPath);
//    command.add("2>" + stdout);
//    command.add("1>" + stderr);
//
//    Map<String, String> env = new HashMap<String, String>();
//    env.put("FILE_IN_ENV", someFile.toString());
//    env.put("JAVA_HOME", javaHome.toString());
//    env.put("LD_LIBRARY_PATH", ldLibraryPath);
//
//    String actualWorkDir = containerWorkDir.toUri().getPath();
//
//    String finalCmdSent = "";
//    for (CharSequence cmd : command) {
//      finalCmdSent += cmd + " ";
//    }
//    finalCmdSent.trim();
//    LOG.info("Final command sent is : " + finalCmdSent);
//
//    // The main method being tested
//    String[] finalCommands =
//        nmContainer.prepareCommandArgs(command, env, actualWorkDir);
//    // //////////////////////////////
//
//    String finalCmd = "";
//    for (String cmd : finalCommands) {
//      finalCmd += cmd + " ";
//    }
//    finalCmd = finalCmd.trim();
//    LOG.info("Final command for launch is : " + finalCmd);
//
//    File actualLogDir = new File(actualWorkDir, "logs");
//    File actualStdout = new File(actualLogDir, "stdout");
//    File actualStderr = new File(actualLogDir, "stderr");
//    File actualTmpDir = new File(actualWorkDir, "tmp");
//    File actualSomeJar = new File(actualWorkDir, "jar-name.jar");
//    File actualSomeFileInEnv = new File(actualWorkDir, "someFileNeededinEnv");
//    Assert.assertEquals(actualSomeFileInEnv.toString(),
//        env.get("FILE_IN_ENV"));
//    Assert.assertEquals("/my/path/to/java-home", env.get("JAVA_HOME"));
//    Assert.assertEquals("/my/path/to/libraries", env.get("LD_LIBRARY_PATH"));
//    Assert.assertEquals("/my/path/to/java-home/bin/java"
//        + " -Djava.library.path=/my/path/to/libraries" + " -Djava.io.tmpdir="
//        + actualTmpDir + " -classpath " + actualSomeJar + PATH_SEPARATOR
//        + actualWorkDir + " 2>" + actualStdout + " 1>" + actualStderr,
//        finalCmd);
//  }
}
