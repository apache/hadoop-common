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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.ContainerStatus;
import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.LocalResourceType;
import org.apache.hadoop.yarn.LocalResourceVisibility;
import org.apache.hadoop.yarn.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.DummyContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.LocalRMInterface;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ApplicationLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.AvroUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContainerManager {

  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  protected FileContext localFS;

  public TestContainerManager() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
  }

  private static Log LOG = LogFactory.getLog(TestContainerManager.class);

  protected static File localDir = new File("target",
      TestContainerManager.class.getName() + "-localDir").getAbsoluteFile();

  protected static File tmpDir = new File("target",
      TestContainerManager.class.getName() + "-tmpDir");

  protected Configuration conf = new YarnConfiguration();
  private Context context = new NMContext();
  private ContainerExecutor exec = new DefaultContainerExecutor();
  private DeletionService delSrvc;
  private Dispatcher dispatcher = new AsyncDispatcher();

  private String user = "nobody";

  private NodeStatusUpdater nodeStatusUpdater = new NodeStatusUpdaterImpl(
      context, dispatcher) {
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

  protected ContainerExecutor createContainerExecutor() {
    return new DefaultContainerExecutor();
  }

  @Before
  public void setup() throws IOException {
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(tmpDir.getAbsolutePath()), true);
    localDir.mkdir();
    tmpDir.mkdir();
    LOG.info("Created localDir in " + localDir.getAbsolutePath());
    LOG.info("Created tmpDir in " + tmpDir.getAbsolutePath());

    String bindAddress = "0.0.0.0:5555";
    conf.set(NMConfig.NM_BIND_ADDRESS, bindAddress);
    conf.set(NMConfig.NM_LOCAL_DIR, localDir.getAbsolutePath());

    // Default delSrvc
    delSrvc = new DeletionService(exec) {
      @Override
      public void delete(String user, Path subDir, Path[] baseDirs) {
        // Don't do any deletions.
      };
    };

    exec = createContainerExecutor();
    containerManager =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater);
    containerManager.init(conf);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (containerManager != null
        && containerManager.getServiceState() == STATE.STARTED) {
      containerManager.stop();
    }
    createContainerExecutor().deleteAsUser(user,
        new Path(localDir.getAbsolutePath()), new Path[] {});
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

    container.user = user;

    // ////// Construct the container-spec.
    ContainerLaunchContext containerLaunchContext =
        new ContainerLaunchContext();
    containerLaunchContext.resources =
        new HashMap<CharSequence, LocalResource>();
    URL resource_alpha =
        AvroUtil.getYarnUrlFromPath(localFS
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
    String appIDStr = AvroUtil.toString(appId);
    String containerIDStr = AvroUtil.toString(cId);
    File userCacheDir = new File(localDir, ApplicationLocalizer.USERCACHE);
    File userDir = new File(userCacheDir, user);
    File appCache = new File(userDir, ApplicationLocalizer.APPCACHE);
    File appDir = new File(appCache, appIDStr);
    File containerDir = new File(appDir, containerIDStr);
    File targetFile = new File(containerDir, destinationFile);
    File sysDir =
        new File(localDir,
            ResourceLocalizationService.NM_PRIVATE_DIR);
    File appSysDir = new File(sysDir, appIDStr);
    File containerSysDir = new File(appSysDir, containerIDStr);

    for (File f : new File[] { localDir, sysDir, userCacheDir, appDir,
        appSysDir,
        containerDir, containerSysDir }) {
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
  public void testContainerLaunchAndStop() throws IOException,
      InterruptedException {
    containerManager.start();

    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "start_file.txt").getAbsoluteFile();
    fileWriter.write("\numask 0"); // So that start file is readable by the test.
    fileWriter.write("\necho Hello World! > " + processStartFile);
    fileWriter.write("\necho $$ >> " + processStartFile);
    fileWriter.write("\nsleep 100");
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext =
        new ContainerLaunchContext();

    // ////// Construct the Container-id
    ApplicationID appId = new ApplicationID();
    ContainerID cId = new ContainerID();
    cId.appID = appId;
    containerLaunchContext.id = cId;

    containerLaunchContext.user = user;

    containerLaunchContext.resources =
        new HashMap<CharSequence, LocalResource>();
    URL resource_alpha =
        AvroUtil.getYarnUrlFromPath(localFS
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
 
    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());
    
    // Now verify the contents of the file
    BufferedReader reader =
        new BufferedReader(new FileReader(processStartFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    // Get the pid of the process
    String pid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    // Now test the stop functionality.

    // Assert that the process is alive
    Assert.assertTrue("Process is not alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));
    // Once more
    Assert.assertTrue("Process is not alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));

    containerManager.stopContainer(cId);

    DummyContainerManager.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);
    ContainerStatus containerStatus = containerManager.getContainerStatus(cId);
    Assert.assertEquals(ExitCode.KILLED.getExitCode(),
        containerStatus.exitStatus);

    // Assert that the process is not alive anymore
    Assert.assertFalse("Process is still alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));
  }

  @Test
  public void testLocalFilesCleanup() throws InterruptedException,
      IOException {
    // Real del service
    delSrvc = new DeletionService(exec);
    containerManager =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater);
    containerManager.init(conf);
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
    rsrc_alpha.size = -1;
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

    waitForApplicationState(containerManager, cId.appID,
        ApplicationState.RUNNING);

    // Now ascertain that the resources are localised correctly.
    String appIDStr = AvroUtil.toString(appId);
    String containerIDStr = AvroUtil.toString(cId);
    File userCacheDir = new File(localDir, ApplicationLocalizer.USERCACHE);
    File userDir = new File(userCacheDir, user);
    File appCache = new File(userDir, ApplicationLocalizer.APPCACHE);
    File appDir = new File(appCache, appIDStr);
    File containerDir = new File(appDir, containerIDStr);
    File targetFile = new File(containerDir, destinationFile);
    File sysDir =
        new File(localDir,
            ResourceLocalizationService.NM_PRIVATE_DIR);
    File appSysDir = new File(sysDir, appIDStr);
    File containerSysDir = new File(appSysDir, containerIDStr);
    // AppDir should still exist
    Assert.assertTrue("AppDir " + appDir.getAbsolutePath()
        + " doesn't exist!!", appDir.exists());
    Assert.assertTrue("AppSysDir " + appSysDir.getAbsolutePath()
        + " doesn't exist!!", appSysDir.exists());
    for (File f : new File[] { containerDir, containerSysDir }) {
      Assert.assertFalse(f.getAbsolutePath() + " exists!!", f.exists());
    }
    Assert.assertFalse(targetFile.getAbsolutePath() + " exists!!",
        targetFile.exists());

    // Simulate RM sending an AppFinish event.
    containerManager.handle(new CMgrCompletedAppsEvent(Arrays
        .asList(new ApplicationID[] { appId })));

    waitForApplicationState(containerManager, cId.appID,
        ApplicationState.FINISHED);

    // Now ascertain that the resources are localised correctly.
    for (File f : new File[] { appDir, containerDir, appSysDir,
        containerSysDir }) {
      // Wait for deletion. Deletion can happen long after AppFinish because of
      // the async DeletionService
      int timeout = 0;
      while (f.exists() && timeout++ < 15) {
        Thread.sleep(1000);
      }
      Assert.assertFalse(f.getAbsolutePath() + " exists!!", f.exists());
    }
    // Wait for deletion
    int timeout = 0;
    while (targetFile.exists() && timeout++ < 15) {
      Thread.sleep(1000);
    }
    Assert.assertFalse(targetFile.getAbsolutePath() + " exists!!",
        targetFile.exists());
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

  static void waitForApplicationState(ContainerManagerImpl containerManager,
      ApplicationID appID, ApplicationState finalState)
      throws InterruptedException {
    // Wait for app-finish
    Application app =
        containerManager.context.getApplications().get(appID);
    int timeout = 0;
    while (!(app.getApplicationState().equals(finalState))
        && timeout++ < 15) {
      LOG.info("Waiting for app to reach " + finalState
          + ".. Current state is "
          + app.getApplicationState());
      Thread.sleep(1000);
    }

    Assert.assertTrue("App is not in " + finalState + " yet!! Timedout!!",
        app.getApplicationState().equals(finalState));
  }
}
