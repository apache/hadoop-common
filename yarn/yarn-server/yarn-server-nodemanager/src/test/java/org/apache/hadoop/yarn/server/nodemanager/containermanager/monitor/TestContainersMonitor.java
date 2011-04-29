package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.TestProcfsBasedProcessTree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContainersMonitor {

  private static final Log LOG = LogFactory
      .getLog(TestContainersMonitor.class);

  protected static File localDir = new File("target",
      TestContainersMonitor.class.getName() + "-localDir").getAbsoluteFile();
  protected static File logDir = new File("target",
      TestContainersMonitor.class.getName() + "-logDir").getAbsoluteFile();
  protected static File tmpDir = new File("target",
      TestContainersMonitor.class.getName() + "-tmpDir");

  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  protected FileContext localFS;

  public TestContainersMonitor() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
  }

  protected Configuration conf = new YarnConfiguration();
  private Context context = new NMContext();
  private ContainerExecutor exec = new DefaultContainerExecutor();
  private DeletionService delSrvc= new DeletionService(exec);
  private Dispatcher dispatcher = new AsyncDispatcher();
  private NodeHealthCheckerService healthChecker = null;
  private String user = "nobody";

  private NodeStatusUpdater nodeStatusUpdater = new NodeStatusUpdaterImpl(
      context, dispatcher, healthChecker) {
    @Override
    protected ResourceTracker getRMClient() {
      return new LocalRMInterface();
    };

    @Override
    protected void startStatusUpdater() throws InterruptedException,
        YarnRemoteException {
      return; // Don't start any updating thread.
    }
  };

  private ContainerManagerImpl containerManager = null;

  @Before
  public void setup() throws IOException {
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(tmpDir.getAbsolutePath()), true);
    localFS.delete(new Path(logDir.getAbsolutePath()), true);
    localDir.mkdir();
    tmpDir.mkdir();
    logDir.mkdir();
    LOG.info("Created localDir in " + localDir.getAbsolutePath());
    LOG.info("Created tmpDir in " + tmpDir.getAbsolutePath());

    String bindAddress = "0.0.0.0:5555";
    conf.set(NMConfig.NM_BIND_ADDRESS, bindAddress);
    conf.set(NMConfig.NM_LOCAL_DIR, localDir.getAbsolutePath());
    conf.set(NMConfig.NM_LOG_DIR, logDir.getAbsolutePath());

    containerManager =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater);
    conf.setClass(
        ContainersMonitorImpl.RESOURCE_CALCULATOR_PLUGIN_CONFIG_KEY,
        LinuxResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    containerManager.init(conf);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (containerManager != null
        && containerManager.getServiceState() == STATE.STARTED) {
      containerManager.stop();
    }
    exec.deleteAsUser(user, new Path(localDir.getAbsolutePath()),
        new Path[] {});
  }

  /**
   * Test to verify the check for whether a process tree is over limit or not.
   * 
   * @throws IOException
   *           if there was a problem setting up the fake procfs directories or
   *           files.
   */
  @Test
  public void testProcessTreeLimits() throws IOException {

    // set up a dummy proc file system
    File procfsRootDir = new File(localDir, "proc");
    String[] pids = { "100", "200", "300", "400", "500", "600", "700" };
    try {
      TestProcfsBasedProcessTree.setupProcfsRootDir(procfsRootDir);

      // create pid dirs.
      TestProcfsBasedProcessTree.setupPidDirs(procfsRootDir, pids);

      // create process infos.
      TestProcfsBasedProcessTree.ProcessStatInfo[] procs =
          new TestProcfsBasedProcessTree.ProcessStatInfo[7];

      // assume pids 100, 500 are in 1 tree
      // 200,300,400 are in another
      // 600,700 are in a third
      procs[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "100", "proc1", "1", "100", "100", "100000" });
      procs[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "200", "proc2", "1", "200", "200", "200000" });
      procs[2] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "300", "proc3", "200", "200", "200", "300000" });
      procs[3] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "400", "proc4", "200", "200", "200", "400000" });
      procs[4] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "500", "proc5", "100", "100", "100", "1500000" });
      procs[5] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "600", "proc6", "1", "600", "600", "100000" });
      procs[6] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "700", "proc7", "600", "600", "600", "100000" });
      // write stat files.
      TestProcfsBasedProcessTree.writeStatFiles(procfsRootDir, pids, procs);

      // vmem limit
      long limit = 700000;

      ContainersMonitorImpl test = new ContainersMonitorImpl(null, null);

      // create process trees
      // tree rooted at 100 is over limit immediately, as it is
      // twice over the mem limit.
      ProcfsBasedProcessTree pTree = new ProcfsBasedProcessTree(
                                          "100", true,
                                          procfsRootDir.getAbsolutePath());
      pTree.getProcessTree();
      assertTrue("tree rooted at 100 should be over limit " +
                    "after first iteration.",
                  test.isProcessTreeOverLimit(pTree, "dummyId", limit));

      // the tree rooted at 200 is initially below limit.
      pTree = new ProcfsBasedProcessTree("200", true,
                                          procfsRootDir.getAbsolutePath());
      pTree.getProcessTree();
      assertFalse("tree rooted at 200 shouldn't be over limit " +
                    "after one iteration.",
                  test.isProcessTreeOverLimit(pTree, "dummyId", limit));
      // second iteration - now the tree has been over limit twice,
      // hence it should be declared over limit.
      pTree.getProcessTree();
      assertTrue(
          "tree rooted at 200 should be over limit after 2 iterations",
                  test.isProcessTreeOverLimit(pTree, "dummyId", limit));

      // the tree rooted at 600 is never over limit.
      pTree = new ProcfsBasedProcessTree("600", true,
                                            procfsRootDir.getAbsolutePath());
      pTree.getProcessTree();
      assertFalse("tree rooted at 600 should never be over limit.",
                    test.isProcessTreeOverLimit(pTree, "dummyId", limit));

      // another iteration does not make any difference.
      pTree.getProcessTree();
      assertFalse("tree rooted at 600 should never be over limit.",
                    test.isProcessTreeOverLimit(pTree, "dummyId", limit));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  @Test
  public void testContainerKillOnMemoryOverflow() throws IOException,
      InterruptedException {

    if (!ProcfsBasedProcessTree.isAvailable()) {
      return;
    }

    containerManager.start();

    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "start_file.txt").getAbsoluteFile();
    fileWriter.write("\numask 0"); // So that start file is readable by the
                                   // test.
    fileWriter.write("\necho Hello World! > " + processStartFile);
    fileWriter.write("\necho $$ >> " + processStartFile);
    fileWriter.write("\nsleep 15");
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // ////// Construct the Container-id
    ApplicationId appId =
        recordFactory.newRecordInstance(ApplicationId.class);
    ContainerId cId = recordFactory.newRecordInstance(ContainerId.class);
    cId.setAppId(appId);
    cId.setId(0);
    containerLaunchContext.setContainerId(cId);

    containerLaunchContext.setUser(user);

    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    containerLaunchContext.setLocalResource(destinationFile, rsrc_alpha);
    containerLaunchContext.setUser(containerLaunchContext.getUser());
    containerLaunchContext.addCommand("/bin/bash");
    containerLaunchContext.addCommand(scriptFile.getAbsolutePath());
    containerLaunchContext.setResource(recordFactory
        .newRecordInstance(Resource.class));
    containerLaunchContext.getResource().setMemory(8 * 1024 * 1024);
    StartContainerRequest startRequest =
        recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    containerManager.startContainer(startRequest);

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

    DummyContainerManager.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE, 60);

    GetContainerStatusRequest gcsRequest =
        recordFactory.newRecordInstance(GetContainerStatusRequest.class);
    gcsRequest.setContainerId(cId);
    ContainerStatus containerStatus =
        containerManager.getContainerStatus(gcsRequest).getStatus();
    Assert.assertEquals(String.valueOf(ExitCode.KILLED.getExitCode()),
        containerStatus.getExitStatus());
    String expectedMsgPattern =
        "Container \\[pid=" + pid + ",containerID=" + cId
            + "\\] is running beyond memory-limits. Current usage : "
            + "[0-9]*bytes. Limit : [0-9]*"
            + "bytes. Killing container. \nDump of the process-tree for "
            + cId + " : \n";
    Pattern pat = Pattern.compile(expectedMsgPattern);
    Assert.assertEquals("Expected message patterns is: " + expectedMsgPattern
        + "\n\nObserved message is: " + containerStatus.getDiagnostics(),
        true, pat.matcher(containerStatus.getDiagnostics()).find());

    // Assert that the process is not alive anymore
    Assert.assertFalse("Process is still alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));
  }
}
