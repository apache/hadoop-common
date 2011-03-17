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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.LocalizationProtocol;

public abstract class ContainerExecutor implements Configurable {

  static final Log LOG = LogFactory.getLog(ContainerExecutor.class);
  final public static FsPermission TASK_LAUNCH_SCRIPT_PERMISSION =
    FsPermission.createImmutable((short) 0700);

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Prepare the environment for containers in this application to execute.
   * For $x in local.dirs
   *   create $x/$user/$appId
   * Copy $nmLocal/appTokens -> $N/$user/$appId
   * Copy $nmLocal/publicEnv.sh -> $N/$user/$appId
   * For $rsrc in private resources
   *   Copy $rsrc -> $N/$user/filecache/[idef]
   * For $rsrc in job resources
   *   Copy $rsrc -> $N/$user/$appId/filecache/idef
   * @param user user name of application owner
   * @param appId id of the application
   * @param nmLocal path to localized credentials, rsrc by NM
   * @param nmAddr RPC address to contact NM
   * @throws IOException For most application init failures
   * @throws InterruptedException If application init thread is halted by NM
   */
  public abstract void initApplication(Path nmLocal,
      LocalizationProtocol localization,
      String user, String appId, Path logDir, List<Path> localDirs)
      throws IOException, InterruptedException;

  /**
   * Launch the container on the node.
   * @param launchCtxt 
   */
  public abstract int launchContainer(Container container, Path nmLocal,
      String user, String appId, List<Path> appDirs, String stdout,
      String stderr) throws IOException;

  public abstract boolean signalContainer(String user, int pid, Signal signal)
      throws IOException, InterruptedException;

  public abstract void deleteAsUser(String user, Path subDir, Path... basedirs)
      throws IOException, InterruptedException;

  /**
   * The constants for the signals.
   */
  public enum Signal {
    NULL(0, "NULL"), QUIT(3, "SIGQUIT"), 
    KILL(9, "SIGKILL"), TERM(15, "SIGTERM");
    private final int value;
    private final String str;
    private Signal(int value, String str) {
      this.str = str;
      this.value = value;
    }
    public int getValue() {
      return value;
    }
    @Override
    public String toString() {
      return str;
    }
  }

  protected void logOutput(String output) {
    String shExecOutput = output;
    if (shExecOutput != null) {
      for (String str : shExecOutput.split("\n")) {
        LOG.info(str);
      }
    }
  }

  public static final boolean isSetsidAvailable = isSetsidSupported();
  private static boolean isSetsidSupported() {
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = {"setsid", "bash", "-c", "echo $$"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      LOG.info("setsid exited with exit code " + shexec.getExitCode());
    }
    return setsidSupported;
  }

  public static class DelayedProcessKiller extends Thread {
    private final String user;
    private final int pid;
    private final long delay;
    private final Signal signal;
    private final ContainerExecutor containerExecutor;
    public DelayedProcessKiller(String user, int pid, long delay, Signal signal,
        ContainerExecutor containerExecutor) {
      this.user = user;
      this.pid = pid;
      this.delay = delay;
      this.signal = signal;
      this.containerExecutor = containerExecutor;
      setName("Task killer for " + pid);
      setDaemon(false);
    }
    @Override
    public void run() {
      try {
        Thread.sleep(delay);
        containerExecutor.signalContainer(user, pid, signal);
      } catch (InterruptedException e) {
        return;
      } catch (IOException e) {
        LOG.warn("Exception when killing task " + pid, e);
      }
    }
  }

}
