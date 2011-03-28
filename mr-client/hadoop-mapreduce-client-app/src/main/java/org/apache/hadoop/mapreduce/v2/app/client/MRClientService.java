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

package org.apache.hadoop.mapreduce.v2.app.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

/**
 * This module is responsible for talking to the 
 * jobclient (user facing).
 *
 */
public class MRClientService extends AbstractService 
    implements ClientService {

  private static final Log LOG = LogFactory.getLog(MRClientService.class);
  
  private MRClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private AppContext appContext;

  public MRClientService(AppContext appContext) {
    super("MRClientService");
    this.appContext = appContext;
    this.protocolHandler = new MRClientProtocolHandler();
  }

  public void start() {
    Configuration conf = new Configuration(getConfig()); // Just for not messing up sec-info class config
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = NetUtils.createSocketAddr("0.0.0.0:0");
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = address.getAddress().getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }

    ClientToAMSecretManager secretManager = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      secretManager = new ClientToAMSecretManager();
      String secretKeyStr =
          System.getenv(YarnConfiguration.APPLICATION_CLIENT_SECRET_ENV_NAME);
      byte[] bytes = Base64.decodeBase64(secretKeyStr);
      ApplicationTokenIdentifier identifier =
          new ApplicationTokenIdentifier(this.appContext.getApplicationID());
      secretManager.setMasterKey(identifier, bytes);
      conf.setClass(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_INFO_CLASS_NAME,
          SchedulerSecurityInfo.class, SecurityInfo.class); // Same for now.
    }
    server =
        rpc.getServer(MRClientProtocol.class, protocolHandler, address,
            conf, secretManager);
    server.start();
    this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress()
            + ":" + server.getPort());
    LOG.info("Instantiated MRClientService at " + this.bindAddress);
    try {
      webApp = WebApps.$for("yarn", AppContext.class, appContext).with(conf).
          start(new AMWebApp());
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
    super.start();
  }

  public void stop() {
    server.close();
    if (webApp != null) {
      webApp.stop();
    }
    super.stop();
  }

  @Override
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  public int getHttpPort() {
    return webApp.port();
  }

  class MRClientProtocolHandler implements MRClientProtocol {

    private Job verifyAndGetJob(JobID jobID) throws AvroRemoteException {
      Job job = appContext.getJob(jobID);
      if (job == null) {
        throw RPCUtil.getRemoteException("Unknown job " + jobID);
      }
      return job;
    }
 
    private Task verifyAndGetTask(TaskID taskID) throws AvroRemoteException {
      Task task = verifyAndGetJob(taskID.jobID).getTask(taskID);
      if (task == null) {
        throw RPCUtil.getRemoteException("Unknown Task " + taskID);
      }
      return task;
    }

    private TaskAttempt verifyAndGetAttempt(TaskAttemptID attemptID) 
          throws AvroRemoteException {
      TaskAttempt attempt = verifyAndGetTask(attemptID.taskID).getAttempt(attemptID);
      if (attempt == null) {
        throw RPCUtil.getRemoteException("Unknown TaskAttempt " + attemptID);
      }
      return attempt;
    }

    @Override
    public Counters getCounters(JobID jobID) throws AvroRemoteException {
      Job job = verifyAndGetJob(jobID);
      return job.getCounters();
    }

    @Override
    public JobReport getJobReport(JobID jobID) throws AvroRemoteException {
      Job job = verifyAndGetJob(jobID);
      return job.getReport();
    }

    @Override
    public TaskAttemptReport getTaskAttemptReport(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      return verifyAndGetAttempt(taskAttemptID).getReport();
    }

    @Override
    public TaskReport getTaskReport(TaskID taskID) throws AvroRemoteException {
      return verifyAndGetTask(taskID).getReport();
    }

    @Override
    public List<TaskAttemptCompletionEvent> getTaskAttemptCompletionEvents(JobID jobID, 
        int fromEventId, int maxEvents) throws AvroRemoteException {
      Job job = verifyAndGetJob(jobID);
      return Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, maxEvents));
    }

    @Override
    public Void killJob(JobID jobID) throws AvroRemoteException {
      LOG.info("Kill Job received from client " + jobID);
      verifyAndGetJob(jobID);
      appContext.getEventHandler().handle(
          new JobEvent(jobID, JobEventType.JOB_KILL));
      return null;
    }

    @Override
    public Void killTask(TaskID taskID) throws AvroRemoteException {
      LOG.info("Kill task received from client " + taskID);
      verifyAndGetTask(taskID);
      appContext.getEventHandler().handle(
          new TaskEvent(taskID, TaskEventType.T_KILL));
      return null;
    }

    @Override
    public Void killTaskAttempt(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      LOG.info("Kill task attempt received from client " + taskAttemptID);
      verifyAndGetAttempt(taskAttemptID);
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptID, 
              TaskAttemptEventType.TA_KILL));
      return null;
    }

    @Override
    public Void failTaskAttempt(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      LOG.info("Fail task attempt received from client " + taskAttemptID);
      verifyAndGetAttempt(taskAttemptID);
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptID, 
              TaskAttemptEventType.TA_FAILMSG));
      return null;
    }

    @Override
    public List<CharSequence> getDiagnostics(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      return verifyAndGetAttempt(taskAttemptID).getDiagnostics();
    }

    @Override
    public List<TaskReport> getTaskReports(JobID jobID, TaskType taskType)
        throws AvroRemoteException {
      Job job = verifyAndGetJob(jobID);
      LOG.info("Getting task report for " + taskType + "   " + jobID);
      List<TaskReport> reports = new ArrayList<TaskReport>();
      Collection<Task> tasks = job.getTasks(taskType).values();
      LOG.info("Getting task report size " + tasks.size());
      for (Task task : tasks) {
        reports.add(task.getReport());
      }
      return reports;
    }

  }
}
