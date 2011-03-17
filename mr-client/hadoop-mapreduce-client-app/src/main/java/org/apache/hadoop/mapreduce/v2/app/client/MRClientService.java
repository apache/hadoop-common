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
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
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
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
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
  private EventHandler<Event> handler;

  public MRClientService(AppContext appContext) {
    super("MRClientService");
    this.appContext = appContext;
    this.protocolHandler = new MRClientProtocolHandler(appContext);
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

    private AppContext appContext;

    private Job getJob(JobID jobID) throws AvroRemoteException {
      Job job = appContext.getJob(jobID);
      if (job == null) {
        throw RPCUtil.getRemoteException("Unknown job " + jobID);
      }
      return job;
    }
    
    MRClientProtocolHandler(AppContext appContext) {
      this.appContext = appContext;
    }

    @Override
    public Counters getCounters(JobID jobID) throws AvroRemoteException {
      Job job = getJob(jobID);
      return job.getCounters();
    }

    @Override
    public JobReport getJobReport(JobID jobID) throws AvroRemoteException {
      Job job = getJob(jobID);
      return job.getReport();
    }

    @Override
    public TaskAttemptReport getTaskAttemptReport(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      Job job = getJob(taskAttemptID.taskID.jobID);
      return job.getTask(taskAttemptID.taskID).
          getAttempt(taskAttemptID).getReport();
    }

    @Override
    public TaskReport getTaskReport(TaskID taskID) throws AvroRemoteException {
      Job job = appContext.getJob(taskID.jobID);
      return job.getTask(taskID).getReport();
    }

    @Override
    public List<TaskAttemptCompletionEvent> getTaskAttemptCompletionEvents(JobID jobID, 
        int fromEventId, int maxEvents) throws AvroRemoteException {
      Job job = appContext.getJob(jobID);
      return Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, maxEvents));
    }

    @Override
    public Void killJob(JobID jobID) throws AvroRemoteException {
      getJob(jobID);
      handler.handle(
          new JobEvent(jobID, JobEventType.JOB_KILL));
      return null;
    }

    @Override
    public Void killTask(TaskID taskID) throws AvroRemoteException {
      getJob(taskID.jobID);
      handler.handle(
          new TaskEvent(taskID, TaskEventType.T_KILL));
      return null;
    }

    @Override
    public Void killTaskAttempt(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      getJob(taskAttemptID.taskID.jobID);
     handler.handle(
          new TaskAttemptEvent(taskAttemptID, 
              TaskAttemptEventType.TA_KILL));
      return null;
    }

    @Override
    public List<CharSequence> getDiagnostics(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      Job job = getJob(taskAttemptID.taskID.jobID);
      return job.getTask(taskAttemptID.taskID).
                 getAttempt(taskAttemptID).getDiagnostics();
    }

    @Override
    public List<TaskReport> getTaskReports(JobID jobID, TaskType taskType)
        throws AvroRemoteException {
      Job job = appContext.getJob(jobID);
      List<TaskReport> reports = new ArrayList<TaskReport>();
      Collection<Task> tasks = job.getTasks(taskType).values();
      for (Task task : tasks) {
        reports.add(task.getReport());
      }
      return reports;
    }

  }
}
