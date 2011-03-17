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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
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
 * JobClient (user facing).
 *
 */
public class HistoryClientService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(HistoryClientService.class);
  
  private MRClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private HistoryContext history;

  public HistoryClientService(HistoryContext history) {
    super("HistoryClientService");
    this.history = history;
    this.protocolHandler = new MRClientProtocolHandler();
  }

  public void start() {
    Configuration conf = new Configuration(getConfig());
    YarnRPC rpc = YarnRPC.create(conf);
    String serviceAddr = conf.get("jobhistory.server.hostname") + ":"
        + conf.get("jobhistory.server.port");
    InetSocketAddress address = NetUtils.createSocketAddr(serviceAddr);
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = address.getAddress().getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }

    //TODO: security
    server =
        rpc.getServer(MRClientProtocol.class, protocolHandler, address,
            conf, null);
    server.start();
    this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress()
            + ":" + server.getPort());
    LOG.info("Instantiated MRClientService at " + this.bindAddress);
    
    //TODO: start webApp on fixed port ??
    super.start();
  }

  public void stop() {
    server.close();
    if (webApp != null) {
      webApp.stop();
    }
    super.stop();
  }

  private class MRClientProtocolHandler implements MRClientProtocol {

    private Job getJob(JobID jobID) throws AvroRemoteException {
      Job job = history.getJob(jobID);
      if (job == null) {
        throw RPCUtil.getRemoteException("Unknown job " + jobID);
      }
      return job;
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
      Job job = getJob(taskID.jobID);
      return job.getTask(taskID).getReport();
    }

    @Override
    public List<TaskAttemptCompletionEvent> getTaskAttemptCompletionEvents(
        JobID jobID, 
        int fromEventId, int maxEvents) throws AvroRemoteException {
      Job job = getJob(jobID);
      return Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, 
          maxEvents));
    }

    @Override
    public Void killJob(JobID jobID) throws AvroRemoteException {
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }

    @Override
    public Void killTask(TaskID taskID) throws AvroRemoteException {
      getJob(taskID.jobID);
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }

    @Override
    public Void killTaskAttempt(TaskAttemptID taskAttemptID)
        throws AvroRemoteException {
      getJob(taskAttemptID.taskID.jobID);
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
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
      Job job = getJob(jobID);
      List<TaskReport> reports = new ArrayList<TaskReport>();
      Collection<Task> tasks = job.getTasks(taskType).values();
      for (Task task : tasks) {
        reports.add(task.getReport());
      }
      return reports;
    }

  }
}
