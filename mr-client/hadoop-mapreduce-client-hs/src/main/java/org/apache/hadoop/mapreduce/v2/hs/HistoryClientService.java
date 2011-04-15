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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.hs.webapp.HSWebApp;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;

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
    initializeWebApp(conf);
    String serviceAddr = conf.get(YarnMRJobConfig.HS_BIND_ADDRESS,
        YarnMRJobConfig.DEFAULT_HS_BIND_ADDRESS);
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

  private void initializeWebApp(Configuration conf) {
    webApp = new HSWebApp(history);
    String bindAddress = conf.get(YarnMRJobConfig.HS_WEBAPP_BIND_ADDRESS,
        YarnMRJobConfig.DEFAULT_HS_WEBAPP_BIND_ADDRESS);
    WebApps.$for("yarn", this).at(bindAddress).start(webApp); 
  }

  @Override
  public void stop() {
    if (server != null) {
      server.close();
    }
    if (webApp != null) {
      webApp.stop();
    }
    super.stop();
  }

  private class MRClientProtocolHandler implements MRClientProtocol {

    private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    
    private Job getJob(JobId jobId) throws YarnRemoteException {
      Job job = history.getJob(jobId);
      if (job == null) {
        throw RPCUtil.getRemoteException("Unknown job " + jobId);
      }
      return job;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = getJob(jobId);
      GetCountersResponse response = recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(job.getCounters());
      return response;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = getJob(jobId);
      GetJobReportResponse response = recordFactory.newRecordInstance(GetJobReportResponse.class);
      response.setJobReport(job.getReport());
      return response;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(GetTaskAttemptReportRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      Job job = getJob(taskAttemptId.getTaskId().getJobId());
      GetTaskAttemptReportResponse response = recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      Job job = getJob(taskId.getJobId());
      GetTaskReportResponse response = recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(job.getTask(taskId).getReport());
      return response;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(GetTaskAttemptCompletionEventsRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      int fromEventId = request.getFromEventId();
      int maxEvents = request.getMaxEvents();
      
      Job job = getJob(jobId);
      GetTaskAttemptCompletionEventsResponse response = recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
      response.addAllCompletionEvents(Arrays.asList(job.getTaskAttemptCompletionEvents(fromEventId, maxEvents)));
      return response;
    }
      
    @Override
    public KillJobResponse killJob(KillJobRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }
    
    @Override
    public KillTaskResponse killTask(KillTaskRequest request) throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      getJob(taskId.getJobId());
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }
    
    @Override
    public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      getJob(taskAttemptId.getTaskId().getJobId());
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
    
      Job job = getJob(taskAttemptId.getTaskId().getJobId());
      
      GetDiagnosticsResponse response = recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
      response.addAllDiagnostics(job.getTask(taskAttemptId.getTaskId()).getAttempt(taskAttemptId).getDiagnostics());
      return response;
    }

    @Override 
    public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      getJob(taskAttemptId.getTaskId().getJobId());
      throw RPCUtil.getRemoteException("Invalid operation on completed job");
    }

    @Override
    public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      TaskType taskType = request.getTaskType();
      
      GetTaskReportsResponse response = recordFactory.newRecordInstance(GetTaskReportsResponse.class);
      Job job = getJob(jobId);
      Collection<Task> tasks = job.getTasks(taskType).values();
      for (Task task : tasks) {
        response.addTaskReport(task.getReport());
      }
      return response;
    }

  }

}
