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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);

  private Configuration conf;
  private ApplicationId currentAppId;
  private final ResourceMgrDelegate rm;
  private MRClientProtocol realProxy = null;
  private String serviceAddr = "";
  private String serviceHttpAddr = "";
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm) {
    this.conf = conf;
    this.rm = rm;
  }

  private MRClientProtocol getProxy(JobID jobId) throws YarnRemoteException {
    return getProxy(TypeConverter.toYarn(jobId).getAppId(), false);
  }

  private MRClientProtocol getRefreshedProxy(JobID jobId) throws YarnRemoteException {
    return getProxy(TypeConverter.toYarn(jobId).getAppId(), true);
  }

  private MRClientProtocol getProxy(ApplicationId appId, 
      boolean forceRefresh) throws YarnRemoteException {
    if (!appId.equals(currentAppId) || forceRefresh) {
      currentAppId = appId;
      refreshProxy();
    }
    return realProxy;
  }

  private void refreshProxy() throws YarnRemoteException {
    ApplicationMaster appMaster = rm.getApplicationMaster(currentAppId);
    while (!ApplicationState.COMPLETED.equals(appMaster.getState()) ||
           !ApplicationState.FAILED.equals(appMaster.getState()) || 
           !ApplicationState.KILLED.equals(appMaster.getState())) {
      try {
        if (appMaster.getHost() == null || "".equals(appMaster.getHost())) {
          LOG.info("AM not assigned to Job. Waiting to get the AM ...");
          Thread.sleep(2000);
          appMaster = rm.getApplicationMaster(currentAppId);
          continue;
        }
        serviceAddr = appMaster.getHost() + ":" + appMaster.getRpcPort();
        serviceHttpAddr = appMaster.getHost() + ":" + appMaster.getHttpPort();
        if (UserGroupInformation.isSecurityEnabled()) {
          String clientTokenEncoded = appMaster.getClientToken();
          Token<ApplicationTokenIdentifier> clientToken =
            new Token<ApplicationTokenIdentifier>();
          clientToken.decodeFromUrlString(clientTokenEncoded);
            clientToken.setService(new Text(appMaster.getHost() + ":"
                + appMaster.getRpcPort()));
            UserGroupInformation.getCurrentUser().addToken(clientToken);
        }
        LOG.info("Connecting to " + serviceAddr);
        instantiateProxy(serviceAddr);
        return;
      } catch (Exception e) {
        //possibly
        //possibly the AM has crashed
        //there may be some time before AM is restarted
        //keep retrying by getting the address from RM
        LOG.info("Could not connect to " + serviceAddr + 
            ". Waiting for getting the latest AM address...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
        }
        appMaster = rm.getApplicationMaster(currentAppId);
      }
    }
    if (ApplicationState.COMPLETED.equals(appMaster.getState())) {
      serviceAddr = conf.get(YarnMRJobConfig.HS_BIND_ADDRESS,
          YarnMRJobConfig.DEFAULT_HS_BIND_ADDRESS);
      LOG.info("Application state is completed. " +
            "Redirecting to job history server " + serviceAddr);
      //TODO:
      serviceHttpAddr = "";
      try {
        instantiateProxy(serviceAddr);
        return;
      } catch (IOException e) {
        throw new YarnException(e);
      }
    }
    LOG.warn("Cannot connect to Application with state " + appMaster.getState());
      throw new YarnException(
        "Cannot connect to Application with state " + appMaster.getState());
  }

  private void instantiateProxy(final String serviceAddr) throws IOException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    realProxy = currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
      @Override
      public MRClientProtocol run() {
        Configuration myConf = new Configuration(conf);
        myConf.setClass(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_INFO_CLASS_NAME,
        SchedulerSecurityInfo.class, SecurityInfo.class); 
        YarnRPC rpc = YarnRPC.create(myConf);
        return (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
        NetUtils.createSocketAddr(serviceAddr), myConf);
      }
    });
  }

  public org.apache.hadoop.mapreduce.Counters getJobCounters(JobID arg0) throws IOException,
      InterruptedException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobID = TypeConverter.toYarn(arg0);
    try {
      GetCountersRequest request = recordFactory.newRecordInstance(GetCountersRequest.class);
      request.setJobId(jobID);
      return TypeConverter.fromYarn(getProxy(arg0).getCounters(request).getCounters());
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failing to contact application master", e);
      try {
        GetCountersRequest request = recordFactory.newRecordInstance(GetCountersRequest.class);
        request.setJobId(jobID);
        return TypeConverter.fromYarn(getRefreshedProxy(arg0).getCounters(request).getCounters());
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
  }

  public String getJobHistoryDir() throws IOException, InterruptedException {
    //TODO fix this
    return "";
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobID = TypeConverter.toYarn(arg0);
    List<org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent> list = null;
    GetTaskAttemptCompletionEventsRequest request = recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsRequest.class);
    try {
      request.setJobId(jobID);
      request.setFromEventId(arg1);
      request.setMaxEvents(arg2);
      list = getProxy(arg0).getTaskAttemptCompletionEvents(request).getCompletionEventList();
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        request.setJobId(jobID);
        request.setFromEventId(arg1);
        request.setMaxEvents(arg2);
        list = getRefreshedProxy(arg0).getTaskAttemptCompletionEvents(request).getCompletionEventList();
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return TypeConverter.fromYarn(
        list.toArray(new org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent[0]));
  }

  public String[] getTaskDiagnostics(org.apache.hadoop.mapreduce.TaskAttemptID
      arg0)
  throws IOException,
      InterruptedException {
    
    List<String> list = null;
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID = TypeConverter.toYarn(arg0);
    GetDiagnosticsRequest request = recordFactory.newRecordInstance(GetDiagnosticsRequest.class);
    try {
      request.setTaskAttemptId(attemptID);
      list = getProxy(arg0.getJobID()).getDiagnostics(request).getDiagnosticsList();
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        request.setTaskAttemptId(attemptID);
        list = getRefreshedProxy(arg0.getJobID()).getDiagnostics(request).getDiagnosticsList();
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    String[] result = new String[list.size()];
    int i = 0;
    for (String c : list) {
      result[i++] = c.toString();
    }
    return result;
  }

  public JobStatus getJobStatus(JobID oldJobID) throws YarnRemoteException,
      YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId jobId = 
      TypeConverter.toYarn(oldJobID);
    LOG.debug("Getting Job status");
    String stagingDir = conf.get("yarn.apps.stagingDir");
    String jobFile = stagingDir + "/" + jobId.toString();
    JobReport report = null;
    GetJobReportRequest request = recordFactory.newRecordInstance(GetJobReportRequest.class);
    try {
      request.setJobId(jobId);
      report = getProxy(oldJobID).getJobReport(request).getJobReport();
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch (Exception e) {
      try {
        request.setJobId(jobId);
        report = getRefreshedProxy(oldJobID).getJobReport(request).getJobReport();
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return TypeConverter.fromYarn(report, jobFile, serviceHttpAddr);
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
      throws YarnRemoteException, YarnRemoteException {
      List<org.apache.hadoop.mapreduce.v2.api.records.TaskReport> taskReports = null;
      org.apache.hadoop.mapreduce.v2.api.records.JobId nJobID = TypeConverter.toYarn(jobID);
      GetTaskReportsRequest request = recordFactory.newRecordInstance(GetTaskReportsRequest.class);
      try {
        request.setJobId(nJobID);
        request.setTaskType(TypeConverter.toYarn(taskType));
        taskReports = getProxy(jobID).getTaskReports(request).getTaskReportList();
      } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      } catch(Exception e) {
        LOG.debug("Failed to contact application master ", e);
        try {
          request.setJobId(nJobID);
          request.setTaskType(TypeConverter.toYarn(taskType));
          taskReports = getRefreshedProxy(jobID).getTaskReports(request).getTaskReportList();
        } catch(YarnRemoteException yre) {
          LOG.warn(RPCUtil.toString(yre));
          throw yre;
        }
      }
      return TypeConverter.fromYarn
        (taskReports).toArray(new org.apache.hadoop.mapreduce.TaskReport[0]);
  }

  public Void killJob(JobID jobID) throws YarnRemoteException,
      YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.JobId  nJobID = TypeConverter.toYarn(jobID);
    KillJobRequest request = recordFactory.newRecordInstance(KillJobRequest.class);
    try {
      request.setJobId(nJobID);
      getProxy(jobID).killJob(request);
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        request.setJobId(nJobID);
        getRefreshedProxy(jobID).killJob(request);
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return null;
  }

  public boolean killTask(TaskAttemptID taskAttemptID, boolean fail)
      throws YarnRemoteException {
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID 
      = TypeConverter.toYarn(taskAttemptID);
    KillTaskAttemptRequest killRequest = recordFactory.newRecordInstance(KillTaskAttemptRequest.class);
    FailTaskAttemptRequest failRequest = recordFactory.newRecordInstance(FailTaskAttemptRequest.class);
    try {
      if (fail) {
        failRequest.setTaskAttemptId(attemptID);
        getProxy(taskAttemptID.getJobID()).failTaskAttempt(failRequest);
      } else {
        killRequest.setTaskAttemptId(attemptID);
        getProxy(taskAttemptID.getJobID()).killTaskAttempt(killRequest);
      }
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        if (fail) {
          failRequest.setTaskAttemptId(attemptID);
          getRefreshedProxy(taskAttemptID.getJobID()).failTaskAttempt(failRequest);
        } else {
          killRequest.setTaskAttemptId(attemptID);
          getRefreshedProxy(taskAttemptID.getJobID()).killTaskAttempt(killRequest);
        }
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return true;
  }
}
