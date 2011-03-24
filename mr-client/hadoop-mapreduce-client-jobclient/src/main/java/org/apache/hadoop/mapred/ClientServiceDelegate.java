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

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.lib.TypeConverter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.YarnRemoteException;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);
  private Configuration conf;
  private ApplicationID appId;
  private final ResourceMgrDelegate rm;
  private MRClientProtocol realProxy = null;
  private String serviceAddr = "";
  private String serviceHttpAddr = "";
  
  ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm, 
      ApplicationID appId) throws AvroRemoteException {
    this.conf = conf;
    this.rm = rm;
    this.appId = appId;
    if (appId != null) {
      refreshProxy();
    }
  }

  private void refreshProxy() throws AvroRemoteException {
    ApplicationMaster appMaster = rm.getApplicationMaster(appId);
    if (ApplicationState.COMPLETED.equals(appMaster.state)) {
      String serviceAddr = conf.get(YarnMRJobConfig.HS_BIND_ADDRESS,
          YarnMRJobConfig.DEFAULT_HS_BIND_ADDRESS);
      LOG.debug("Reconnecting to job history server " + serviceAddr);
    } else {
      /* TODO check to confirm its really launched */
      serviceAddr = appMaster.host + ":" + appMaster.rpcPort;
      serviceHttpAddr = appMaster.host + ":" + appMaster.httpPort;
    }
    try {
      instantiateProxy(serviceAddr);
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  void instantiateProxy(ApplicationID applicationId, ApplicationMaster appMaster)
      throws IOException {
    try {
      this.appId = applicationId;
      LOG.info("Trying to connect to the ApplicationManager of"
          + " application " + applicationId + " running at " + appMaster);
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      serviceAddr = appMaster.host + ":"
          + appMaster.rpcPort;
      serviceHttpAddr = appMaster.host + ":" + appMaster.httpPort;
      if (UserGroupInformation.isSecurityEnabled()) {
        String clientTokenEncoded = appMaster.clientToken.toString();
        Token<ApplicationTokenIdentifier> clientToken = new Token<ApplicationTokenIdentifier>();
        clientToken.decodeFromUrlString(clientTokenEncoded);
        clientToken.setService(new Text(appMaster.host.toString() + ":"
            + appMaster.rpcPort));
        currentUser.addToken(clientToken);
      }
      instantiateProxy(serviceAddr);
      LOG.info("Connection to the ApplicationManager established.");
    } catch (IOException e) {
      throw (new IOException(e));
    }
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
    appId = TypeConverter.toYarn(arg0).appID;
    org.apache.hadoop.mapreduce.v2.api.JobID jobID = TypeConverter.toYarn(arg0);
    if (realProxy == null) refreshProxy();
    try {
      return TypeConverter.fromYarn(realProxy.getCounters(jobID));
    } catch(Exception e) {
      LOG.debug("Failing to contact application master", e);
      refreshProxy();
      return TypeConverter.fromYarn(realProxy.getCounters(jobID));
    }
  }

  public String getJobHistoryDir() throws IOException, InterruptedException {
    //TODO fix this
    return "";
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    appId = TypeConverter.toYarn(arg0).appID;
    if (realProxy == null) refreshProxy();
    
    org.apache.hadoop.mapreduce.v2.api.JobID jobID = TypeConverter.toYarn(arg0);
    List<org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent> list = null;
    try {
      list = realProxy.getTaskAttemptCompletionEvents(jobID,
          arg1, arg2);
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      refreshProxy();
      list = realProxy.getTaskAttemptCompletionEvents(jobID,
          arg1, arg2);
    }
    return TypeConverter.fromYarn(
        list.toArray(new org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent[0]));
  }

  public String[] getTaskDiagnostics(org.apache.hadoop.mapreduce.TaskAttemptID
      arg0)
  throws IOException,
      InterruptedException {
    
    List<CharSequence> list = null;
    org.apache.hadoop.mapreduce.v2.api.TaskAttemptID attemptID = TypeConverter.toYarn(arg0);
    appId = TypeConverter.toYarn(arg0.getJobID()).appID;
    if (realProxy == null) refreshProxy();
  
    try {
      list = realProxy.getDiagnostics(attemptID);
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      refreshProxy();
      list = realProxy.getDiagnostics(attemptID);
    }
    String[] result = new String[list.size()];
    int i = 0;
    for (CharSequence c : list) {
      result[i++] = c.toString();
    }
    return result;
  }

  //this method is here due to package restriction of 
  //TaskReport constructor
  public static org.apache.hadoop.mapred.TaskReport[] fromYarn(
      List<TaskReport> reports) {
    org.apache.hadoop.mapred.TaskReport[] result = 
      new org.apache.hadoop.mapred.TaskReport[reports.size()];
    int i = 0;
    for (TaskReport report : reports) {
      List<CharSequence> diag = report.diagnostics;
      String[] diagnosticArr = new String[diag.size()];
      int j = 0;
      for (CharSequence c : diag) {
        diagnosticArr[j++] = c.toString();
      }
      org.apache.hadoop.mapred.TaskReport oldReport = 
        new org.apache.hadoop.mapred.TaskReport(
            TypeConverter.fromYarn(report.id), report.progress, 
            report.state.toString(),
            diagnosticArr, TypeConverter.fromYarn(report.state), 
          report.startTime, report.finishTime,
          new org.apache.hadoop.mapred.Counters(
              TypeConverter.fromYarn(report.counters)));
      result[i++] = oldReport;
    }
    return result;
  }

 
  public JobReport getJobReport(org.apache.hadoop.mapreduce.v2.api.JobID jobID)
      throws AvroRemoteException, YarnRemoteException {
    appId = jobID.appID;
    if (realProxy == null) refreshProxy();
    
    try {
      return realProxy.getJobReport(jobID);
    } catch (Exception e) {
      refreshProxy();
      return realProxy.getJobReport(jobID);
    }
  }

  public JobStatus getJobStatus(org.apache.hadoop.mapreduce.v2.api.JobID jobId) 
      throws AvroRemoteException, YarnRemoteException {
    appId = jobId.appID;
    if (realProxy == null) refreshProxy();
    String trackingUrl = serviceAddr;
    String stagingDir = conf.get("yarn.apps.stagingDir");
    String jobFile = stagingDir + "/" + jobId.toString();
    return TypeConverter.fromYarn(getJobReport(jobId), jobFile, serviceHttpAddr);
  }
  

  public JobStatus getJobStatus(JobID jobID) throws YarnRemoteException,
      AvroRemoteException {
    return getJobStatus(TypeConverter.toYarn(jobID));
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
      throws YarnRemoteException, AvroRemoteException {
      List<TaskReport> taskReports = null;
      org.apache.hadoop.mapreduce.v2.api.JobID nJobID = TypeConverter.toYarn(jobID);
      appId = nJobID.appID;
      if (realProxy == null) refreshProxy();
    
      try {
        taskReports = realProxy.getTaskReports(nJobID, 
            TypeConverter.toYarn(taskType));
      } catch(Exception e) {
        LOG.debug("Failed to contact application master ", e);
        refreshProxy();
        taskReports = realProxy.getTaskReports(nJobID, 
            TypeConverter.toYarn(taskType));
      }
      return (org.apache.hadoop.mapreduce.TaskReport[])TypeConverter.fromYarn
        (taskReports).toArray();
  }

  public Void killJob(JobID jobID) throws YarnRemoteException,
      AvroRemoteException {
    org.apache.hadoop.mapreduce.v2.api.JobID  nJobID = TypeConverter.toYarn(jobID);
    appId = nJobID.appID;
    if (realProxy == null) refreshProxy();
    
    try {
      realProxy.killJob(nJobID);
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      refreshProxy();
      realProxy.killJob(nJobID);
    }
    return null;
  }

  public boolean killTask(TaskAttemptID taskAttemptID, boolean killed)
      throws YarnRemoteException, AvroRemoteException {
    org.apache.hadoop.mapreduce.v2.api.TaskAttemptID attemptID 
      = TypeConverter.toYarn(taskAttemptID);
    appId = attemptID.taskID.jobID.appID;
    if (realProxy == null) refreshProxy();
    
    try {
      realProxy.killTaskAttempt(attemptID);
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      refreshProxy();
      realProxy.killTaskAttempt(attemptID);
    }
    return true;
  }
}
