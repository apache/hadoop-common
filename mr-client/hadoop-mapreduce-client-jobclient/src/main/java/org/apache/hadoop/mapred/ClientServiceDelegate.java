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
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);

  private Configuration conf;
  private ApplicationID currentAppId;
  private final ResourceMgrDelegate rm;
  private MRClientProtocol realProxy = null;
  private String serviceAddr = "";
  private String serviceHttpAddr = "";

  ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm) {
    this.conf = conf;
    this.rm = rm;
  }

  private MRClientProtocol getProxy(JobID jobId) throws AvroRemoteException {
    return getProxy(TypeConverter.toYarn(jobId).appID, false);
  }

  private MRClientProtocol getRefreshedProxy(JobID jobId) throws AvroRemoteException {
    return getProxy(TypeConverter.toYarn(jobId).appID, true);
  }

  private MRClientProtocol getProxy(ApplicationID appId, 
      boolean forceRefresh) throws AvroRemoteException {
    if (!appId.equals(currentAppId) || forceRefresh) {
      currentAppId = appId;
      refreshProxy();
    }
    return realProxy;
  }

  private void refreshProxy() throws AvroRemoteException {
    ApplicationMaster appMaster = rm.getApplicationMaster(currentAppId);
    if (ApplicationState.COMPLETED.equals(appMaster.state)) {
      serviceAddr = conf.get(YarnMRJobConfig.HS_BIND_ADDRESS,
          YarnMRJobConfig.DEFAULT_HS_BIND_ADDRESS);
      LOG.info("Application state is completed. " +
            "Redirecting to job history server " + serviceAddr);
      //TODO:
      serviceHttpAddr = "";
    } else if (ApplicationState.RUNNING.equals(appMaster.state)){
      serviceAddr = appMaster.host + ":" + appMaster.rpcPort;
      serviceHttpAddr = appMaster.host + ":" + appMaster.httpPort;
      if (UserGroupInformation.isSecurityEnabled()) {
        String clientTokenEncoded = appMaster.clientToken.toString();
        Token<ApplicationTokenIdentifier> clientToken =
            new Token<ApplicationTokenIdentifier>();
        try {
          clientToken.decodeFromUrlString(clientTokenEncoded);
          clientToken.setService(new Text(appMaster.host.toString() + ":"
              + appMaster.rpcPort));
          UserGroupInformation.getCurrentUser().addToken(clientToken);
        } catch (IOException e) {
          throw new YarnException(e);
        }
      }
    } else {
      LOG.warn("Cannot connect to Application with state " + appMaster.state);
      throw new YarnException(
          "Cannot connect to Application with state " + appMaster.state);
    }
    try {
      instantiateProxy(serviceAddr);
    } catch (IOException e) {
      throw new YarnException(e);
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
    org.apache.hadoop.mapreduce.v2.api.JobID jobID = TypeConverter.toYarn(arg0);
    try {
      return TypeConverter.fromYarn(getProxy(arg0).getCounters(jobID));
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failing to contact application master", e);
      try {
        return TypeConverter.fromYarn(getRefreshedProxy(arg0).getCounters(jobID));
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
    org.apache.hadoop.mapreduce.v2.api.JobID jobID = TypeConverter.toYarn(arg0);
    List<org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent> list = null;
    try {
      list = getProxy(arg0).getTaskAttemptCompletionEvents(jobID,
          arg1, arg2);
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        list = getRefreshedProxy(arg0).getTaskAttemptCompletionEvents(jobID,
            arg1, arg2);
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
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
    try {
      list = getProxy(arg0.getJobID()).getDiagnostics(attemptID);
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        list = getRefreshedProxy(arg0.getJobID()).getDiagnostics(attemptID);
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    String[] result = new String[list.size()];
    int i = 0;
    for (CharSequence c : list) {
      result[i++] = c.toString();
    }
    return result;
  }

  public JobStatus getJobStatus(JobID oldJobID) throws YarnRemoteException,
      AvroRemoteException {
    org.apache.hadoop.mapreduce.v2.api.JobID jobId = 
      TypeConverter.toYarn(oldJobID);
    LOG.debug("Getting Job status");
    String stagingDir = conf.get("yarn.apps.stagingDir");
    String jobFile = stagingDir + "/" + jobId.toString();
    JobReport report = null;
    try {
      report = getProxy(oldJobID).getJobReport(jobId);
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch (Exception e) {
      try {
        report = getRefreshedProxy(oldJobID).getJobReport(jobId);
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return TypeConverter.fromYarn(report, jobFile, serviceHttpAddr);
  }

  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
      throws YarnRemoteException, AvroRemoteException {
      List<TaskReport> taskReports = null;
      org.apache.hadoop.mapreduce.v2.api.JobID nJobID = TypeConverter.toYarn(jobID);
      try {
        taskReports = getProxy(jobID).getTaskReports(nJobID, 
            TypeConverter.toYarn(taskType));
      } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      } catch(Exception e) {
        LOG.debug("Failed to contact application master ", e);
        try {
          taskReports = getRefreshedProxy(jobID).getTaskReports(nJobID, 
              TypeConverter.toYarn(taskType));
        } catch(YarnRemoteException yre) {
          LOG.warn(RPCUtil.toString(yre));
          throw yre;
        }
      }
      return TypeConverter.fromYarn
        (taskReports).toArray(new org.apache.hadoop.mapreduce.TaskReport[0]);
  }

  public Void killJob(JobID jobID) throws YarnRemoteException,
      AvroRemoteException {
    org.apache.hadoop.mapreduce.v2.api.JobID  nJobID = TypeConverter.toYarn(jobID);
    try {
      getProxy(jobID).killJob(nJobID);
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        getRefreshedProxy(jobID).killJob(nJobID);
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return null;
  }

  public boolean killTask(TaskAttemptID taskAttemptID, boolean fail)
      throws YarnRemoteException, AvroRemoteException {
    org.apache.hadoop.mapreduce.v2.api.TaskAttemptID attemptID 
      = TypeConverter.toYarn(taskAttemptID);
    try {
      if (fail) {
        getProxy(taskAttemptID.getJobID()).failTaskAttempt(attemptID);
      } else {
        getProxy(taskAttemptID.getJobID()).killTaskAttempt(attemptID);
      }
    } catch(YarnRemoteException yre) {//thrown by remote server, no need to redirect
      LOG.warn(RPCUtil.toString(yre));
      throw yre;
    } catch(Exception e) {
      LOG.debug("Failed to contact application master ", e);
      try {
        if (fail) {
          getRefreshedProxy(taskAttemptID.getJobID()).failTaskAttempt(attemptID);
        } else {
          getRefreshedProxy(taskAttemptID.getJobID()).killTaskAttempt(attemptID);
        }
      } catch(YarnRemoteException yre) {
        LOG.warn(RPCUtil.toString(yre));
        throw yre;
      }
    }
    return true;
  }
}
