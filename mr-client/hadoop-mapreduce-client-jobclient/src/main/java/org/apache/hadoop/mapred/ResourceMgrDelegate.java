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
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;


// TODO: This should be part of something like yarn-client.
public class ResourceMgrDelegate {
  private static final Log LOG = LogFactory.getLog(ResourceMgrDelegate.class);
      
  private Configuration conf;
  ClientRMProtocol applicationsManager;
  private ApplicationId applicationId;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  public ResourceMgrDelegate(Configuration conf) throws UnsupportedFileSystemException {
    this.conf = conf;
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress =
        NetUtils.createSocketAddr(conf.get(
            YarnConfiguration.APPSMANAGER_ADDRESS,
            YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    Configuration appsManagerServerConf = new Configuration(this.conf);
    appsManagerServerConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    applicationsManager =
        (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class,
            rmAddress, appsManagerServerConf);
    LOG.info("Connected to ResourceManager at " + rmAddress);
  }
  
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    return;
  }


  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    return null;
  }


  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    return null;
  }


  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    throw new IOException("Not implemented");
  }


  public QueueInfo[] getChildQueues(String arg0) throws IOException,
      InterruptedException {
    throw new IOException("Not implemented");
  }


  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    GetClusterMetricsRequest request = recordFactory.newRecordInstance(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse response = applicationsManager.getClusterMetrics(request);
    YarnClusterMetrics metrics = response.getClusterMetrics();
    ClusterMetrics oldMetrics = new ClusterMetrics(1, 1, 1, 1, 1, 1, 
        metrics.getNumNodeManagers() * 10, metrics.getNumNodeManagers() * 2, 1,
        metrics.getNumNodeManagers(), 0, 0);
    return oldMetrics;
  }


  public Token<DelegationTokenIdentifier> getDelegationToken(Text arg0)
      throws IOException, InterruptedException {
    throw new IOException("Not Implemented");
  }


  public String getFilesystemName() throws IOException, InterruptedException {
    return FileSystem.get(conf).getUri().toString();
  }

  public JobID getNewJobID() throws IOException, InterruptedException {
    GetNewApplicationIdRequest request = recordFactory.newRecordInstance(GetNewApplicationIdRequest.class);
    applicationId = applicationsManager.getNewApplicationId(request).getApplicationId();
    return TypeConverter.fromYarn(applicationId);
  }

  
  public QueueInfo getQueue(String arg0) throws IOException,
      InterruptedException {
    throw new IOException("Not implemented");
  }


  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    throw new IOException("Not implemented");
  }


  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    throw new IOException("Not implemented");
  }


  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    throw new IOException("Not Implemented");
  }


  public String getStagingAreaDir() throws IOException, InterruptedException {
//    Path path = new Path(MRJobConstants.JOB_SUBMIT_DIR);
    Path path = new Path(conf.get(YARNApplicationConstants.APPS_STAGING_DIR_KEY));
    LOG.info("DEBUG --- getStagingAreaDir: dir=" + path);
    return path.toString();
  }


  public String getSystemDir() throws IOException, InterruptedException {
    Path sysDir = new Path(
        YARNApplicationConstants.JOB_SUBMIT_DIR);
    FileContext.getFileContext(conf).delete(sysDir, true);
    return sysDir.toString();
  }
  

  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return 0;
  }
  
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    return;
  }


  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new IOException("Not implemented");
  }
  
  
  public ApplicationId submitApplication(ApplicationSubmissionContext appContext) 
  throws IOException {
    appContext.setApplicationId(applicationId);
    SubmitApplicationRequest request = recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);
    applicationsManager.submitApplication(request);
    LOG.info("Submitted application " + applicationId + " to ResourceManager");
    return applicationId;
  }

  public ApplicationMaster getApplicationMaster(ApplicationId appId) 
    throws YarnRemoteException {
    GetApplicationMasterRequest request = recordFactory.newRecordInstance(GetApplicationMasterRequest.class);
    request.setApplicationId(appId);
    GetApplicationMasterResponse response = applicationsManager.getApplicationMaster(request);
    ApplicationMaster appMaster = response.getApplicationMaster(); 
    while (appMaster.getState() != ApplicationState.RUNNING &&
        appMaster.getState() != ApplicationState.KILLED && 
        appMaster.getState() != ApplicationState.FAILED && 
        appMaster.getState() != ApplicationState.COMPLETED) {
      appMaster = applicationsManager.getApplicationMaster(request).getApplicationMaster();
      try {
        LOG.info("Waiting for appMaster to start..");
        Thread.sleep(2000);
      } catch(InterruptedException ie) {
        //DO NOTHING
      }
    }
    return appMaster;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }
}
