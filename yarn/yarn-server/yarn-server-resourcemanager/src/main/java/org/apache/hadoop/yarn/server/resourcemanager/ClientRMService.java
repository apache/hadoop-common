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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTracker;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.ClientRMProtocol;
import org.apache.hadoop.yarn.YarnClusterMetrics;

/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements ClientRMProtocol {
  private static final Log LOG = LogFactory.getLog(ClientRMService.class);
  private RMResourceTracker clusterInfo;
  private ApplicationsManager applicationsManager;
  private String clientServiceBindAddress;
  private Server server;
  InetSocketAddress clientBindAddress;
  
  public ClientRMService(ApplicationsManager applicationsManager, 
        RMResourceTracker clusterInfo) {
    super(ClientRMService.class.getName());
    this.clusterInfo = clusterInfo;
    this.applicationsManager = applicationsManager;
  }
  
  @Override
  public void init(Configuration conf) {
    clientServiceBindAddress =
      conf.get(YarnConfiguration.APPSMANAGER_ADDRESS,
          YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
    clientBindAddress =
      NetUtils.createSocketAddr(clientServiceBindAddress);
    super.init(conf);
  }
  
  @Override
  public void start() {
    // All the clients to appsManager are supposed to be authenticated via
    // Kerberos if security is enabled, so no secretManager.
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration clientServerConf = new Configuration(getConfig());
    clientServerConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    this.server =   
      rpc.getServer(ClientRMProtocol.class, this,
            clientBindAddress,
            clientServerConf, null);
    this.server.start();
    super.start();
  }

  @Override
  public ApplicationID getNewApplicationId() throws AvroRemoteException {
    return applicationsManager.getNewApplicationID();
  }

  @Override
  public ApplicationMaster getApplicationMaster(ApplicationID applicationId)
      throws AvroRemoteException {
    return applicationsManager.getApplicationMaster(applicationId);
  }

  @Override
  public Void submitApplication(ApplicationSubmissionContext context)
      throws AvroRemoteException {
    try {
      applicationsManager.submitApplication(context);
    } catch (IOException ie) {
      LOG.info("Exception in submitting application", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    return null;
  }

  @Override
  public Void finishApplication(ApplicationID applicationId)
      throws AvroRemoteException {
    try {
      applicationsManager.finishApplication(applicationId);
    } catch(IOException ie) {
      LOG.info("Error finishing application ", ie);
    }
    return null;
  }

  @Override
  public YarnClusterMetrics getClusterMetrics() throws AvroRemoteException {
    return clusterInfo.getClusterMetrics();
  }
  
  @Override
  public void stop() {
    if (this.server != null) {
        this.server.close();
    }
    super.stop();
  }
}