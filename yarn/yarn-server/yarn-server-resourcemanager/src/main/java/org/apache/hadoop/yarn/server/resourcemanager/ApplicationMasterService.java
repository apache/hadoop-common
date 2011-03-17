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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.AMRMProtocol;
import org.apache.hadoop.yarn.AMResponse;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.ASMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationMasterHandler;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationTrackerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.AbstractService;

@Private
public class ApplicationMasterService extends AbstractService implements 
AMRMProtocol, EventHandler<ASMEvent<ApplicationTrackerEventType>> {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private ApplicationMasterHandler applicationsManager;
  private YarnScheduler rScheduler;
  private ApplicationTokenSecretManager appTokenManager;
  private InetSocketAddress masterServiceAddress;
  private Server server;
  private Map<ApplicationID, AMResponse> responseMap = 
    new HashMap<ApplicationID, AMResponse>();
  private final AMResponse reboot = new AMResponse();
  private final ASMContext asmContext;
  
  public ApplicationMasterService(ApplicationTokenSecretManager appTokenManager,
      ApplicationMasterHandler applicationsManager, YarnScheduler scheduler, ASMContext asmContext) {
    super(ApplicationMasterService.class.getName());
    this.appTokenManager = appTokenManager;
    this.applicationsManager = applicationsManager;
    this.rScheduler = scheduler;
    this.reboot.reboot = true;
    this.reboot.containers = new ArrayList<Container>();
    this.asmContext = asmContext;
  }

  @Override
  public void init(Configuration conf) {
    String bindAddress =
      conf.get(YarnConfiguration.SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_SCHEDULER_BIND_ADDRESS);
    masterServiceAddress =  NetUtils.createSocketAddr(bindAddress);
    this.asmContext.getDispatcher().register(ApplicationTrackerEventType.class, this);
    super.init(conf);
  }

  public void start() {
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration serverConf = new Configuration(getConfig());
    serverConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        SchedulerSecurityInfo.class, SecurityInfo.class);
    this.server =
      rpc.getServer(AMRMProtocol.class, this, masterServiceAddress,
          serverConf, this.appTokenManager);
    this.server.start();
    super.start();
  }
  @Override
  public Void registerApplicationMaster(ApplicationMaster applicationMaster)
  throws AvroRemoteException {
    try {
      applicationsManager.registerApplicationMaster(applicationMaster);
    } catch(IOException ie) {
      LOG.info("Exception registering application ", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    return null;
  }

  @Override
  public  Void finishApplicationMaster(ApplicationMaster applicationMaster)
  throws AvroRemoteException {
    try {
      applicationsManager.finishApplicationMaster(applicationMaster);
    } catch(IOException ie) {
      LOG.info("Exception finishing application", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    return null;
  }

  @Override
  public AMResponse allocate(ApplicationStatus status,
      List<ResourceRequest> ask, List<Container> release)
  throws AvroRemoteException {
    try {
      /* check if its in cache */
      synchronized(responseMap) {
        AMResponse lastResponse = responseMap.get(status.applicationId);
        if (lastResponse == null) {
          LOG.error("Application doesnt exist in cache " + status.applicationId);
          return reboot;
        }
        if ((status.responseID + 1) == lastResponse.responseId) {
          /* old heartbeat */
          return lastResponse;
        } else if (status.responseID + 1 < lastResponse.responseId) {
          LOG.error("Invalid responseid from application " + status.applicationId);
          return reboot;
        }
        applicationsManager.applicationHeartbeat(status);
        List<Container> containers = rScheduler.allocate(status.applicationId, ask, release);
        AMResponse  response = new AMResponse();
        response.containers = containers;
        response.responseId = lastResponse.responseId + 1;
        responseMap.put(status.applicationId, response);
        return response;
      }
    } catch(IOException ie) {
      LOG.info("Error in allocation for " + status.applicationId, ie);
      throw RPCUtil.getRemoteException(ie);
    }
  } 

  @Override
  public void stop() {
    if (this.server != null) {
      this.server.close();
    }
    super.stop();
  }

  @Override
  public void handle(ASMEvent<ApplicationTrackerEventType> appEvent) {
    ApplicationTrackerEventType event = appEvent.getType();
    ApplicationID id = appEvent.getAppContext().getApplicationID();
    synchronized(responseMap) {
      switch (event) {
      case ADD:
        AMResponse response = new AMResponse();
        response.containers = null;
        response.responseId = 0;
        responseMap.put(id, response);
        break;
      case REMOVE:
        responseMap.remove(id);
        break;
      default: 
        break;
      }
    }
  }
}
