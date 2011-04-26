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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.ASMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.junit.After;
import org.junit.Before;

public class TestAMRMRPCResponseId extends TestCase {
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  ApplicationMasterService amService = null;
  ApplicationTokenSecretManager appTokenManager = new ApplicationTokenSecretManager();
  DummyApplicationsManager applicationsManager;
  DummyScheduler scheduler;

  private ASMContext context;
  private class DummyApplicationsManager extends ApplicationsManagerImpl {
    public DummyApplicationsManager(
        ApplicationTokenSecretManager applicationTokenSecretManager,
        YarnScheduler scheduler, ASMContext asmContext) {
      super(applicationTokenSecretManager, scheduler, asmContext);      
    }
    @Override
    public void registerApplicationMaster(ApplicationMaster applicationMaster)
    throws IOException {
    }
    @Override
    public void applicationHeartbeat(ApplicationStatus status)
    throws IOException {      
    }
    @Override
    public void finishApplicationMaster(ApplicationMaster applicationMaster)
    throws IOException {  
    }
  }
  
  
  private class DummyScheduler implements YarnScheduler {
    @Override
    public List<Container> allocate(ApplicationId applicationId,
        List<ResourceRequest> ask, List<Container> release) throws IOException {
      return null;
    }

    @Override
    public void addApplication(ApplicationId applicationId, String user,
        String queue, Priority priority) throws IOException {
    }

    @Override
    public void removeApplication(ApplicationId applicationId)
        throws IOException {
    }

    @Override
    public QueueInfo getQueueInfo(String queueName,
        boolean includeApplications, boolean includeChildQueues,
        boolean recursive) throws IOException {
      return null;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
      return null;
    }
  }
  
  @Before
  public void setUp() {
    context = new ResourceManager.ASMContextImpl();
    scheduler = new DummyScheduler();
    applicationsManager = new DummyApplicationsManager(new 
        ApplicationTokenSecretManager(), scheduler, context);
    amService = new ApplicationMasterService(
        appTokenManager, applicationsManager, scheduler, context);
    Configuration conf = new Configuration();
    applicationsManager.init(conf);
    amService.init(conf);
  }
  
  @After
  public void tearDown() {
    
  }
  
  public void testARRMResponseId() throws Exception {
    ApplicationId applicationID = applicationsManager.getNewApplicationID();
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(applicationID);
    applicationsManager.submitApplication(context);
    ApplicationMaster applicationMaster = recordFactory.newRecordInstance(ApplicationMaster.class);
    applicationMaster.setApplicationId(applicationID);
    applicationMaster.setStatus(recordFactory.newRecordInstance(ApplicationStatus.class));
    RegisterApplicationMasterRequest request = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
    request.setApplicationMaster(applicationMaster);
    amService.registerApplicationMaster(request);
    ApplicationStatus status = recordFactory.newRecordInstance(ApplicationStatus.class);
    status.setApplicationId(applicationID);
    
    AllocateRequest allocateRequest = recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationStatus(status);
    AMResponse response = amService.allocate(allocateRequest).getAMResponse();
    assertTrue(response.getResponseId() == 1);
    assertFalse(response.getReboot());
    status.setResponseId(response.getResponseId());
    
    allocateRequest.setApplicationStatus(status);
    response = amService.allocate(allocateRequest).getAMResponse();
    assertTrue(response.getResponseId() == 2);
    /* try resending */
    response = amService.allocate(allocateRequest).getAMResponse();
    assertTrue(response.getResponseId() == 2);
    
    /** try sending old **/
    status.setResponseId(0);
    allocateRequest.setApplicationStatus(status);
    response = amService.allocate(allocateRequest).getAMResponse();
    assertTrue(response.getReboot());
  }
}
