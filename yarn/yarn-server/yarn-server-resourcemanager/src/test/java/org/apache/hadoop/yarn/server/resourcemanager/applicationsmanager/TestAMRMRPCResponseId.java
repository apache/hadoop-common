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
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.ASMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.AMResponse;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.ResourceRequest;
import org.junit.After;
import org.junit.Before;

public class TestAMRMRPCResponseId extends TestCase {
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
    public List<Container> allocate(ApplicationID applicationId,
        List<ResourceRequest> ask, List<Container> release) throws IOException {
      return null;
    }
    @Override
    public void addApplication(ApplicationID applicationId, String user,
        String queue, Priority priority) throws IOException {
    }
    @Override
    public void removeApplication(ApplicationID applicationId)
        throws IOException {
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
    ApplicationID applicationID = applicationsManager.getNewApplicationID();
    ApplicationSubmissionContext context = new ApplicationSubmissionContext();
    context.applicationId = applicationID;
    applicationsManager.submitApplication(context);
    ApplicationMaster applicationMaster = new ApplicationMaster();
    applicationMaster.applicationId = applicationID;
    applicationMaster.status = new ApplicationStatus();
    amService.registerApplicationMaster(applicationMaster);
    ApplicationStatus status = new ApplicationStatus();
    status.applicationId = applicationID;
    AMResponse response = amService.allocate(status, null, null);
    assertTrue(response.responseId == 1);
    assertFalse(response.reboot);
    status.responseID = response.responseId;
    response = amService.allocate(status, null, null);
    assertTrue(response.responseId == 2);
    /* try resending */
    response = amService.allocate(status, null, null);
    assertTrue(response.responseId == 2);
    
    /** try sending old **/
    status.responseID = 0;
    response = amService.allocate(status, null, null);
    assertTrue(response.reboot);
  }
}