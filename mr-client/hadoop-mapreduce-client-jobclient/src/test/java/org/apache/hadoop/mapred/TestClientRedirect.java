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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.ApplicationsManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.ClientRMProtocol;
import org.apache.hadoop.yarn.YarnClusterMetrics;
import org.apache.hadoop.yarn.YarnRemoteException;
import org.apache.hadoop.mapreduce.v2.api.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.Counters;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.JobReport;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.TaskReport;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.junit.Test;

public class TestClientRedirect {

  private static final Log LOG = LogFactory.getLog(TestClientRedirect.class);
  private static final String RMADDRESS = "0.0.0.0:8054";
  private static final String AMHOSTADDRESS = "0.0.0.0:10020";
  private static final String HSHOSTADDRESS = "0.0.0.0:10021";
  private static final int HSPORT = 10020;
  private volatile boolean amContact = false; 
  private volatile boolean hsContact = false;
  private volatile boolean amRunning = false;
 
  @Test
  public void testRedirect() throws Exception {
    
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.APPSMANAGER_ADDRESS, RMADDRESS);
    conf.set(YarnMRJobConfig.HS_BIND_ADDRESS, HSHOSTADDRESS);
    RMService rmService = new RMService("test");
    rmService.init(conf);
    rmService.start();
  
    AMService amService = new AMService();
    amService.init(conf);
    amService.start(conf);
    amRunning = true;

    HistoryService historyService = new HistoryService();
    historyService.init(conf);
    historyService.start(conf);
  
    LOG.info("services started");
    YARNRunner yarnRunner = new YARNRunner(conf);
    Throwable t = null;
    org.apache.hadoop.mapreduce.JobID jobID =
      new org.apache.hadoop.mapred.JobID("201103121733", 1);
    yarnRunner.getJobCounters(jobID);
    Assert.assertTrue(amContact);
    
    //bring down the AM service
    amService.stop();
    amRunning = false;
    
    yarnRunner.getJobCounters(jobID);
    Assert.assertTrue(hsContact);
    
    rmService.stop();
    historyService.stop();
  }

  class RMService extends AbstractService implements ClientRMProtocol {
    private ApplicationsManager applicationsManager;
    private String clientServiceBindAddress;
    InetSocketAddress clientBindAddress;
    private Server server;

    public RMService(String name) {
      super(name);
    }

    @Override
    public void init(Configuration conf) {
      clientServiceBindAddress = RMADDRESS;
      /*
      clientServiceBindAddress = conf.get(
          YarnConfiguration.APPSMANAGER_ADDRESS,
          YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
          */
      clientBindAddress = NetUtils.createSocketAddr(clientServiceBindAddress);
      super.init(conf);
    }

    @Override
    public void start() {
      // All the clients to appsManager are supposed to be authenticated via
      // Kerberos if security is enabled, so no secretManager.
      YarnRPC rpc = YarnRPC.create(getConfig());
      Configuration clientServerConf = new Configuration(getConfig());
      this.server = rpc.getServer(ClientRMProtocol.class, this,
          clientBindAddress, clientServerConf, null);
      this.server.start();
      super.start();
    }

    @Override
    public ApplicationID getNewApplicationId() throws AvroRemoteException {
      return null;
    }

    @Override
    public ApplicationMaster getApplicationMaster(ApplicationID applicationId)
        throws AvroRemoteException {
      ApplicationMaster master = new ApplicationMaster();
      master.applicationId = applicationId;
      master.status = new ApplicationStatus();
      master.status.applicationId = applicationId;
      if (amRunning) {
        master.state = ApplicationState.RUNNING;
      } else {
        master.state = ApplicationState.COMPLETED;
      }
      String[] split = AMHOSTADDRESS.split(":");
      master.host = split[0];
      master.rpcPort = Integer.parseInt(split[1]);
      return master;
  }

    @Override
    public Void submitApplication(ApplicationSubmissionContext context)
        throws AvroRemoteException {
      throw new AvroRemoteException("Test");
    }

    @Override
    public Void finishApplication(ApplicationID applicationId)
        throws AvroRemoteException {
      return null;
    }

    @Override
    public YarnClusterMetrics getClusterMetrics() throws AvroRemoteException {
      return null;
    }
  }

  class HistoryService extends AMService {
    public HistoryService() {
      super(HSHOSTADDRESS);
    }

    @Override
    public Counters getCounters(JobID jobID) throws AvroRemoteException,
      YarnRemoteException {
      hsContact = true;
      Counters counters = new Counters();
      counters.groups = new HashMap<CharSequence, CounterGroup>();
      return counters;
   }
  }

  class AMService extends AbstractService 
      implements MRClientProtocol {
    private InetSocketAddress bindAddress;
    private Server server;
    private final String hostAddress;
    public AMService() {
      this(AMHOSTADDRESS);
    }
    
    public AMService(String hostAddress) {
      super("TestClientService");
      this.hostAddress = hostAddress;
    }

    public void start(Configuration conf) {
      YarnRPC rpc = YarnRPC.create(conf);
      //TODO : use fixed port ??
      InetSocketAddress address = NetUtils.createSocketAddr(hostAddress);
      InetAddress hostNameResolved = null;
      try {
        address.getAddress();
        hostNameResolved = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        throw new YarnException(e);
      }

      server =
          rpc.getServer(MRClientProtocol.class, this, address,
              conf, null);
      server.start();
      this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress()
            + ":" + server.getPort());
       super.start();
    }

    public void stop() {
      server.close();
      super.stop();
    }

    @Override
    public Counters getCounters(JobID jobID) throws AvroRemoteException,
      YarnRemoteException {
      amContact = true;
      Counters counters = new Counters();
      counters.groups = new HashMap<CharSequence, CounterGroup>();
      return counters;
   }

    @Override
    public List<CharSequence> getDiagnostics(
        org.apache.hadoop.mapreduce.v2.api.TaskAttemptID taskAttemptID)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }

    @Override
    public JobReport getJobReport(JobID jobID) throws AvroRemoteException,
        YarnRemoteException {
      return null;
    }

    @Override
    public List<TaskAttemptCompletionEvent> getTaskAttemptCompletionEvents(
        JobID jobID, int fromEventId, int maxEvents)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }

    @Override
    public TaskAttemptReport getTaskAttemptReport(
        org.apache.hadoop.mapreduce.v2.api.TaskAttemptID taskAttemptID)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }

    @Override
    public TaskReport getTaskReport(org.apache.hadoop.mapreduce.v2.api.TaskID taskID)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }

    @Override
    public List<TaskReport> getTaskReports(JobID jobID,
        org.apache.hadoop.mapreduce.v2.api.TaskType taskType)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }

    @Override
    public Void killJob(JobID jobID) throws AvroRemoteException,
        YarnRemoteException {
      return null;
    }

    @Override
    public Void killTask(org.apache.hadoop.mapreduce.v2.api.TaskID taskID)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }

    @Override
    public Void killTaskAttempt(
        org.apache.hadoop.mapreduce.v2.api.TaskAttemptID taskAttemptID)
        throws AvroRemoteException, YarnRemoteException {
      return null;
    }
  }
}
