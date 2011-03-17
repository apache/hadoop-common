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

package org.apache.hadoop.yarn;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.ipc.AvroYarnRPC;
import org.apache.hadoop.yarn.ipc.HadoopYarnRPC;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerManager;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.ContainerStatus;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.YarnRemoteException;
import org.junit.Test;

public class TestRPC {

  private static final String EXCEPTION_MSG = "test error";
  private static final String EXCEPTION_CAUSE = "exception cause";
  
  @Test
  public void testAvroRPC() throws Exception {
    test(AvroYarnRPC.class.getName());
  }

  @Test
  public void testHadoopNativeRPC() throws Exception {
    test(HadoopYarnRPC.class.getName());
  }

  private void test(String rpcClass) throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnRPC.RPC_CLASSNAME, rpcClass);
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManager.class, 
            new DummyContainerManager(), addr, conf, null);
    server.start();
    ContainerManager proxy = (ContainerManager) 
        rpc.getProxy(ContainerManager.class, 
            NetUtils.createSocketAddr("localhost:" + server.getPort()), conf);
    ContainerLaunchContext containerLaunchContext = new ContainerLaunchContext();
    containerLaunchContext.user = "dummy-user";
    containerLaunchContext.id = new ContainerID();
    containerLaunchContext.id.appID = new ApplicationID();
    containerLaunchContext.id.appID.id = 0;
    containerLaunchContext.id.id = 100;  
    containerLaunchContext.env = new HashMap<CharSequence, CharSequence>();
    containerLaunchContext.resource = new Resource();
    containerLaunchContext.command = new ArrayList<CharSequence>();
    proxy.startContainer(containerLaunchContext);
    ContainerStatus status = proxy.getContainerStatus(containerLaunchContext.id);
    
    //test remote exception
    boolean exception = false;
    try {
      proxy.stopContainer(containerLaunchContext.id);
    } catch (YarnRemoteException e) {
      exception = true;
      Assert.assertTrue(EXCEPTION_MSG.equals(e.message.toString()));
      Assert.assertTrue(EXCEPTION_CAUSE.equals(e.cause.message.toString()));
      System.out.println("Test Exception is " + RPCUtil.toString(e));
    }
    Assert.assertTrue(exception);
    
    server.close();
    Assert.assertNotNull(status);
    Assert.assertEquals(ContainerState.RUNNING, status.state.RUNNING);
  }

  public class DummyContainerManager implements ContainerManager {

    private ContainerStatus status = null;

    @Override
    public Void cleanupContainer(ContainerID containerId) 
        throws AvroRemoteException {
      return null;
    }

    @Override
    public ContainerStatus getContainerStatus(ContainerID containerId)
        throws AvroRemoteException {
      return status;
    }

    @Override
    public Void startContainer(ContainerLaunchContext container)
        throws AvroRemoteException {
      status = new ContainerStatus();
      status.state = ContainerState.RUNNING;
      status.containerID = container.id;
      status.exitStatus = 0;
      return null;
    }

    @Override
    public Void stopContainer(ContainerID containerId)
        throws AvroRemoteException {
      Exception e = new Exception(EXCEPTION_MSG, 
          new Exception(EXCEPTION_CAUSE));
      throw RPCUtil.getRemoteException(e);
    }
    
  }
}
