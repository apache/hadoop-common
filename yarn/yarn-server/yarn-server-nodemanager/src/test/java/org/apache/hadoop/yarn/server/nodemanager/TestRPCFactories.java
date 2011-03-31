package org.apache.hadoop.yarn.server.nodemanager;

import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationResponse;
import org.junit.Test;

public class TestRPCFactories {
  
  
  
  @Test
  public void test() {
    testPbServerFactory();
    
    testPbClientFactory();
  }
  
  
  
  private void testPbServerFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    Configuration conf = new Configuration();
    LocalizationProtocol instance = new LocalizationProtocolTestImpl();
    Server server = null;
    try {
      server = RpcServerFactoryPBImpl.get().getServer(LocalizationProtocol.class, instance, addr, conf, null);
      server.start();
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      server.stop();
    }
  }

  
  private void testPbClientFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    System.err.println(addr.getHostName() + addr.getPort());
    Configuration conf = new Configuration();
    LocalizationProtocol instance = new LocalizationProtocolTestImpl();
    Server server = null;
    try {
      server = RpcServerFactoryPBImpl.get().getServer(LocalizationProtocol.class, instance, addr, conf, null);
      server.start();
      System.err.println(server.getListenerAddress());
      System.err.println(NetUtils.getConnectAddress(server));

      LocalizationProtocol client = null;
      try {
        client = (LocalizationProtocol) RpcClientFactoryPBImpl.get().getClient(LocalizationProtocol.class, 1, NetUtils.getConnectAddress(server), conf);
      } catch (YarnException e) {
        e.printStackTrace();
        Assert.fail("Failed to create client");
      }
      
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to create server");
    } finally {
      server.stop();
    }     
  }

  public class LocalizationProtocolTestImpl implements LocalizationProtocol {

    @Override
    public SuccessfulLocalizationResponse successfulLocalization(
        SuccessfulLocalizationRequest request) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FailedLocalizationResponse failedLocalization(
        FailedLocalizationRequest request) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
