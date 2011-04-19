package org.apache.hadoop.yarn.server.resourcemanager.api.impl.pb.client;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;
import org.apache.hadoop.yarn.proto.RMAdminProtocol.RMAdminProtocolService;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.server.resourcemanager.api.RMAdminProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshQueuesRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb.RefreshQueuesResponsePBImpl;

import com.google.protobuf.ServiceException;


public class RMAdminProtocolPBClientImpl implements RMAdminProtocol {

  private RMAdminProtocolService.BlockingInterface proxy;
  
  public RMAdminProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, 
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, RMAdminProtocolService.BlockingInterface.class, 
        ProtoOverHadoopRpcEngine.class);
    proxy = (RMAdminProtocolService.BlockingInterface)RPC.getProxy(
        RMAdminProtocolService.BlockingInterface.class, clientVersion, addr, conf);
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws YarnRemoteException {
    RefreshQueuesRequestProto requestProto = 
      ((RefreshQueuesRequestPBImpl)request).getProto();
    try {
      return new RefreshQueuesResponsePBImpl(
          proxy.refreshQueues(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  
}
