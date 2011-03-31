package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.client;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;
import org.apache.hadoop.yarn.proto.LocalizationProtocol.LocalizationProtocolService;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.FailedLocalizationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.SuccessfulLocalizationRequestProto;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.FailedLocalizationRequestPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.FailedLocalizationResponsePBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.SuccessfulLocalizationRequestPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.SuccessfulLocalizationResponsePBImpl;

import com.google.protobuf.ServiceException;

public class LocalizationProtocolPBClientImpl implements LocalizationProtocol {

  private LocalizationProtocolService.BlockingInterface proxy;
  
  public LocalizationProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, LocalizationProtocolService.BlockingInterface.class, ProtoOverHadoopRpcEngine.class);
    proxy = (LocalizationProtocolService.BlockingInterface)RPC.getProxy(
        LocalizationProtocolService.BlockingInterface.class, clientVersion, addr, conf);
  }
  
  @Override
  public SuccessfulLocalizationResponse successfulLocalization(
      SuccessfulLocalizationRequest request) throws YarnRemoteException {
    SuccessfulLocalizationRequestProto requestProto = ((SuccessfulLocalizationRequestPBImpl)request).getProto();
    try {
      return new SuccessfulLocalizationResponsePBImpl(proxy.successfulLocalization(null, requestProto));
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

  @Override
  public FailedLocalizationResponse failedLocalization(
      FailedLocalizationRequest request) throws YarnRemoteException {
    FailedLocalizationRequestProto requestProto = ((FailedLocalizationRequestPBImpl)request).getProto();
    try {
      return new FailedLocalizationResponsePBImpl(proxy.failedLocalization(null, requestProto));
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
