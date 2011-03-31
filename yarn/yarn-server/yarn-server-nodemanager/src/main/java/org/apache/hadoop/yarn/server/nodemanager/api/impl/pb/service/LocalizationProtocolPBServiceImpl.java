package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.service;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.LocalizationProtocol.LocalizationProtocolService.BlockingInterface;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.FailedLocalizationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.FailedLocalizationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.SuccessfulLocalizationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.SuccessfulLocalizationResponseProto;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.FailedLocalizationRequestPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.FailedLocalizationResponsePBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.SuccessfulLocalizationRequestPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.SuccessfulLocalizationResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class LocalizationProtocolPBServiceImpl implements BlockingInterface {

  private LocalizationProtocol real;
  
  public LocalizationProtocolPBServiceImpl(LocalizationProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public SuccessfulLocalizationResponseProto successfulLocalization(
      RpcController controller, SuccessfulLocalizationRequestProto proto)
      throws ServiceException {
    SuccessfulLocalizationRequestPBImpl request = new SuccessfulLocalizationRequestPBImpl(proto);
    try {
      SuccessfulLocalizationResponse response = real.successfulLocalization(request);
      return ((SuccessfulLocalizationResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FailedLocalizationResponseProto failedLocalization(
      RpcController controller, FailedLocalizationRequestProto proto)
      throws ServiceException {
    FailedLocalizationRequestPBImpl request = new FailedLocalizationRequestPBImpl(proto);
    try {
      FailedLocalizationResponse response = real.failedLocalization(request);
      return ((FailedLocalizationResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

}
