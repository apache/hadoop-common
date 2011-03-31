package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.SuccessfulLocalizationResponseProto;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationResponse;


    
public class SuccessfulLocalizationResponsePBImpl extends ProtoBase<SuccessfulLocalizationResponseProto> implements SuccessfulLocalizationResponse {
  SuccessfulLocalizationResponseProto proto = SuccessfulLocalizationResponseProto.getDefaultInstance();
  SuccessfulLocalizationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public SuccessfulLocalizationResponsePBImpl() {
    builder = SuccessfulLocalizationResponseProto.newBuilder();
  }

  public SuccessfulLocalizationResponsePBImpl(SuccessfulLocalizationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public SuccessfulLocalizationResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SuccessfulLocalizationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
