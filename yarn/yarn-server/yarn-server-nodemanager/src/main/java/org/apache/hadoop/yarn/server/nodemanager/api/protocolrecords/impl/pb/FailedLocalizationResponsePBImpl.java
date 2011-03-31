package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.FailedLocalizationResponseProto;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationResponse;


    
public class FailedLocalizationResponsePBImpl extends ProtoBase<FailedLocalizationResponseProto> implements FailedLocalizationResponse {
  FailedLocalizationResponseProto proto = FailedLocalizationResponseProto.getDefaultInstance();
  FailedLocalizationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public FailedLocalizationResponsePBImpl() {
    builder = FailedLocalizationResponseProto.newBuilder();
  }

  public FailedLocalizationResponsePBImpl(FailedLocalizationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FailedLocalizationResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FailedLocalizationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
