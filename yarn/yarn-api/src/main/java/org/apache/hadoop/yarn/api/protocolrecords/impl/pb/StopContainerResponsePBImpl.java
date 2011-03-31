package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerResponseProto;


    
public class StopContainerResponsePBImpl extends ProtoBase<StopContainerResponseProto> implements StopContainerResponse {
  StopContainerResponseProto proto = StopContainerResponseProto.getDefaultInstance();
  StopContainerResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public StopContainerResponsePBImpl() {
    builder = StopContainerResponseProto.newBuilder();
  }

  public StopContainerResponsePBImpl(StopContainerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public StopContainerResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StopContainerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
