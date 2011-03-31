package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.CleanupContainerResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.CleanupContainerResponseProto;


    
public class CleanupContainerResponsePBImpl extends ProtoBase<CleanupContainerResponseProto> implements CleanupContainerResponse {
  CleanupContainerResponseProto proto = CleanupContainerResponseProto.getDefaultInstance();
  CleanupContainerResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public CleanupContainerResponsePBImpl() {
    builder = CleanupContainerResponseProto.newBuilder();
  }

  public CleanupContainerResponsePBImpl(CleanupContainerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public CleanupContainerResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CleanupContainerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
