package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.CleanupContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.CleanupContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.CleanupContainerRequestProtoOrBuilder;


    
public class CleanupContainerRequestPBImpl extends ProtoBase<CleanupContainerRequestProto> implements CleanupContainerRequest {
  CleanupContainerRequestProto proto = CleanupContainerRequestProto.getDefaultInstance();
  CleanupContainerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  
  
  public CleanupContainerRequestPBImpl() {
    builder = CleanupContainerRequestProto.newBuilder();
  }

  public CleanupContainerRequestPBImpl(CleanupContainerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public CleanupContainerRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CleanupContainerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ContainerId getContainerId() {
    CleanupContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId =  convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) 
      builder.clearContainerId();
    this.containerId = containerId;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }



}  
