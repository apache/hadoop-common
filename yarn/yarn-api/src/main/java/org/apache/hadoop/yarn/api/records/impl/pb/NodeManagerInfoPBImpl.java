package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.NodeManagerInfo;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeManagerInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeManagerInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

public class NodeManagerInfoPBImpl extends ProtoBase<NodeManagerInfoProto>
    implements NodeManagerInfo {

  NodeManagerInfoProto proto = NodeManagerInfoProto.getDefaultInstance();
  NodeManagerInfoProto.Builder builder = null;
  boolean viaProto = false;

  Resource used;
  Resource capability;
  
  public NodeManagerInfoPBImpl() {
    builder = NodeManagerInfoProto.newBuilder();
  }
  
  public NodeManagerInfoPBImpl(NodeManagerInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public Resource getCapability() {
    if (this.capability != null) {
      return this.capability;
    }

    NodeManagerInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCapability()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getCapability());
    return this.capability;
  }

  @Override
  public String getNodeName() {
    NodeManagerInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNodeName()) ? p.getNodeName() : null;
  }

  @Override
  public int getNumContainers() {
    NodeManagerInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumContainers()) ? p.getNumContainers() : 0;
  }

  @Override
  public String getRackName() {
    NodeManagerInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRackName()) ? p.getRackName() : null;
  }

  @Override
  public Resource getUsed() {
    if (this.used != null) {
      return this.used;
    }

    NodeManagerInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUsed()) {
      return null;
    }
    this.used = convertFromProtoFormat(p.getUsed());
    return this.used;
  }

  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null)
      builder.clearCapability();
    this.capability = capability;
  }

  @Override
  public void setNodeName(String nodeName) {
    maybeInitBuilder();
    if (nodeName == null) {
      builder.clearNodeName();
      return;
    }
    builder.setNodeName(nodeName);
  }

  @Override
  public void setNumContainers(int numContainers) {
    maybeInitBuilder();
    if (numContainers == 0) {
      builder.clearNumContainers();
      return;
    }
    builder.setNumContainers(numContainers);
  }

  @Override
  public void setRackName(String rackName) {
    maybeInitBuilder();
    if (rackName == null) {
      builder.clearRackName();
      return;
    }
    builder.setRackName(rackName);
  }

  @Override
  public void setUsed(Resource used) {
    maybeInitBuilder();
    if (used == null)
      builder.clearUsed();
    this.used = used;
  }

  @Override
  public NodeManagerInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.used != null
        && !((ResourcePBImpl) this.used).getProto().equals(
            builder.getUsed())) {
      builder.setUsed(convertToProtoFormat(this.used));
    }
    if (this.capability != null
        && !((ResourcePBImpl) this.capability).getProto().equals(
            builder.getCapability())) {
      builder.setCapability(convertToProtoFormat(this.used));
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
      builder = NodeManagerInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }


  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource r) {
    return ((ResourcePBImpl) r).getProto();
  }

}
