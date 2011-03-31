package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.URLPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.SuccessfulLocalizationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.SuccessfulLocalizationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationRequest;


    
public class SuccessfulLocalizationRequestPBImpl extends ProtoBase<SuccessfulLocalizationRequestProto> implements SuccessfulLocalizationRequest {
  SuccessfulLocalizationRequestProto proto = SuccessfulLocalizationRequestProto.getDefaultInstance();
  SuccessfulLocalizationRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private LocalResource resource = null;
  private URL path = null;
  
  
  public SuccessfulLocalizationRequestPBImpl() {
    builder = SuccessfulLocalizationRequestProto.newBuilder();
  }

  public SuccessfulLocalizationRequestPBImpl(SuccessfulLocalizationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public SuccessfulLocalizationRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.path != null) {
      builder.setPath(convertToProtoFormat(this.path));
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
      builder = SuccessfulLocalizationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getUser() {
    SuccessfulLocalizationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return (p.getUser());
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser((user));
  }
  @Override
  public LocalResource getResource() {
    SuccessfulLocalizationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(LocalResource resource) {
    maybeInitBuilder();
    if (resource == null) 
      builder.clearResource();
    this.resource = resource;
  }
  @Override
  public URL getPath() {
    SuccessfulLocalizationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.path != null) {
      return this.path;
    }
    if (!p.hasPath()) {
      return null;
    }
    this.path = convertFromProtoFormat(p.getPath());
    return this.path;
  }

  @Override
  public void setPath(URL path) {
    maybeInitBuilder();
    if (path == null) 
      builder.clearPath();
    this.path = path;
  }

  private LocalResourcePBImpl convertFromProtoFormat(LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private LocalResourceProto convertToProtoFormat(LocalResource t) {
    return ((LocalResourcePBImpl)t).getProto();
  }

  private URLPBImpl convertFromProtoFormat(URLProto p) {
    return new URLPBImpl(p);
  }

  private URLProto convertToProtoFormat(URL t) {
    return ((URLPBImpl)t).getProto();
  }



}  
