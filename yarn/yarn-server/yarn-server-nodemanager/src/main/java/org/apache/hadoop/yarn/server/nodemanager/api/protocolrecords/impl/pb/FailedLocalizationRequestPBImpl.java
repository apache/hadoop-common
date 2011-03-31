package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnRemoteExceptionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.FailedLocalizationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.FailedLocalizationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationRequest;


    
public class FailedLocalizationRequestPBImpl extends ProtoBase<FailedLocalizationRequestProto> implements FailedLocalizationRequest {
  FailedLocalizationRequestProto proto = FailedLocalizationRequestProto.getDefaultInstance();
  FailedLocalizationRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private LocalResource resource = null;
  private YarnRemoteException exception = null;
  
  
  public FailedLocalizationRequestPBImpl() {
    builder = FailedLocalizationRequestProto.newBuilder();
  }

  public FailedLocalizationRequestPBImpl(FailedLocalizationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FailedLocalizationRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.exception != null) {
      builder.setException(convertToProtoFormat(this.exception));
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
      builder = FailedLocalizationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getUser() {
    FailedLocalizationRequestProtoOrBuilder p = viaProto ? proto : builder;
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
    FailedLocalizationRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public YarnRemoteException getException() {
    FailedLocalizationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.exception != null) {
      return this.exception;
    }
    if (!p.hasException()) {
      return null;
    }
    this.exception = convertFromProtoFormat(p.getException());
    return this.exception;
  }

  @Override
  public void setException(YarnRemoteException exception) {
    maybeInitBuilder();
    if (exception == null) 
      builder.clearException();
    this.exception = exception;
  }

  private LocalResourcePBImpl convertFromProtoFormat(LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private LocalResourceProto convertToProtoFormat(LocalResource t) {
    return ((LocalResourcePBImpl)t).getProto();
  }

  private YarnRemoteExceptionPBImpl convertFromProtoFormat(YarnRemoteExceptionProto p) {
    return new YarnRemoteExceptionPBImpl(p);
  }

  private YarnRemoteExceptionProto convertToProtoFormat(YarnRemoteException t) {
    return ((YarnRemoteExceptionPBImpl)t).getProto();
  }



}  
