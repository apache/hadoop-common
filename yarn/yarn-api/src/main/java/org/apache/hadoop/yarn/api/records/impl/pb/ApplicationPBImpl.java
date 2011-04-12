package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.Application;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ApplicationPBImpl extends ProtoBase<ApplicationProto> 
implements Application {
  ApplicationProto proto = ApplicationProto.getDefaultInstance();
  ApplicationProto.Builder builder = null;
  boolean viaProto = false;

  ApplicationId applicationId;
  ApplicationStatus status;

  public ApplicationPBImpl() {
    builder = ApplicationProto.newBuilder();
  }
  
  public ApplicationPBImpl(ApplicationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }

    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStatus()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }
  
  @Override
  public String getMasterHost() {
    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMasterHost()) {
      return null;
    }
    return (p.getMasterHost());
  }

  @Override
  public int getMasterPort() {
    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMasterPort()) {
      return -1;
    }
    return p.getMasterPort();
  }

  @Override
  public String getName() {
    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return p.getName();
  }

  @Override
  public String getQueue() {
    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return p.getQueue();
  }

  @Override
  public ApplicationState getState() {
    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public ApplicationStatus getStatus() {
    if (this.status != null) {
      return this.status;
    }

    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStatus()) {
      return null;
    }
    this.status = convertFromProtoFormat(p.getStatus());
    return this.status;
  }

  @Override
  public String getUser() {
    ApplicationProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return p.getUser();
  }
  
  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearStatus();
    this.applicationId = applicationId;
  }

  @Override
  public void setMasterHost(String masterHost) {
    maybeInitBuilder();
    if (masterHost == null) {
      builder.clearMasterHost();
      return;
    }
    builder.setMasterHost((masterHost));
  }

  @Override
  public void setMasterPort(int masterPort) {
    maybeInitBuilder();
    if (masterPort == -1) {
      builder.clearMasterPort();
      return;
    }
    builder.setMasterPort(masterPort);
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);
  }

  @Override
  public void setState(ApplicationState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }

  @Override
  public void setStatus(ApplicationStatus status) {
    maybeInitBuilder();
    if (status == null)
      builder.clearStatus();
    this.status = status;
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
  public ApplicationProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
            builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.status != null
        && !((ApplicationStatusPBImpl) this.status).getProto().equals(
            builder.getStatus())) {
      builder.setStatus(convertToProtoFormat(this.status));
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
      builder = ApplicationProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private ApplicationStatusPBImpl convertFromProtoFormat(
      ApplicationStatusProto p) {
    return new ApplicationStatusPBImpl(p);
  }

  private ApplicationStatusProto convertToProtoFormat(ApplicationStatus t) {
    return ((ApplicationStatusPBImpl) t).getProto();
  }

  private ApplicationState convertFromProtoFormat(ApplicationStateProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private ApplicationStateProto convertToProtoFormat(ApplicationState s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }

}
