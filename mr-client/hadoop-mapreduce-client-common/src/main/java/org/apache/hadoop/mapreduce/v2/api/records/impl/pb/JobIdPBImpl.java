package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;


    
public class JobIdPBImpl extends ProtoBase<JobIdProto> implements JobId {
  JobIdProto proto = JobIdProto.getDefaultInstance();
  JobIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
//  boolean hasLocalAppId = false;
  
  
  public JobIdPBImpl() {
    builder = JobIdProto.newBuilder();
  }

  public JobIdPBImpl(JobIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public JobIdProto getProto() {
  
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null && !((ApplicationIdPBImpl)this.applicationId).getProto().equals(builder.getAppId()))   {
      builder.setAppId(convertToProtoFormat(this.applicationId));
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
      builder = JobIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationId getAppId() {
    JobIdProtoOrBuilder p = viaProto ? proto : builder;
    if (applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasAppId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getAppId());
    return applicationId;
  }

  @Override
  public void setAppId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) {
      builder.clearAppId();
    }
    this.applicationId = appId;
//    builder.setAppId(convertToProtoFormat(appId));
  }
  @Override
  public int getId() {
    JobIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}  
