/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import java.text.NumberFormat;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskTypeProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class TaskIdPBImpl extends ProtoBase<TaskIdProto> implements TaskId {
  TaskIdProto proto = TaskIdProto.getDefaultInstance();
  TaskIdProto.Builder builder = null;
  boolean viaProto = false;
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }
  
  protected static final NumberFormat jobidFormat = NumberFormat.getInstance();
  static {
    jobidFormat.setGroupingUsed(false);
    jobidFormat.setMinimumIntegerDigits(4);
  }
  
  
  private JobId jobId = null;
  
  
  public TaskIdPBImpl() {
    builder = TaskIdProto.newBuilder(proto);
  }

  public TaskIdPBImpl(TaskIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public TaskIdProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.jobId != null && !((JobIdPBImpl)this.jobId).getProto().equals(builder.getJobId()) ) {
      builder.setJobId(convertToProtoFormat(this.jobId));
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
      builder = TaskIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getId() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }
  @Override
  public JobId getJobId() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasJobId()) {
      return null;
    }
    jobId = convertFromProtoFormat(p.getJobId());
    return jobId;
  }

  @Override
  public void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null)
      builder.clearJobId();
    this.jobId = jobId;
  }
  @Override
  public TaskType getTaskType() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskType()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskType());
  }

  @Override
  public void setTaskType(TaskType taskType) {
    maybeInitBuilder();
    if (taskType == null) {
      builder.clearTaskType();
      return;
    }
    builder.setTaskType(convertToProtoFormat(taskType));
  }

  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

  private TaskTypeProto convertToProtoFormat(TaskType e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private TaskType convertFromProtoFormat(TaskTypeProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

  
  @Override
  public String toString() {
    String jobIdentifier =  (jobId == null) ? "none":
      jobId.getAppId().getClusterTimestamp() + "_" + 
      jobidFormat.format(jobId.getAppId().getId()) + "_" + 
      ((getTaskType() == TaskType.MAP) ? "m":"r") + "_" + idFormat.format(getId());
    return "task_" + jobIdentifier;
  }
}  
