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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;



public class TaskAttemptContainerAssignedEvent extends TaskAttemptEvent {

  private ContainerId containerID;
  private String containerManagerAddress;
  private ContainerToken containerToken;

  public TaskAttemptContainerAssignedEvent(TaskAttemptId id,
      ContainerId containerID, String containerManagerAddress,
      ContainerToken containerToken) {
    super(id, TaskAttemptEventType.TA_ASSIGNED);
    this.containerID = containerID;
    this.containerManagerAddress = containerManagerAddress;
    this.containerToken = containerToken;
  }

  public ContainerId getContainerID() {
    return this.containerID;
  }

  public String getContainerManagerAddress() {
    return this.containerManagerAddress;
  }

  public ContainerToken getContainerToken() {
    return this.containerToken;
  }
}
