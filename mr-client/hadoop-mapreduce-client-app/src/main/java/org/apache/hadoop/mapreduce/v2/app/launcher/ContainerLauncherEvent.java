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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerToken;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;

public class ContainerLauncherEvent 
    extends AbstractEvent<ContainerLauncher.EventType> {

  private TaskAttemptID taskAttemptID;
  private ContainerID containerID;
  private String containerMgrAddress;
  private ContainerToken containerToken;

  public ContainerLauncherEvent(TaskAttemptID taskAttemptID, 
      ContainerID containerID,
      String containerMgrAddress,
      ContainerToken containerToken,
      ContainerLauncher.EventType type) {
    super(type);
    this.taskAttemptID = taskAttemptID;
    this.containerID = containerID;
    this.containerMgrAddress = containerMgrAddress;
    this.containerToken = containerToken;
  }

  public TaskAttemptID getTaskAttemptID() {
    return this.taskAttemptID;
  }

  public ContainerID getContainerID() {
    return containerID;
  }

  public String getContainerMgrAddress() {
    return containerMgrAddress;
  }

  public ContainerToken getContainerToken() {
    return containerToken;
  }

  @Override
  public String toString() {
    return super.toString() + " for taskAttempt " + taskAttemptID;
  }
}
