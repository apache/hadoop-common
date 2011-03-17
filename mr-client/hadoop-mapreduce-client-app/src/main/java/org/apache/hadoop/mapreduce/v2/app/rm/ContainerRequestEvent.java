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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;

public class ContainerRequestEvent extends ContainerAllocatorEvent {
  
  private Priority priority;
  private Resource capability;
  private String[] hosts;
  private String[] racks;

  public ContainerRequestEvent(TaskAttemptID attemptID, 
      Resource capability, int priority,
      String[] hosts, String[] racks) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.priority = new Priority();
    this.priority.priority = priority;
    this.hosts = hosts;
    this.racks = racks;
  }

  public Resource getCapability() {
    return capability;
  }

  public Priority getPriority() {
    return priority;
  }

  public String[] getHosts() {
    return hosts;
  }
  
  public String[] getRacks() {
    return racks;
  }
}