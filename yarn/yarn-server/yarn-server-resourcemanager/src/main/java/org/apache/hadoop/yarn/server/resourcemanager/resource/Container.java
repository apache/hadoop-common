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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerState;
import org.apache.hadoop.yarn.Resource;

public class Container {
  

  private static ContainerID getNewContainerId(ApplicationID applicationId, 
      int containerId) {
    ContainerID id = new ContainerID();
    id.appID = applicationId;
    id.id = containerId;
    return id;
  }
  
  public static org.apache.hadoop.yarn.Container create(
      org.apache.hadoop.yarn.Container c) {
    org.apache.hadoop.yarn.Container container = new org.apache.hadoop.yarn.Container();
    container.id = c.id;
    container.hostName = c.hostName;
    container.resource = c.resource;
    container.state = c.state;
    return container;
  }

  public static org.apache.hadoop.yarn.Container create(
      ApplicationID applicationId, int containerId, 
      String hostName, Resource resource) {
    ContainerID containerID = getNewContainerId(applicationId, containerId);
    return create(containerID, hostName, resource);
  }

  public static org.apache.hadoop.yarn.Container create(
      ContainerID containerId,
      String hostName, Resource resource) {
    org.apache.hadoop.yarn.Container container = new org.apache.hadoop.yarn.Container();
    container.id = containerId;
    container.hostName = hostName;
    container.resource = resource;
    container.state = ContainerState.INTIALIZING;
    return container;
  }
  
  public static class Comparator 
  implements java.util.Comparator<org.apache.hadoop.yarn.Container> {

    @Override
    public int compare(org.apache.hadoop.yarn.Container c1,
        org.apache.hadoop.yarn.Container c2) {
      return c1.id.compareTo(c2.id);
    }
  }
}
