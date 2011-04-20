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

import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class Container {
  
  public static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  public static org.apache.hadoop.yarn.api.records.Container create(
      org.apache.hadoop.yarn.api.records.Container c) {
    org.apache.hadoop.yarn.api.records.Container container = recordFactory.newRecordInstance(org.apache.hadoop.yarn.api.records.Container.class);
    container.setId(c.getId());
    container.setHostName(c.getHostName());
    container.setResource(c.getResource());
    container.setState(c.getState());
    return container;
  }

  public static org.apache.hadoop.yarn.api.records.Container create(
      RecordFactory recordFactory, ApplicationId applicationId,
      int containerId, String hostName, Resource resource) {
    ContainerId containerID =
        BuilderUtils
            .newContainerId(recordFactory, applicationId, containerId);
    return create(containerID, hostName, resource);
  }

  public static org.apache.hadoop.yarn.api.records.Container create(
      ContainerId containerId,
      String hostName, Resource resource) {
    org.apache.hadoop.yarn.api.records.Container container = recordFactory.newRecordInstance(org.apache.hadoop.yarn.api.records.Container.class);
    container.setId(containerId);
    container.setHostName(hostName);
    container.setResource(resource);
    container.setState(ContainerState.INITIALIZING);
    return container;
  }
  
  public static class ContainerComparator 
  implements java.util.Comparator<org.apache.hadoop.yarn.api.records.Container> {

    @Override
    public int compare(org.apache.hadoop.yarn.api.records.Container c1,
        org.apache.hadoop.yarn.api.records.Container c2) {
      return c1.compareTo(c2);
    }
  }
}
