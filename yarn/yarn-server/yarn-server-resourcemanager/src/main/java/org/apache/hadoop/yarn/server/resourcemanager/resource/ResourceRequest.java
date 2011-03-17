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

import org.apache.hadoop.yarn.Priority;

public class ResourceRequest {
  
  public static org.apache.hadoop.yarn.ResourceRequest create(
      Priority priority, CharSequence hostName, 
      org.apache.hadoop.yarn.Resource capability, int numContainers) {
    org.apache.hadoop.yarn.ResourceRequest request = 
      new org.apache.hadoop.yarn.ResourceRequest();
    request.priority = priority;
    request.hostName = hostName;
    request.capability = capability;
    request.numContainers = numContainers;
    return request;
  }
  
  public static org.apache.hadoop.yarn.ResourceRequest create(
      org.apache.hadoop.yarn.ResourceRequest r) {
    org.apache.hadoop.yarn.ResourceRequest request = 
      new org.apache.hadoop.yarn.ResourceRequest();
    request.priority = r.priority;
    request.hostName = r.hostName;
    request.capability = r.capability;
    request.numContainers = r.numContainers;
    return request;
  }
  
  public static class Comparator 
  implements java.util.Comparator<org.apache.hadoop.yarn.ResourceRequest> {
    @Override
    public int compare(org.apache.hadoop.yarn.ResourceRequest r1,
        org.apache.hadoop.yarn.ResourceRequest r2) {
      
      // Compare priority, host and capability
      int ret = r1.priority.compareTo(r2.priority);
      if (ret == 0) {
        String h1 = r1.hostName.toString();
        String h2 = r2.hostName.toString();
        ret = h1.compareTo(h2);
      }
      if (ret == 0) {
        ret = r1.capability.compareTo(r2.capability);
      }
      return ret;
    }
  }
}
