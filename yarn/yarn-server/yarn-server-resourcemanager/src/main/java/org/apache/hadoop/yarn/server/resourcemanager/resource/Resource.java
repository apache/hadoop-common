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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Private
@Evolving
public class Resource {
  
  public static final org.apache.hadoop.yarn.Resource NONE = createResource(0);

  public static org.apache.hadoop.yarn.Resource createResource(int memory) {
    org.apache.hadoop.yarn.Resource resource = new org.apache.hadoop.yarn.Resource();
    resource.memory = memory;
    return resource;
  }
  
  public static void addResource(org.apache.hadoop.yarn.Resource lhs, 
      org.apache.hadoop.yarn.Resource rhs) {
    lhs.memory += rhs.memory;
  }
  
  public static void subtractResource(org.apache.hadoop.yarn.Resource lhs, 
      org.apache.hadoop.yarn.Resource rhs) {
    lhs.memory -= rhs.memory;
  }
  
  public static boolean equals(org.apache.hadoop.yarn.Resource lhs,
      org.apache.hadoop.yarn.Resource rhs) {
    return lhs.memory == rhs.memory;
  }

  public static boolean lessThan(org.apache.hadoop.yarn.Resource lhs,
      org.apache.hadoop.yarn.Resource rhs) {
    return lhs.memory < rhs.memory;
  }

  public static boolean lessThanOrEqual(org.apache.hadoop.yarn.Resource lhs,
      org.apache.hadoop.yarn.Resource rhs) {
    return lhs.memory <= rhs.memory;
  }

  public static boolean greaterThan(org.apache.hadoop.yarn.Resource lhs,
      org.apache.hadoop.yarn.Resource rhs) {
    return lhs.memory > rhs.memory;
  }

  public static boolean greaterThanOrEqual(org.apache.hadoop.yarn.Resource lhs,
      org.apache.hadoop.yarn.Resource rhs) {
    return lhs.memory >= rhs.memory;
  }
}
