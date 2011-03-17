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

public class ApplicationID {
  
  public static org.apache.hadoop.yarn.ApplicationID create(long clusterTimeStamp,
      int id) {
    org.apache.hadoop.yarn.ApplicationID applicationId =
        new org.apache.hadoop.yarn.ApplicationID();
    applicationId.id = id;
    applicationId.clusterTimeStamp = clusterTimeStamp;
    return applicationId;
  }
  
  public static org.apache.hadoop.yarn.ApplicationID convert(long clustertimestamp,
      CharSequence id) {
    org.apache.hadoop.yarn.ApplicationID applicationId =
        new org.apache.hadoop.yarn.ApplicationID();
    applicationId.id = Integer.valueOf(id.toString());
    applicationId.clusterTimeStamp = clustertimestamp;
    return applicationId;
  }
  
  public static class Comparator 
  implements java.util.Comparator<org.apache.hadoop.yarn.ApplicationID> {

    @Override
    public int compare(org.apache.hadoop.yarn.ApplicationID a1,
        org.apache.hadoop.yarn.ApplicationID a2) {
      return a1.compareTo(a2);
    }
    
  }
}
