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

import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

public class ApplicationID {
  
  public static org.apache.hadoop.yarn.api.records.ApplicationId create(long clusterTimeStamp,
      int id) {
    org.apache.hadoop.yarn.api.records.ApplicationId applicationId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(org.apache.hadoop.yarn.api.records.ApplicationId.class);
    applicationId.setId(id);
    applicationId.setClusterTimestamp(clusterTimeStamp);
    return applicationId;
  }
  
  public static org.apache.hadoop.yarn.api.records.ApplicationId convert(long clustertimestamp,
      CharSequence id) {
    org.apache.hadoop.yarn.api.records.ApplicationId applicationId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(org.apache.hadoop.yarn.api.records.ApplicationId.class);
    applicationId.setId(Integer.valueOf(id.toString()));
    applicationId.setClusterTimestamp(clustertimestamp);
    return applicationId;
  }
  
  public static class Comparator 
  implements java.util.Comparator<org.apache.hadoop.yarn.api.records.ApplicationId> {

    
    @Override
    public int compare(org.apache.hadoop.yarn.api.records.ApplicationId a1,
        org.apache.hadoop.yarn.api.records.ApplicationId a2) {
      return a1.compareTo(a2);
    }
    
  }
}
