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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.Priority;
import org.apache.hadoop.yarn.ResourceRequest;

/**
 * This interface is used by the components to talk to the
 * scheduler for allocating of resources, cleaning up resources.
 *
 */
public interface YarnScheduler {
  /**
   * Allocates and returns resources.
   * @param applicationId
   * @param ask
   * @param release
   * @return
   * @throws IOException
   */
  List<Container> allocate(ApplicationID applicationId,
      List<ResourceRequest> ask, List<Container> release)
      throws IOException;
  /**
   * A new application has been submitted to the ResourceManager
   * @param applicationId application which has been submitted
   * @param user application user
   * @param queue queue to which the applications is being submitted
   * @param priority application priority
   */
  public void addApplication(ApplicationID applicationId, String user, 
      String queue, Priority priority) 
  throws IOException;
  
  /**
   * A submitted application has completed.
   * @param applicationId completed application
   */
  public void removeApplication(ApplicationID applicationId)
  throws IOException;
}
