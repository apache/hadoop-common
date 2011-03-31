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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class ApplicationInitedEvent extends ApplicationEvent {

  private final Path workdir;
  private final Map<Path,String> localizedResources;

  public ApplicationInitedEvent(ApplicationId appID,
      Map<Path,String> localizedResources, Path workdir) {
    super(appID, ApplicationEventType.APPLICATION_INITED);
    this.workdir = workdir;
    this.localizedResources = localizedResources;
  }

  public Map<Path,String> getLocalizedResources() {
    return localizedResources;
  }

  public Path getWorkDirectory() {
    return workdir;
  }

}
