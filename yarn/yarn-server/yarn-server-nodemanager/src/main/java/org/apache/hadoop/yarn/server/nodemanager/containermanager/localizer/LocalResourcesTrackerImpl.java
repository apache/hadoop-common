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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;

/**
 * A collection of {@link LocalizedResource}s all of same
 * {@link LocalResourceVisibility}.
 * 
 */
class LocalResourcesTrackerImpl implements LocalResourcesTracker {

  static final Log LOG = LogFactory.getLog(LocalResourcesTrackerImpl.class);

  private final Dispatcher dispatcher;
  private final ConcurrentHashMap<LocalResourceRequest,LocalizedResource>
    localrsrc = new ConcurrentHashMap<LocalResourceRequest,LocalizedResource>();

  public LocalResourcesTrackerImpl(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void handle(ResourceEvent event) {
    LocalResourceRequest req = event.getLocalResourceRequest();
    LocalizedResource rsrc = localrsrc.get(req);
    switch (event.getType()) {
    case REQUEST:
    case LOCALIZED:
      if (null == rsrc) {
        rsrc = new LocalizedResource(req, dispatcher);
        localrsrc.put(req, rsrc);
      }
      break;
    case RELEASE:
      if (null == rsrc) {
        LOG.info("Release unknown rsrc " + rsrc + " (discard)");
        return;
      }
      break;
    }
    rsrc.handle(event);
  }

  @Override
  public boolean contains(LocalResourceRequest resource) {
    return localrsrc.contains(resource);
  }

}
