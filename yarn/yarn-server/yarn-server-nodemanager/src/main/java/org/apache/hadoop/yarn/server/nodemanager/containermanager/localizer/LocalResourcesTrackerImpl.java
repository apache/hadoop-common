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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.YarnRemoteException;

class LocalResourcesTrackerImpl implements LocalResourcesTracker {

  static final Log LOG = LogFactory.getLog(LocalResourcesTrackerImpl.class);

  private final ConcurrentHashMap<LocalResource,LocalizedResource> localrsrc =
    new ConcurrentHashMap<LocalResource,LocalizedResource>();

  public void setSuccess(LocalResource lResource, long rsrcSize,
      Path localPath) throws IllegalArgumentException, InterruptedException {
    LocalizedResource rsrc = localrsrc.get(lResource);
    if (null == rsrc) {
      throw new IllegalArgumentException("Unknown: " + lResource.getPath());
    }
    rsrc.success(lResource, rsrcSize, localPath);
  }

  public void removeFailedResource(LocalResource lResource,
      YarnRemoteException e) throws IllegalArgumentException {
    LocalizedResource rsrc = localrsrc.get(lResource);
    if (null == rsrc) {
      throw new IllegalArgumentException("Unknown: " + lResource.getPath());
    }
    rsrc.failure(lResource, e);
  }

  protected long currentTime() {
    return System.nanoTime();
  }

  // TODO replace w/ queue over RPC
  /** @return Resources not present in this bundle */
  public Collection<org.apache.hadoop.yarn.LocalResource> register(
      AppLocalizationRunnerImpl app,
      Collection<org.apache.hadoop.yarn.LocalResource> rsrcs)
      throws URISyntaxException {
    ArrayList<org.apache.hadoop.yarn.LocalResource> ret =
      new ArrayList<org.apache.hadoop.yarn.LocalResource>(rsrcs.size());
    for (final org.apache.hadoop.yarn.LocalResource yrsrc : rsrcs) {
      final LocalizedResource cand =
        new LocalizedResource(new Callable<Map<LocalResource,Path>>() {
              @Override
              public Map<LocalResource,Path> call() {
                // Future complete from RPC callback
                throw new UnsupportedOperationException();
              }
            });
      final LocalResource rsrc = new LocalResource(yrsrc);
      while (true) {
        LocalizedResource actual = localrsrc.putIfAbsent(rsrc, cand);
        if (null == actual) {
          cand.request(app);
          ret.add(yrsrc);
          break;
        }
        // ensure resource localization not cancelled
        if (actual.isCancelled()) {
          if (!localrsrc.replace(rsrc, actual, cand)) {
            // newer entry exists
            continue;
          } else {
            cand.request(app);
            ret.add(yrsrc);
            break;
          }
        }
        actual.request(app);
        break;
      }
    }
    return ret;
  }

  public void release(AppLocalizationRunnerImpl app, LocalResource[] resources) {
    for (LocalResource rsrc : resources) {
      LocalizedResource resource = localrsrc.get(rsrc);
      if (resource != null) {
        // XXX update timestamp to last-used?
        resource.refCount.getAndDecrement();
        resource.notifyQueue.remove(app);
      }
    }
  }

  /**
   * Private inner datum tracking a resource that is already localized.
   */
  // TODO use AQS for download
  private class LocalizedResource extends
      FutureTask<Map<LocalResource, Path>> {
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final AtomicLong timestamp = new AtomicLong(currentTime());
    private final BlockingQueue<AppLocalizationRunnerImpl> notifyQueue =
      new LinkedBlockingQueue<AppLocalizationRunnerImpl>();
    // TODO: Why is it needed?
    private volatile long size = -1;
    LocalizedResource(Callable<Map<LocalResource,Path>> fetch) {
      super(fetch);
    }
    void request(AppLocalizationRunnerImpl appRunner) {
      refCount.getAndIncrement();
      timestamp.set(currentTime());
      notifyQueue.offer(appRunner);
      if (isDone() && notifyQueue.remove(appRunner)) {
        appRunner.localizedResource(this);
      }
    }
    void success(LocalResource rsrc, long rsrcSize, Path p)
        throws InterruptedException {
      size = rsrcSize;
      set(Collections.singletonMap(rsrc, p));
    }
    void failure(LocalResource r, YarnRemoteException e) {
      // TODO: How do you inform the appRunner?
      localrsrc.remove(r, this);
      setException(e);
    }
    @Override
    protected void done() {
      for (AppLocalizationRunner appRunner = notifyQueue.poll(); appRunner != null;
           appRunner = notifyQueue.poll()) {
        appRunner.localizedResource(this);
      }
    }
  }

}
