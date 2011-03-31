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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import static java.util.concurrent.TimeUnit.*;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.AppLocalizationRunnerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourcesTrackerImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.*;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.*;

public class TestLocalResources {

  private static List<org.apache.hadoop.yarn.api.records.LocalResource>
      randResources(Random r, int nRsrc) throws URISyntaxException {
    final List<org.apache.hadoop.yarn.api.records.LocalResource> ret =
      new ArrayList<org.apache.hadoop.yarn.api.records.LocalResource>(nRsrc);
    Path base = new Path("file:///foo/bar");
    long basetime = r.nextLong() >>> 2;
    for (int i = 0; i < nRsrc; ++i) {
      org.apache.hadoop.yarn.api.records.LocalResource rsrc = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(org.apache.hadoop.yarn.api.records.LocalResource.class);
      rsrc.setTimestamp(basetime + i);
      r.setSeed(rsrc.getTimestamp());
      Path p = new Path(base, String.valueOf(r.nextInt(Integer.MAX_VALUE)));
      while (r.nextInt(2) == 1) {
        p = new Path(p, String.valueOf(r.nextInt(Integer.MAX_VALUE)));
      }
      rsrc.setResource(ConverterUtils.getYarnUrlFromPath(p));
      rsrc.setSize(-1);
      rsrc.setType(r.nextInt(2) == 1 ? FILE : ARCHIVE);
      rsrc.setVisibility(PRIVATE);

      System.out.println("RSRC: " + rsrc);
      ret.add(rsrc);
    }
    return ret;
  }

  private static void verify(
      BlockingQueue<Future<Map<LocalResource,Path>>> doneQueue,
      Collection<org.apache.hadoop.yarn.api.records.LocalResource> files)
      throws ExecutionException, InterruptedException, URISyntaxException {
    for (Future<Map<LocalResource,Path>> f = doneQueue.poll(); f != null;
         f = doneQueue.poll()) {
      Map<LocalResource,Path> q = f.get();
      assertEquals(1, q.size());
      for (Map.Entry<LocalResource,Path> loc : q.entrySet()) {
        boolean found = false;
        for (org.apache.hadoop.yarn.api.records.LocalResource yrsrc : files) {
          LocalResource rsrc = new LocalResource(yrsrc);
          found |= rsrc.equals(loc.getKey());
        }
        assertTrue(found);
        assertEquals(new Path("file:///yak/" + loc.getKey().getTimestamp()),
                     loc.getValue());
      }
    }
  }

  private static AppLocalizationRunnerImpl getMockAppLoc(
    final BlockingQueue<Future<Map<LocalResource,Path>>> doneQueue,
    String name) {
    AppLocalizationRunnerImpl mockAppLoc = mock(AppLocalizationRunnerImpl.class);
    doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) {
          doneQueue.offer(
            (Future<Map<LocalResource,Path>>)invocation.getArguments()[0]);
          return null;
        }
      }).when(mockAppLoc).localizedResource(
        Matchers.<Future<Map<LocalResource,Path>>>anyObject());
    when(mockAppLoc.toString()).thenReturn(name);
    return mockAppLoc;
  }

  static void successfulLoc(LocalResourcesTrackerImpl rsrcMap,
      List<org.apache.hadoop.yarn.api.records.LocalResource> rsrc)
      throws InterruptedException, URISyntaxException {
    long i = 0;
    for (org.apache.hadoop.yarn.api.records.LocalResource yRsrc : rsrc) {
      yRsrc.setSize(++i);
      rsrcMap.setSuccess(new LocalResource(yRsrc), yRsrc.getSize(),
          new Path("file:///yak/" + yRsrc.getTimestamp()));
    }
  }

  static void failedLoc(
      BlockingQueue<Future<Map<LocalResource,Path>>> doneQueue) {
    try {
      Future<Map<LocalResource,Path>> f = doneQueue.poll();
      f.get();
      fail("Failed localization succeeded");
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Wrong exception");
    } catch (ExecutionException e) {
      /* expected */
    }
  }

  @Test
  public void testLocalResourceAcquire() throws Exception {
    final int NUM_EXP = 4;
    final int NUM_URIS = 1 << NUM_EXP;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    // shared resource map
    LocalResourcesTrackerImpl rsrcMap = new LocalResourcesTrackerImpl();
    List<org.apache.hadoop.yarn.api.records.LocalResource> resourcesA = randResources(r, NUM_URIS);

    // set up application A mocks
    final BlockingQueue<Future<Map<LocalResource,Path>>> doneQueueA =
      new LinkedBlockingQueue<Future<Map<LocalResource,Path>>>();
    AppLocalizationRunnerImpl mockAppLocA = getMockAppLoc(doneQueueA, "A");

    // set up application B mocks
    final BlockingQueue<Future<Map<LocalResource,Path>>> doneQueueB =
      new LinkedBlockingQueue<Future<Map<LocalResource,Path>>>();
    AppLocalizationRunnerImpl mockAppLocB = getMockAppLoc(doneQueueB, "B");

    Collection<org.apache.hadoop.yarn.api.records.LocalResource> todoA =
      rsrcMap.register(mockAppLocA, resourcesA);
    // ensure no rsrc added until reported back
    assertEquals(NUM_URIS, todoA.size());
    assertTrue(doneQueueA.isEmpty());

    // complete half A localization
    successfulLoc(rsrcMap, resourcesA.subList(0, NUM_URIS >>> 1));

    // verify callback
    assertEquals(NUM_URIS >>> 1, doneQueueA.size());
    verify(doneQueueA, todoA);

    // set up B localization as 1/4 localized A rsrc, 1/4 non-localized A rsrc,
    // 1/2 new rsrc
    long seed2 = r.nextLong();
    r.setSeed(seed2);
    System.out.println("SEED: " + seed2);
    List<org.apache.hadoop.yarn.api.records.LocalResource> resourcesB =
      randResources(r, NUM_URIS >>> 1);
    resourcesB.addAll(resourcesA.subList(NUM_URIS >>> 2,
                                         NUM_URIS - (NUM_URIS >>> 2)));
    Collection<org.apache.hadoop.yarn.api.records.LocalResource> todoB =
      rsrcMap.register(mockAppLocB, resourcesB);
    // all completed A rsrc
    assertEquals(NUM_URIS >>> 2, doneQueueB.size());
    // only uniq rsrc, not in A
    assertEquals(NUM_URIS >>> 1, todoB.size());
    // verify completed rsrc in B done queue completed by A
    verify(doneQueueB, todoA);

    // complete A localization, less 1 shared rsrc
    successfulLoc(rsrcMap, resourcesA.subList((NUM_URIS >>> 1) + 1, NUM_URIS));
    // completed A rsrc in B
    assertEquals((NUM_URIS >>> 2) - 1, doneQueueB.size());
    verify(doneQueueB, todoA);
    assertEquals((NUM_URIS >>> 1) - 1, doneQueueA.size());
    verify(doneQueueA, todoA);

    // fail shared localization
    rsrcMap.removeFailedResource(new LocalResource(resourcesA.get(NUM_URIS >>> 1)),
        RPCUtil.getRemoteException(new IOException("FAIL!")));
    assertEquals(1, doneQueueA.size());
    assertEquals(1, doneQueueB.size());
    failedLoc(doneQueueA);
    failedLoc(doneQueueB);

    // verify cleared
    Collection<org.apache.hadoop.yarn.api.records.LocalResource> todoA2 =
      rsrcMap.register(mockAppLocA, Collections.singletonList(
            resourcesA.get(NUM_URIS >>> 1)));
    assertEquals(1, todoA2.size());
  }

}
