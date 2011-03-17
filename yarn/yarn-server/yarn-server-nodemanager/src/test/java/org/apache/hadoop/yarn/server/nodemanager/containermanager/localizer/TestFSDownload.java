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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.FSDownload;
import org.apache.hadoop.yarn.util.AvroUtil;

import static org.apache.hadoop.fs.CreateFlag.*;

import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.LocalResourceType;

import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestFSDownload {

  @AfterClass
  public static void deleteTestDir() throws IOException {
    FileContext fs = FileContext.getLocalFSFileContext();
    fs.delete(new Path("target", TestFSDownload.class.getSimpleName()), true);
  }

  static LocalResource createFile(FileContext files, Path p, int len, Random r)
      throws IOException, URISyntaxException {
    FSDataOutputStream out = null;
    try {
      byte[] bytes = new byte[len];
      out = files.create(p, EnumSet.of(CREATE, OVERWRITE));
      r.nextBytes(bytes);
      out.write(bytes);
    } finally {
      if (out != null) out.close();
    }
    LocalResource ret = new LocalResource();
    ret.resource = AvroUtil.getYarnUrlFromPath(p);
    ret.size = len;
    ret.type = LocalResourceType.FILE;
    ret.timestamp = files.getFileStatus(p).getModificationTime();
    return ret;
  }

  @Test
  public void testDownload() throws IOException, URISyntaxException,
      InterruptedException {
    Configuration conf = new Configuration();
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
      TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());

    Collection<FSDownload> pending = new ArrayList<FSDownload>();
    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    System.out.println("SEED: " + sharedSeed);
    LocalDirAllocator dirs =
      new LocalDirAllocator(TestFSDownload.class.getName());
    int[] sizes = new int[10];
    for (int i = 0; i < 10; ++i) {
      sizes[i] = rand.nextInt(512) + 512;
      LocalResource rsrc = createFile(files, new Path(basedir, "" + i),
          sizes[i], rand);
      FSDownload fsd =
        new FSDownload(files, conf, dirs, rsrc, new Random(sharedSeed));
      pending.add(fsd);
    }

    ExecutorService exec = Executors.newSingleThreadExecutor();
    CompletionService<Map<LocalResource,Path>> queue =
      new ExecutorCompletionService<Map<LocalResource,Path>>(exec);
    try {
      for (FSDownload fsd : pending) {
        queue.submit(fsd);
      }
      Map<LocalResource,Path> results = new HashMap();
      for (int i = 0; i < 10; ++i) {
        Future<Map<LocalResource,Path>> result = queue.take();
        results.putAll(result.get());
      }
      for (Map.Entry<LocalResource,Path> localized : results.entrySet()) {
        assertEquals(
            sizes[Integer.valueOf(localized.getValue().getName())],
            localized.getKey().size - 4096 - 16); // bad DU impl + .crc ; sigh
      }
    } catch (ExecutionException e) {
      throw new IOException("Failed exec", e);
    } finally {
      exec.shutdown();
    }
  }

}
