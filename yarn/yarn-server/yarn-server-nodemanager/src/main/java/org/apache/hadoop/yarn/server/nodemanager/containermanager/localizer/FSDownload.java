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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static org.apache.hadoop.fs.Options.*;

import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * Download a single URL to the local disk.
 *
 */
public class FSDownload implements Callable<Map<LocalResource,Path>> {

  private static final Log LOG = LogFactory.getLog(FSDownload.class);

  private Random rand;
  private FileContext files;
  private Configuration conf;
  private LocalResource resource;
  private LocalDirAllocator dirs;
  private FsPermission cachePerms = new FsPermission((short) 0755);

  public FSDownload(Configuration conf, LocalDirAllocator dirs,
      LocalResource resource) throws IOException {
    this(FileContext.getLocalFSFileContext(conf), conf, dirs, resource,
        new Random());
  }

  FSDownload(FileContext files, Configuration conf, LocalDirAllocator dirs,
      LocalResource resource, Random rand) {
    this.conf = conf;
    this.dirs = dirs;
    this.files = files;
    this.resource = resource;
    this.rand = rand;
  }

  LocalResource getResource() {
    return resource;
  }

  private Path copy(Path sCopy, Path dstdir) throws IOException {
    Path dCopy = new Path(dstdir, sCopy.getName() + ".tmp");
    FileStatus sStat = files.getFileStatus(sCopy);
    if (sStat.getModificationTime() != resource.getTimestamp()) {
      throw new IOException("Resource " + sCopy +
          " changed on src filesystem (expected " + resource.getTimestamp() +
          ", was " + sStat.getModificationTime());
    }
    files.util().copy(sCopy, dCopy);
    return dCopy;
  }

  private long unpack(File localrsrc, File dst) throws IOException {
    File destDir = new File(localrsrc.getParent());
    switch (resource.getType()) {
    case ARCHIVE:
      String lowerDst = dst.getName().toLowerCase();
      if (lowerDst.endsWith(".jar")) {
        RunJar.unJar(localrsrc, dst);
      } else if (lowerDst.endsWith(".zip")) {
        FileUtil.unZip(localrsrc, dst);
      } else if (lowerDst.endsWith(".tar.gz") ||
                 lowerDst.endsWith(".tgz") ||
                 lowerDst.endsWith(".tar")) {
        FileUtil.unTar(localrsrc, dst);
      } else {
        LOG.warn("Cannot unpack " + localrsrc);
        localrsrc.renameTo(dst);
      }
      break;
    case FILE:
    default:
      localrsrc.renameTo(dst);
      break;
    }
    return FileUtil.getDU(destDir);
  }

  @Override
  public Map<LocalResource,Path> call() throws IOException {
    Path sCopy;
    try {
      sCopy = ConverterUtils.getPathFromYarnURL(resource.getResource());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid resource", e);
    }

    Path tmp;
    Path dst =
        dirs.getLocalPathForWrite(".", getEstimatedSize(resource),
            conf);
    do {
      tmp = new Path(dst, String.valueOf(rand.nextLong()));
    } while (files.util().exists(tmp));
    dst = tmp;
    files.mkdir(dst, cachePerms, false);
    Path dst_work = new Path(dst + "_tmp");
    files.mkdir(dst_work, cachePerms, false);

    Path dFinal = files.makeQualified(new Path(dst_work, sCopy.getName()));
    try {
      Path dTmp = files.makeQualified(copy(sCopy, dst_work));
      resource.setSize(unpack(new File(dTmp.toUri()), new File(dFinal.toUri())));
      files.rename(dst_work, dst, Rename.OVERWRITE);
    } catch (IOException e) {
      try { files.delete(dst, true); } catch (IOException ignore) { }
      throw e;
    } finally {
      try {
        files.delete(dst_work, true);
      } catch (FileNotFoundException ignore) { }
      // clear ref to internal var
      rand = null;
      conf = null;
      //resource = null; TODO change downstream to not req
      dirs = null;
      cachePerms = null;
    }
    return Collections.singletonMap(resource,
        files.makeQualified(new Path(dst, sCopy.getName())));
  }

  private static long getEstimatedSize(LocalResource rsrc) {
    if (rsrc.getSize() < 0) {
      return -1;
    }
    switch (rsrc.getType()) {
      case ARCHIVE:
        return 5 * rsrc.getSize();
      case FILE:
      default:
        return rsrc.getSize();
    }
  }

}
