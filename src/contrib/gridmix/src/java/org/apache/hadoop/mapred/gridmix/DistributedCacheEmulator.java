/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Emulation of Distributed Cache Usage in gridmix.
 * <br> Emulation of Distributed Cache Load in gridmix will put load on
 * TaskTrackers and affects execution time of tasks because of localization of
 * distributed cache files by TaskTrackers.
 * <br> Gridmix creates private and public distributed cache files by launching
 * a MapReduce job {@link GenerateDistCacheData} in advance i.e. before
 * launching simulated jobs.
 * <br> The file paths of original cluster are mapped to the simulation
 * cluster's paths. The configuration properties like
 * {@link MRJobConfig#CACHE_FILES}, {@link MRJobConfig#CACHE_FILE_VISIBILITIES},
 * {@link MRJobConfig#CACHE_FILES_SIZES} and
 * {@link MRJobConfig#CACHE_FILE_TIMESTAMPS} obtained from trace are used to
 *  decide
 * <li> file size of each distributed cache file to be generated
 * <li> whether a dist cache file is already seen in this trace file
 * <li> whether a distributed cache file be considered public or private.
 * <br>
 * <br> Gridmix configures these generated files as distributed cache files for
 * the simulated jobs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class DistributedCacheEmulator {
  private static final Log LOG =
      LogFactory.getLog(DistributedCacheEmulator.class);

  static final long AVG_BYTES_PER_MAP = 128 * 1024 * 1024L;// 128MB

  // If at least 1 dist cache file is missing in the expected dist cache dir,
  // Gridmix cannot proceed with emulation of distributed cache load.
  int MISSING_DIST_CACHE_FILES_ERROR = 1;

  private Path distCachePath;
  private Path privateDistCachePath = null;
  private Path publicDistCachePath = null;

  /**
   * Map between simulated cluster's dist cache file paths and their file sizes.
   * Unique dist cache files are entered into this map. 2 dist cache files are
   * considered same if and only if their file paths, visibilities and
   * timestamps are same.
   */
  private Map<String, Long> distCacheFiles = new HashMap<String, Long>();

  /**
   * Configuration property for whether gridmix should emulate
   * distributed cache usage or not. Default value is true.
   */
  static final String GRIDMIX_EMULATE_DISTRIBUTEDCACHE =
      "gridmix.distributed-cache-emulation.enable";

  // Whether to emulate distributed cache usage or not
  boolean emulateDistributedCache = true;

  // Whether to generate dist cache data or not
  boolean generateDistCacheData = false;

  Configuration conf; // gridmix configuration
  
  /**
   * @param conf gridmix configuration
   * @param ioPath &lt;ioPath&gt;/distributedCache/ is the gridmix Distributed
   *               Cache directory
   */
  public DistributedCacheEmulator(Configuration conf, Path ioPath) {
    this.conf = conf;
    distCachePath = new Path(ioPath, "distributedCache");
  }

  /**
   * This is to be called before any other method of DistributedCacheEmulator.
   * <br> Checks if emulation of Dist Cache load is needed and is feasible.
   *  Sets the flags generateDistCacheData and emulateDistributedCache to the
   *  appropriate values.
   * <br> Gridmix does not emulate dist cache load if
   * <ol><li> the specific gridmix job type doesn't need emulation of dist cache
   * load OR
   * <li> the trace is coming from a stream instead of file OR
   * <li> the dist cache dir where dist cache data is to be generated by
   * gridmix is on local file system OR
   * <li> execute permission is not there for any of the ascendant directories
   * of &lt;ioPath&gt; till root. This is because for emulation of dist cache
   * load, dist cache files created under
   * &lt;ioPath/distributedCache/public/&gt; should be considered by hadoop
   * as public dist cache files.</ol>
   * <br> For (2), (3) and (4), generation of distributed cache data
   * is also disabled.
   * 
   * @param traceIn trace file path. If this is '-', then trace comes from the
   *                stream stdin.
   * @param jobCreator job creator of gridmix jobs of a specific type
   * @param generate  true if -generate option was specified
   * @throws IOException
   */
  void init(String traceIn, JobCreator jobCreator, boolean generate)
      throws IOException {
    emulateDistributedCache = jobCreator.canEmulateDistCacheLoad()
        && conf.getBoolean(GRIDMIX_EMULATE_DISTRIBUTEDCACHE, true);
    generateDistCacheData = generate;

    if (generateDistCacheData || emulateDistributedCache) {
      if ("-".equals(traceIn)) {// trace is from stdin
        LOG.warn("Gridmix will not emulate Distributed Cache load because "
            + "the input trace source is a stream instead of file.");
        emulateDistributedCache = generateDistCacheData = false;
      } else if (FileSystem.getLocal(conf).getUri().getScheme().equals(
          distCachePath.toUri().getScheme())) {// local FS
        LOG.warn("Gridmix will not emulate Distributed Cache load because "
            + "<iopath> provided is on local file system.");
        emulateDistributedCache = generateDistCacheData = false;
      } else {
        // Check if execute permission is there for all the ascendant
        // directories of distCachePath till root.
        FileSystem fs = FileSystem.get(conf);
        Path cur = distCachePath.getParent();
        while (cur != null) {
          if (cur.toString().length() > 0) {
            FsPermission perm = fs.getFileStatus(cur).getPermission();
            if (!perm.getOtherAction().and(FsAction.EXECUTE).equals(
                FsAction.EXECUTE)) {
              LOG.warn("Gridmix will not emulate Distributed Cache load "
                  + "because the ascendant directory (of distributed cache "
                  + "directory) " + cur + " doesn't have execute permission "
                  + "for others.");
              emulateDistributedCache = generateDistCacheData = false;
              break;
            }
          }
          cur = cur.getParent();
        }
      }
    }
  }

  /**
   * @return true if gridmix should emulate Dist Cache load
   */
  boolean shouldEmulateDistCacheLoad() {
    return emulateDistributedCache;
  }

  /**
   * @return true if gridmix should generate distributed cache data
   */
  boolean shouldGenerateDistCacheData() {
    return generateDistCacheData;
  }

  /**
   * @return Dir under which gridmix creates private distributed cache files
   */
  Path getPrivateDistCacheDir() {
    return privateDistCachePath;
  }

  /**
   * @return Dir under which gridmix creates public distributed cache files
   */
  Path getPublicDistCacheDir() {
    return publicDistCachePath;
  }

  /**
   * Create distributed cache directories.
   * Also create a file that contains the list of distributed cache files
   * that will be used as dist cache files for all the simulated jobs.
   * @param jsp job story producer for the trace
   * @return exit code
   * @throws IOException
   */
  int setupGenerateDistCacheData(JobStoryProducer jsp)
      throws IOException {

    createDistCacheDirectories();
    return buildDistCacheFilesList(jsp);
  }

  /**
   * Create distributed cache directories where dist cache files will be
   * created by the MapReduce job GRIDMIX_GENERATE_DISTCACHE_DATA.
   * @throws IOException
   */
  void createDistCacheDirectories() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileSystem.mkdirs(fs, distCachePath, new FsPermission((short) 0777));

    // For private dist cache dir, no execute permission for others
    privateDistCachePath =
        new Path(distCachePath, "private");
    FileSystem.mkdirs(fs, privateDistCachePath,
        new FsPermission((short) 0776));

    publicDistCachePath =
        new Path(distCachePath, "public");
    FileSystem.mkdirs(fs, publicDistCachePath,
        new FsPermission((short) 0777));
  }

  /**
   * Create the list of unique dist cache files needed for all the
   * simulated jobs and write the list to a special file.
   * @param jsp job story producer for the trace
   * @return exit code
   * @throws IOException
   */
  private int buildDistCacheFilesList(JobStoryProducer jsp) throws IOException {
    // Read all the jobs from the trace file and build the list of unique
    // dist cache files.
    JobStory jobStory;
    while ((jobStory = jsp.getNextJob()) != null) {
      if (jobStory.getOutcome() == Pre21JobHistoryConstants.Values.SUCCESS && 
         jobStory.getSubmissionTime() >= 0) {
        updateDistCacheFilesList(jobStory);
      }
    }
    jsp.close();

    return writeDistCacheFilesList();
  }

  /**
   * For the job to be simulated, identify the needed distributed cache files by
   * mapping original cluster's dist cache file paths to the simulated cluster's
   * paths and add these paths in the map {@code distCacheFiles}.
   *<br>
   * JobStory should contain dist cache related properties like
   * <li> {@link MRJobConfig#CACHE_FILES}
   * <li> {@link MRJobConfig#CACHE_FILE_VISIBILITIES}
   * <li> {@link MRJobConfig#CACHE_FILES_SIZES}
   * <li> {@link MRJobConfig#CACHE_FILE_TIMESTAMPS}
   * <li> {@link MRJobConfig#CLASSPATH_FILES}
   *
   * <li> {@link MRJobConfig#CACHE_ARCHIVES}
   * <li> {@link MRJobConfig#CACHE_ARCHIVES_VISIBILITIES}
   * <li> {@link MRJobConfig#CACHE_ARCHIVES_SIZES}
   * <li> {@link MRJobConfig#CACHE_ARCHIVES_TIMESTAMPS}
   * <li> {@link MRJobConfig#CLASSPATH_ARCHIVES}
   *
   * <li> {@link MRJobConfig#CACHE_SYMLINK}
   *
   * @param jobdesc JobStory of original job obtained from trace
   * @throws IOException
   */
  void updateDistCacheFilesList(JobStory jobdesc)
      throws IOException {

    // Map original job's dist cache file paths to simulated cluster's paths,
    // to be used by this simulated job.
    JobConf jobConf = jobdesc.getJobConf();

    String[] files = jobConf.getStrings(MRJobConfig.CACHE_FILES);
    if (files != null) {

      String[] fileSizes = jobConf.getStrings(MRJobConfig.CACHE_FILES_SIZES);
      String[] visibilities =
        jobConf.getStrings(MRJobConfig.CACHE_FILE_VISIBILITIES);
      String[] timeStamps =
        jobConf.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS);

      FileSystem fs = FileSystem.get(conf);
      for (int i = 0; i < files.length; i++) {
        String mappedPath = mapDistCacheFilePath(files[i], timeStamps[i],
                              Boolean.valueOf(visibilities[i]));

        // No need to add a dist cache file path to the list if
        // (1) the mapped path is already there in the list OR
        // (2) the file with the mapped path already exists.
        // In any of the above 2 cases, file paths, timestamps, file sizes and
        // visibilities match. File sizes should match if file paths and
        // timestamps match because single file path with single timestamp
        // should correspond to a single file size.
        if (distCacheFiles.containsKey(mappedPath) ||
            fs.exists(new Path(mappedPath))) {
          continue;
        }
        distCacheFiles.put(mappedPath, Long.valueOf(fileSizes[i]));
      }
    }
  }

  /**
   * Map the dist cache file path from original cluster to simulated cluster
   * @param file dist cache file path
   * @param timeStamp time stamp of dist cachce file
   * @param isPublic true if this dist cache file is a public dist cache file
   * @return the mapped path on simulated cluster
   */
  private String mapDistCacheFilePath(String file, String timeStamp,
      boolean isPublic) {
    Path distCacheDir = isPublic ? publicDistCachePath
                                 : privateDistCachePath;
    return new Path(distCacheDir,
        MD5Hash.digest(file + timeStamp).toString()).toUri().getPath();
  }

  /**
   * Write the list of dist cache files in the decreasing order of file sizes
   * into the sequence file. This file will be input to the job
   * {@link GenerateDistCacheData}.
   * Also validates if -generate option is missing and dist cache files are
   * missing.
   * @return exit code
   * @throws IOException
   */
  private int writeDistCacheFilesList()
      throws IOException {
    // Sort the dist cache files based on file sizes in the decreasing order.
    List dcFiles = new ArrayList(distCacheFiles.entrySet());
    Collections.sort(dcFiles, new Comparator() {
      public int compare(Object dc1, Object dc2) {
        return ((Comparable) ((Map.Entry) (dc2)).getValue())
       .compareTo(((Map.Entry) (dc1)).getValue());
      }
    });

    // write the sorted dist cache files to the sequence file
    FileSystem fs = FileSystem.get(conf);
    Path distCacheFilesList = new Path(distCachePath, "_distCacheFiles.txt");
    conf.set(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_LIST,
        distCacheFilesList.toString());
    SequenceFile.Writer src_writer = SequenceFile.createWriter(fs, conf,
        distCacheFilesList, LongWritable.class, BytesWritable.class,
        SequenceFile.CompressionType.NONE);
    int fileCount = dcFiles.size();// total number of unique dist cache files
    long byteCount = 0;// total size of all dist cache files
    long bytesSync = 0;// bytes after previous sync;used to add sync marker

    for (Iterator it = dcFiles.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry)it.next();
      LongWritable fileSize =
          new LongWritable(Long.valueOf(entry.getValue().toString()));
      BytesWritable filePath =
          new BytesWritable(entry.getKey().toString().getBytes());

      byteCount += fileSize.get();
      bytesSync += fileSize.get();
      if (bytesSync > AVG_BYTES_PER_MAP) {
        src_writer.sync();
        bytesSync = fileSize.get();
      }
      src_writer.append(fileSize, filePath);
    }
    if (src_writer != null) {
      src_writer.close();
    }
    conf.setInt(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_COUNT, fileCount);
    conf.setLong(GenerateDistCacheData.GRIDMIX_DISTCACHE_BYTE_COUNT, byteCount);
    LOG.info("DistCacheFilesToBeGeneratedCount=" + fileCount
        + ". TotalBytesToBeGeneratedInDistCacheFiles=" + byteCount);

    if (!shouldGenerateDistCacheData() && fileCount > 0) {
      LOG.error("Missing " + fileCount + " distributed cache files under the "
          + " directory\n" + distCachePath + "\nthat are needed for gridmix"
          + " to emulate distributed cache load. Either use -generate\noption"
          + " to generate distributed cache data along with input data OR "
          + "disable\ndistributed cache emulation by configuring '"
          + DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE
          + "' to false.");
      return MISSING_DIST_CACHE_FILES_ERROR;
    }
    return 0;
  }

  /**
   * If gridmix needs to emulate dist cache load, then configure dist cache
   * files of a simulated job by mapping the original cluster's dist cache
   * file paths to the simulated cluster's paths and setting these mapped paths
   * in the job configuration of the simulated job.
   * @param conf configuration for the simulated job to be run
   * @param jobConf job configuration of original cluster's job, obtained from
   *                trace
   * @throws IOException
   */
  void configureDistCacheFiles(Configuration conf, JobConf jobConf)
      throws IOException {
    if (shouldEmulateDistCacheLoad()) {

      String[] files = jobConf.getStrings(MRJobConfig.CACHE_FILES);
      if (files != null) {
        String[] cacheFiles = new String[files.length];
        String[] visibilities =
          jobConf.getStrings(MRJobConfig.CACHE_FILE_VISIBILITIES);
        String[] timeStamps =
          jobConf.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS);

        for (int i = 0; i < files.length; i++) {
          String mappedPath = mapDistCacheFilePath(files[i], timeStamps[i],
              Boolean.valueOf(visibilities[i]));
          cacheFiles[i] = mappedPath;
        }
        conf.setStrings(MRJobConfig.CACHE_FILES, cacheFiles);
      }
    }
  }
}
