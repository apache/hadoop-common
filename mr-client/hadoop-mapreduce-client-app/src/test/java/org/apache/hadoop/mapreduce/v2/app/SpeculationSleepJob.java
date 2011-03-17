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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.IOException;

import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeculationSleepJob extends SleepJob {
  public Job createJob(int numMapper, int numReducer,
                       long mapSleepTime, int mapSleepCount,
                       long reduceSleepTime, int reduceSleepCount)
      throws IOException {
    Job result = super.createJob
        (numMapper, numReducer, mapSleepTime,
         mapSleepCount, reduceSleepTime, reduceSleepCount);

    result.setMapperClass(SpeculationSleepMapper.class);

    return result;
  }

  // This is a new class rather than a subclass of SleepJob.SleepMapper
  //  because SleepMapper has private [rather than protected] fields
  class SpeculationSleepMapper
      extends Mapper<IntWritable, IntWritable, IntWritable, NullWritable> {
    private long mapSleepDuration = 100;
    private int mapSleepCount = 1;
    private int count = 0;

    private final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";

    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      this.mapSleepCount =
        conf.getInt(MAP_SLEEP_COUNT, mapSleepCount);
      this.mapSleepDuration =
        conf.getLong(MAP_SLEEP_TIME , 100) / mapSleepCount;
      final String attemptID = conf.get(TASK_ATTEMPT_ID);
      if (attemptID.endsWith("0000_0")) {
        mapSleepDuration *= 20L;
      }
    }

    // we need to do this instead of
    @Override
    public void map(IntWritable key, IntWritable value, Context context
               ) throws IOException, InterruptedException {
      //it is expected that every map processes mapSleepCount number of records.
      try {
        context.setStatus("Sleeping... (" +
          (mapSleepDuration * (mapSleepCount - count)) + ") ms left");
        Thread.sleep(mapSleepDuration);
      }
      catch (InterruptedException ex) {
        throw (IOException)new IOException("Interrupted while sleeping", ex);
      }
      ++count;
      // output reduceSleepCount * numReduce number of random values, so that
      // each reducer will get reduceSleepCount number of keys.
      int k = key.get();
      for (int i = 0; i < value.get(); ++i) {
        context.write(new IntWritable(k + i), NullWritable.get());
      }
    }
  }
}
