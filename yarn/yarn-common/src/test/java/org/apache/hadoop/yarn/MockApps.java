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

package org.apache.hadoop.yarn;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Utilities to generate fake test apps
 */
public class MockApps {
  static final Iterator<String> NAMES = Iterators.cycle("SleepJob",
      "RandomWriter", "TeraSort", "TeraGen", "PigLatin", "WordCount",
      "I18nApp<☯>");
  static final Iterator<String> USERS = Iterators.cycle("dorothy", "tinman",
      "scarecrow", "glinda", "nikko", "toto", "winkie", "zeke", "gulch");
  static final Iterator<ApplicationState> STATES = Iterators.cycle(
      ApplicationState.values());
  static final Iterator<String> QUEUES = Iterators.cycle("a.a1", "a.a2",
      "b.b1", "b.b2", "b.b3", "c.c1.c11", "c.c1.c12", "c.c1.c13",
      "c.c2", "c.c3", "c.c4");
  static final long TS = System.currentTimeMillis();

  public static String newAppName() {
    synchronized(NAMES) {
      return NAMES.next();
    }
  }

  public static String newUserName() {
    synchronized(USERS) {
      return USERS.next();
    }
  }

  public static String newQueue() {
    synchronized(QUEUES) {
      return QUEUES.next();
    }
  }

  public static List<Application> genApps(int n) {
    List<Application> list = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      list.add(newApp(i));
    }
    return list;
  }

  public static Application newApp(int i) {
    final ApplicationID id = newAppID(i);
    final ApplicationStatus status = newAppStatus();
    final ApplicationState state = newAppState();
    final String user = newUserName();
    final String name = newAppName();
    final String queue = newQueue();
    return new Application() {
      @Override public ApplicationID id() { return id; }
      @Override public CharSequence user() { return user; }
      @Override public CharSequence name() { return name; }
      @Override public ApplicationStatus status() { return status; }
      @Override public ApplicationState state() { return state; }
      @Override public CharSequence queue() { return queue; }
      @Override public CharSequence master() {
        return Math.random() > 0.8 ? null : "localhost";
      }
      @Override public int httpPort() { return 58888; }
      @Override public boolean isFinished() {
        switch (state) {
          case COMPLETED:
          case FAILED:
          case KILLED: return true;
        }
        return false;
      }
    };
  }

  public static ApplicationID newAppID(int i) {
    ApplicationID id = new ApplicationID();
    id.clusterTimeStamp = TS;
    id.id = i;
    return id;
  }

  public static ApplicationStatus newAppStatus() {
    ApplicationStatus status = new ApplicationStatus();
    status.progress = (float) Math.random();
    status.lastSeen = System.currentTimeMillis();
    return status;
  }

  public static ApplicationState newAppState() {
    synchronized(STATES) {
      return STATES.next();
    }
  }
}
