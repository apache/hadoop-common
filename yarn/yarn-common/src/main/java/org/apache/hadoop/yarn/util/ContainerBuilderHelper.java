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

package org.apache.hadoop.yarn.util;

import java.util.regex.Pattern;

import org.apache.hadoop.yarn.YarnContainerTags;

// TODO: Remove this and related stuff?
public class ContainerBuilderHelper {

  private static final String DOT = ".";
  private static final String YARN_TAG_BEGIN = "<";
  private static final String YARN_TAG_END = ">";

  private static String workDir = YARN_TAG_BEGIN
      + YarnContainerTags.YARN_WORK_DIR + YARN_TAG_END;
  private static Pattern envPattern = Pattern.compile(getEnvVar("([^"
      + YARN_TAG_END + "]*)")); // <YARN_ENV_TAG.([^>])*>

  public static String getWorkDir() {
    return "$PWD";
  }

  public static String getEnvVar(String varName) {
    return YARN_TAG_BEGIN + YarnContainerTags.YARN_ENV_TAG + DOT + varName
        + YARN_TAG_END;
  }

  public static Pattern getEnvPattern() {
    return envPattern;
  }
}
