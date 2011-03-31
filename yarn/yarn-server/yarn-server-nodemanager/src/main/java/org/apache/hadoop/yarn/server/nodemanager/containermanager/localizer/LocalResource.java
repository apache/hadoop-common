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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.util.ConverterUtils;


/**
 * A comparable {@link org.apache.hadoop.yarn.XLocalResource}.
 * 
 */
class LocalResource implements Comparable<LocalResource> {

  private final Path loc;
  private final long timestamp;
  private final LocalResourceType type;

  /**
   * Convert yarn.LocalResource into a localizer.LocalResource.
   * @param resource
   * @throws URISyntaxException
   */
  public LocalResource(org.apache.hadoop.yarn.api.records.LocalResource resource)
      throws URISyntaxException {
    this.loc = ConverterUtils.getPathFromYarnURL(resource.getResource());
    this.timestamp = resource.getTimestamp();
    this.type = resource.getType();
  }

  @Override
  public int hashCode() {
    return loc.hashCode() ^
      (int)((timestamp >>> 1) ^ timestamp) *
      type.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalResource)) {
      return false;
    }
    final LocalResource other = (LocalResource) o;
    return loc.equals(other.loc) &&
           timestamp == other.timestamp &&
           type == other.type;
  }

  @Override
  public int compareTo(LocalResource other) {
    if (this == other) {
      return 0;
    }
    int ret = loc.compareTo(other.loc);
    if (0 == ret) {
      ret = (int)(timestamp - other.timestamp);
      if (0 == ret) {
        ret = type.ordinal() - other.type.ordinal();
      }
    }
    return ret;
  }

  public Path getPath() {
    return loc;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public LocalResourceType getType() {
    return type;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ");
    sb.append(loc.toString()).append(", ");
    sb.append(timestamp).append(", ");
    sb.append(type).append(" }");
    return sb.toString();
  }
}
