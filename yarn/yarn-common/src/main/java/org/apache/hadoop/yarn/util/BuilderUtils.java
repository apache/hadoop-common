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

import java.net.URI;

import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.LocalResourceType;
import org.apache.hadoop.yarn.LocalResourceVisibility;

/**
 * Builder utilities to construct various objects.
 *
 */
public class BuilderUtils {

  public static LocalResource newLocalResource(URI uri, 
      LocalResourceType type, LocalResourceVisibility visibility, 
      long size, long timestamp) {
    LocalResource resource = new LocalResource();
    resource.resource = AvroUtil.getYarnUrlFromURI(uri);
    resource.type = type;
    resource.state = visibility;
    resource.size = size;
    resource.timestamp = timestamp;
    return resource;
  }

}
