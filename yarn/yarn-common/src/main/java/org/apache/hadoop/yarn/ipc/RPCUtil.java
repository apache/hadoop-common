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

package org.apache.hadoop.yarn.ipc;

import org.apache.hadoop.yarn.YarnRemoteException;

public class RPCUtil {

  /**
   * Returns the YarnRemoteException which is serializable by 
   * Avro.
   */
  public static YarnRemoteException getRemoteException(Throwable t) {
    YarnRemoteException e = new YarnRemoteException();
    if (t != null) {
      e.message = t.getMessage();
      StringBuilder buf = new StringBuilder();
      StackTraceElement[] trace = t.getStackTrace();
      if (trace != null) {
        for (StackTraceElement element : trace) {
          buf.append(element.toString() + "\n    at ");
        }
        e.trace = buf.toString();
      }
      Throwable cause = t.getCause();
      if (cause != null) {
        e.cause = getRemoteException(cause);
      }
    }
    return e;
  }

  /**
   * Returns the YarnRemoteException which is serializable by 
   * Avro.
   */
  public static YarnRemoteException getRemoteException(String message) {
    YarnRemoteException e = new YarnRemoteException();
    if (message != null) {
      e.message = message;
    }
    return e;
  }

  public static String toString(YarnRemoteException e) {
    return (e.message == null ? "" : e.message) + 
        (e.trace == null ? "" : "\n StackTrace: " + e.trace) +
        (e.cause == null ? "" : "\n Caused by: " + toString(e.cause));
        
  }
}
