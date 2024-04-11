/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.exception;

import org.apache.iotdb.commons.conf.CommonDescriptor;

public class TTLException extends Exception {

  public TTLException(String path) {
    super(
        String.format(
            "Illegal pattern path: %s, pattern path should end with **, otherwise, it should be a specific database or device path without *",
            path));
  }

  public TTLException() {
    super(
        String.format(
            "The number of TTL stored in the system has reached threshold %d, please increase the ttl_count parameter.",
            CommonDescriptor.getInstance().getConfig().getTTLCountThreshold()));
  }
}