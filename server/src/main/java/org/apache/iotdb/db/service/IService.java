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
package org.apache.iotdb.db.service;

import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.StartupException;

public interface IService { //服务类对象

  /** Start current service. */
  void start() throws StartupException; //启动该服务

  /**
   * Stop current service. If current service uses thread or thread pool, current service should
   * guarantee to putBack thread or thread pool.
   */
  void stop();  //停止该服务，如果该服务有自己的线程池，则应该把该服务线程丢尽对应的池里

  default void waitAndStop(long milliseconds) {     //在milliseconds时间后停止该服务
    stop();
  }

  default void shutdown(long milliseconds) throws ShutdownException {
    waitAndStop(milliseconds);
  }

  /**
   * Get the name of the the service.
   *
   * @return current service name
   */
  ServiceType getID();
}
