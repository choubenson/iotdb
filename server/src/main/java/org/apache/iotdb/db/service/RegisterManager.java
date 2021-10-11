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
import org.apache.iotdb.db.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RegisterManager { // 服务注册管理类，用于管理服务的注册并运行和注销并停止运行

  private static final Logger logger = LoggerFactory.getLogger(RegisterManager.class);
  private List<IService> iServices; // 存放了该IOTDB在运行的所有服务
  private static long deregisterTimeOut = 10_000L;

  public RegisterManager() {
    iServices = new ArrayList<>();
  }

  /** register service. */
  public void register(IService service) throws StartupException { // 往该管理类里注册新的服务，并运行该新的服务
    for (IService s : iServices) { // 循环遍历判断该服务是否已经注册过
      if (s.getID() == service.getID()) {
        logger.debug("{} has already been registered. skip", service.getID().getName());
        return;
      }
    }
    iServices.add(service); // 若没注册过，说明是一个新的服务，则把它加进服务列表中
    service.start(); // 启动该服务
  }

  /** stop all service and clear iService list. */
  public void deregisterAll() { // 停止所有的服务，并清空服务列表
    // we stop JMXServer at last
    Collections.reverse(iServices); // 反转服务列表里的元素
    for (IService service : iServices) {
      try {
        service.waitAndStop(deregisterTimeOut); // 在milliseconds时间后停止该服务
        logger.debug("{} deregistered", service.getID());
      } catch (Exception e) {
        logger.error("Failed to stop {} because:", service.getID().getName(), e);
      }
    }
    iServices.clear(); // 清空服务列表
    logger.info("deregister all service.");
  }

  /** stop all service and clear iService list. */
  public void shutdownAll() throws ShutdownException {
    // we stop JMXServer at last
    Collections.reverse(iServices);
    for (IService service : iServices) {
      service.shutdown(deregisterTimeOut);
    }
    iServices.clear();
    logger.info("deregister all service.");
  }

  @TestOnly
  public static void setDeregisterTimeOut(long deregisterTimeOut) {
    RegisterManager.deregisterTimeOut = deregisterTimeOut;
  }
}
