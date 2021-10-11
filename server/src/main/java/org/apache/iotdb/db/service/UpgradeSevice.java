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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.db.engine.upgrade.UpgradeTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.UpgradeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class UpgradeSevice implements IService {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeSevice.class);

  private ExecutorService upgradeThreadPool; // 升级线程的线程池，该池里存放了一个个升级线程（这些线程可能是被占用也可能空闲）
  private AtomicInteger threadCnt = new AtomicInteger(); // 线程数量，默认是0
  // //AtomicInteger其实就是整数类型的数据，只不过它是线程安全的，主要用在高并发环境下的高效程序处理,来帮助我们简化同步处理.
  private static AtomicInteger cntUpgradeFileNum = new AtomicInteger(); // 待升级的TSFile文件数量

  private UpgradeSevice() {}

  public static UpgradeSevice getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final UpgradeSevice INSTANCE = new UpgradeSevice();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    int updateThreadNum =
        IoTDBDescriptor.getInstance().getConfig().getUpgradeThreadNum(); // 获取系统配置的升级线程的数量
    if (updateThreadNum <= 0) {
      updateThreadNum = 1;
    }
    upgradeThreadPool =
        Executors.newFixedThreadPool(
            updateThreadNum,
            r -> new Thread(r, "UpgradeThread-" + threadCnt.getAndIncrement())); // 创建升级线程池
    UpgradeLog.createUpgradeLog(); // 创建该系统本地的升级日志文件
    countUpgradeFiles(); // 计算该系统的待升级TSFile文件数量存入cntUpgradeFileNum变量中
    if (cntUpgradeFileNum.get() == 0) { // 若没有带升级的文件，则停止该服务
      stop();
      return;
    }
    upgradeAll(); // 进行升级所有的待升级TSFile
  }

  @Override
  public void stop() {
    UpgradeLog.closeLogWriter();
    UpgradeUtils.clearUpgradeRecoverMap();
    if (upgradeThreadPool != null) {
      upgradeThreadPool.shutdownNow();
      logger.info("Waiting for upgrade task pool to shut down");
      upgradeThreadPool = null;
      logger.info("Upgrade service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UPGRADE_SERVICE;
  }

  public static AtomicInteger getTotalUpgradeFileNum() {
    return cntUpgradeFileNum;
  }

  public void submitUpgradeTask(
      UpgradeTask upgradeTask) { // 往该“升级TSFile文件”服务的线程池里提交该升级线程upgradeTask
    upgradeThreadPool.submit(upgradeTask);
  }

  private static void countUpgradeFiles() { // 计算该IOTDB系统里待升级的TSFile文件数量
    cntUpgradeFileNum.addAndGet(
        StorageEngine.getInstance().countUpgradeFiles()); // 将cntUpgradeFileNum相加后再返回
    logger.info("finish counting upgrading files, total num:{}", cntUpgradeFileNum);
  }

  private static void upgradeAll() { // 升级所有待升级的TSFile
    try {
      StorageEngine.getInstance().upgradeAll();
    } catch (StorageEngineException e) {
      logger.error("Cannot perform a global upgrade because", e);
    }
  }
}
