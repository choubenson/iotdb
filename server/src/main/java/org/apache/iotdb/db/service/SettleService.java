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

import java.io.File;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SettleService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SettleService.class);

  private AtomicInteger threadCnt = new AtomicInteger();
  private ExecutorService settleThreadPool;
  private static AtomicInteger filesToBeSettledCount = new AtomicInteger();
  private PartialPath storageGroupPath;
  private String tsFilePath;

  public static SettleService getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final SettleService INSTANCE = new SettleService();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    try {
      int settleThreadNum = IoTDBDescriptor.getInstance().getConfig().getSettleThreadNum();
      settleThreadPool =
          Executors.newFixedThreadPool(
              settleThreadNum, r -> new Thread(r, "SettleThread-" + threadCnt.getAndIncrement()));
      startSettling();
    } catch (WriteProcessException | StorageEngineException e) {
      e.printStackTrace();
    }
  }

  public void startSettling()
      throws WriteProcessException, StorageEngineException {
      TsFileAndModSettleTool.findFilesToBeRecovered();
      boolean isSg=storageGroupPath==null?false:true;
      countSettleFiles(isSg);
      if (!SettleLog.createSettleLog() || filesToBeSettledCount.get() == 0) {
        stop();
        return;
      }
      settleAll();
  }

  @Override
  public void stop() {
    SettleLog.closeLogWriter();
    TsFileAndModSettleTool.clearRecoverSettleFileMap();
    setStorageGroupPath(null);
    setTsFilePath("");
    filesToBeSettledCount.set(0);
    if (settleThreadPool != null) {
      settleThreadPool.shutdownNow();
      logger.info("Waiting for settle task pool to shut down");
      settleThreadPool = null;
      logger.info("Settle service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SETTLE_SERVICE;
  }

  private void settleAll() throws StorageEngineException, WriteProcessException {
    logger.info(
        "Totally find "
            + getFilesToBeSettledCount()
            + " tsFiles to be settled, including "
            + TsFileAndModSettleTool.recoverSettleFileMap.size()
            + " tsFiles to be recovered.");
    StorageEngine.getInstance().settleAll(getStorageGroupPath());
  }

  public static AtomicInteger getFilesToBeSettledCount() {
    return filesToBeSettledCount;
  }

  private void countSettleFiles(boolean isSg) throws StorageEngineException {
    if (!isSg) {
      PartialPath sgPath= null;
      try {
        sgPath = new PartialPath(new File(getTsFilePath()).getParentFile().getParentFile().getParentFile().getName());
        setStorageGroupPath(sgPath);
      } catch (IllegalPathException e) {
        e.printStackTrace();
      }
    }
    filesToBeSettledCount.addAndGet(
        StorageEngine.getInstance().countSettleFiles(getStorageGroupPath(),getTsFilePath()));
  }

  public void submitSettleTask(SettleTask settleTask) {
    settleThreadPool.submit(settleTask);
  }

  /** This method is used to settle TsFile in the main thread. */
  public void settleTsFile(SettleTask settleTask) throws Exception {
    settleTask.settleTsFile();
  }

  public  PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  public void setStorageGroupPath(PartialPath storageGroupPath) {
    this.storageGroupPath = storageGroupPath;
  }

  public String getTsFilePath() {
    return tsFilePath;
  }

  public void setTsFilePath(String tsFilePath) {
    this.tsFilePath = tsFilePath;
  }
}
