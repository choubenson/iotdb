/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.exception.WriteProcessException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableManager {    //内存memtable管理类

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final Logger logger = LoggerFactory.getLogger(MemTableManager.class);

  private static final int WAIT_TIME = 100;
  public static final int MEMTABLE_NUM_FOR_EACH_PARTITION = 4;  //给每个时间分区分配的内存memtable数量
  private int currentMemtableNumber = 0;

  private MemTableManager() {}

  public static MemTableManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Called when memory control is disabled
   *
   * @throws WriteProcessException
   */
  public synchronized IMemTable getAvailableMemTable(String storageGroup)   //当系统没开启内存控制时，就调用此方法来获得系统内还可用的memtable，该方法对系统的memtable进行数量控制，进而侧面实现了内存控制。
      throws WriteProcessException {
    if (!reachMaxMemtableNumber()) {//判断memtable的数量是否达到或超过系统配置的允许最大数量，若没有则
      currentMemtableNumber++;    //memtable数量+1
      return new PrimitiveMemTable(); //返回一个新的memtable
    }

    //如果memtable的数量是否达到或超过系统配置的允许最大数量，则进行循环等待
    // wait until the total number of memtable is less than the system capacity
    int waitCount = 1;  //计算线程循环等待的次数
    while (true) {
      if (!reachMaxMemtableNumber()) {  //如果memtable数量小于系统配置的最大数量，则允许新建一个并返回
        currentMemtableNumber++;
        return new PrimitiveMemTable();
      }
      try {
        wait(WAIT_TIME);  //该线程等待WAIT_TIME时间
      } catch (InterruptedException e) {
        logger.error("{} fails to wait for memtables {}, continue to wait", storageGroup, e);
        Thread.currentThread().interrupt();
        throw new WriteProcessException(e);
      }
      if (waitCount++ % 10 == 0) {  //每循环等待10次就输出日志
        logger.info("{} has waited for a memtable for {}ms", storageGroup, waitCount * WAIT_TIME);
      }
    }
  }

  public int getCurrentMemtableNumber() {
    return currentMemtableNumber;
  }

  public synchronized void addMemtableNumber() {
    currentMemtableNumber++;
  }

  public synchronized void decreaseMemtableNumber() {
    currentMemtableNumber--;
    notifyAll();
  }

  /** Called when memory control is disabled */
  private boolean reachMaxMemtableNumber() {    //判断memtable的数量是否达到或超过系统配置的允许最大数量
    return currentMemtableNumber >= CONFIG.getMaxMemtableNumber();
  }

  /** Called when memory control is disabled */
  public synchronized void addOrDeleteStorageGroup(int diff) {
    int maxMemTableNum = CONFIG.getMaxMemtableNumber();
    maxMemTableNum +=
        MEMTABLE_NUM_FOR_EACH_PARTITION * CONFIG.getConcurrentWritingTimePartition() * diff;
    CONFIG.setMaxMemtableNumber(maxMemTableNum);
    notifyAll();
  }

  public synchronized void close() {
    currentMemtableNumber = 0;
  }

  private static class InstanceHolder {

    private static final MemTableManager INSTANCE = new MemTableManager();

    private InstanceHolder() {}
  }
}
