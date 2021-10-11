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
package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class VirtualStorageGroupManager { // 虚拟存储组管理类
  // 每个真实的存储组StorageGroup会对应一个虚拟存储组管理类VirtualStorageGroupManager，用来管理该存储组下的一个个虚拟存储组。该类里会有虚拟存储组分割器VirtualPartitioner类对象用来根据该存储组下的不同deviceId的哈希值来分割出一个个虚拟存储组。

  /** logger of this class */
  private static final Logger logger = LoggerFactory.getLogger(VirtualStorageGroupManager.class);

  /** virtual storage group partitioner */
  VirtualPartitioner partitioner = HashVirtualPartitioner.getInstance(); // 虚拟存储组分割器

  /** all virtual storage group processor */
  StorageGroupProcessor[] virtualStorageGroupProcessor; // 存放了该存储组下的所有虚拟存储组对应的StorageGroupProcessor

  /**
   * recover status of each virtual storage group processor, null if this logical storage group is
   * new created
   */
  private AtomicBoolean[] isVsgReady;

  private AtomicBoolean isSettling = new AtomicBoolean();

  /** value of root.stats."root.sg".TOTAL_POINTS */
  private long monitorSeriesValue;

  public VirtualStorageGroupManager() {
    this(false);
  }

  public VirtualStorageGroupManager(boolean needRecovering) {
    virtualStorageGroupProcessor = new StorageGroupProcessor[partitioner.getPartitionCount()];
    isVsgReady = new AtomicBoolean[partitioner.getPartitionCount()];
    boolean recoverReady = !needRecovering;
    for (int i = 0; i < partitioner.getPartitionCount(); i++) {
      isVsgReady[i] = new AtomicBoolean(recoverReady);
    }
  }

  /** push forceCloseAllWorkingTsFileProcessors down to all sg */
  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.forceCloseAllWorkingTsFileProcessors();
      }
    }
  }

  /** push syncCloseAllWorkingTsFileProcessors down to all sg */
  public void syncCloseAllWorkingTsFileProcessors() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  /** push check ttl down to all sg */
  public void checkTTL() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.checkFilesTTL();
      }
    }
  }

  /** push check sequence memtable flush interval down to all sg */
  public void timedFlushSeqMemTable() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.timedFlushSeqMemTable();
      }
    }
  }

  /** push check unsequence memtable flush interval down to all sg */
  public void timedFlushUnseqMemTable() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.timedFlushUnseqMemTable();
      }
    }
  }

  /** push check TsFileProcessor close interval down to all sg */
  public void timedCloseTsFileProcessor() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.timedCloseTsFileProcessor();
      }
    }
  }

  /**
   * get processor from device id
   *
   * @param partialPath device path
   * @return virtual storage group processor
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  public StorageGroupProcessor
      getProcessor( // 根据设备ID计算其属于该真实存储组下的哪个虚拟存储组，并返回该虚拟存储组的StorageGroupProcessor
      PartialPath partialPath, IStorageGroupMNode storageGroupMNode)
          throws StorageGroupProcessorException, StorageEngineException {
    int loc = partitioner.deviceToVirtualStorageGroupId(partialPath); // 根据设备ID路径获得虚拟存储组ID

    StorageGroupProcessor processor =
        virtualStorageGroupProcessor[loc]; // 根据虚拟存储组ID获得对应的虚拟存储组的StorageGroupProcessor
    if (processor == null) {
      // if finish recover
      if (isVsgReady[loc].get()) {
        synchronized (storageGroupMNode) {
          processor = virtualStorageGroupProcessor[loc];
          if (processor == null) { // 如果虚拟存储组ID获得对应的虚拟存储组的StorageGroupProcessor为空
            processor = // 根据存储组路径对象、节点类对象和其下的某一虚拟存储组ID来创建该虚拟存储组的StorageGroupProcessor
                StorageEngine.getInstance()
                    .buildNewStorageGroupProcessor(
                        storageGroupMNode.getPartialPath(), storageGroupMNode, String.valueOf(loc));
            virtualStorageGroupProcessor[loc] =
                processor; // 将此虚拟存储组ID的新建的StorageGroupProcessor放入virtualStorageGroupProcessor里
          }
        }
      } else {
        // not finished recover, refuse the request
        throw new StorageEngineException(
            String.format(
                "the virtual storage group %s[%d] may not ready now, please wait and retry later",
                storageGroupMNode.getFullPath(), loc),
            TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
      }
    }

    return processor;
  }

  /**
   * async recover all virtual storage groups in this logical storage group
   *
   * @param storageGroupMNode logical sg mnode
   * @param pool thread pool to run virtual storage group recover task
   * @param futures virtual storage group recover tasks
   */
  public void asyncRecover(
      IStorageGroupMNode storageGroupMNode, ExecutorService pool, List<Future<Void>> futures) {
    for (int i = 0; i < partitioner.getPartitionCount(); i++) {
      int cur = i;
      Callable<Void> recoverVsgTask =
          () -> {
            isVsgReady[cur].set(false);
            StorageGroupProcessor processor = null;
            try {
              processor =
                  StorageEngine.getInstance()
                      .buildNewStorageGroupProcessor(
                          storageGroupMNode.getPartialPath(),
                          storageGroupMNode,
                          String.valueOf(cur));
            } catch (StorageGroupProcessorException e) {
              logger.error(
                  "failed to recover virtual storage group {}[{}]",
                  storageGroupMNode.getFullPath(),
                  cur,
                  e);
            }
            virtualStorageGroupProcessor[cur] = processor;
            isVsgReady[cur].set(true);
            return null;
          };
      futures.add(pool.submit(recoverVsgTask));
    }
  }

  public long getMonitorSeriesValue() {
    return monitorSeriesValue;
  }

  public void setMonitorSeriesValue(long monitorSeriesValue) {
    this.monitorSeriesValue = monitorSeriesValue;
  }

  public void updateMonitorSeriesValue(int successPointsNum) {
    this.monitorSeriesValue += successPointsNum;
  }

  /** push closeStorageGroupProcessor operation down to all virtual storage group processors */
  public void closeStorageGroupProcessor(boolean isSeq, boolean isSync) {
    for (StorageGroupProcessor processor : virtualStorageGroupProcessor) {
      if (processor == null) {
        continue;
      }

      if (logger.isInfoEnabled()) {
        logger.info(
            "{} closing sg processor is called for closing {}, seq = {}",
            isSync ? "sync" : "async",
            processor.getVirtualStorageGroupId() + "-" + processor.getLogicalStorageGroupName(),
            isSeq);
      }

      processor.writeLock("VirtualCloseStorageGroupProcessor-204");
      try {
        if (isSeq) {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor :
              new ArrayList<>(processor.getWorkSequenceTsFileProcessors())) {
            if (isSync) {
              processor.syncCloseOneTsFileProcessor(true, tsfileProcessor);
            } else {
              processor.asyncCloseOneTsFileProcessor(true, tsfileProcessor);
            }
          }
        } else {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor :
              new ArrayList<>(processor.getWorkUnsequenceTsFileProcessors())) {
            if (isSync) {
              processor.syncCloseOneTsFileProcessor(false, tsfileProcessor);
            } else {
              processor.asyncCloseOneTsFileProcessor(false, tsfileProcessor);
            }
          }
        }
      } finally {
        processor.writeUnlock();
      }
    }
  }

  /** push closeStorageGroupProcessor operation down to all virtual storage group processors */
  public void closeStorageGroupProcessor(long partitionId, boolean isSeq, boolean isSync) {
    for (StorageGroupProcessor processor : virtualStorageGroupProcessor) {
      if (processor != null) {
        logger.info(
            "async closing sg processor is called for closing {}, seq = {}, partitionId = {}",
            processor.getVirtualStorageGroupId() + "-" + processor.getLogicalStorageGroupName(),
            isSeq,
            partitionId);
        processor.writeLock("VirtualCloseStorageGroupProcessor-242");
        try {
          // to avoid concurrent modification problem, we need a new array list
          List<TsFileProcessor> processors =
              isSeq
                  ? new ArrayList<>(processor.getWorkSequenceTsFileProcessors())
                  : new ArrayList<>(processor.getWorkUnsequenceTsFileProcessors());
          for (TsFileProcessor tsfileProcessor : processors) {
            if (tsfileProcessor.getTimeRangeId() == partitionId) {
              if (isSync) {
                processor.syncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
              } else {
                processor.asyncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
              }
              break;
            }
          }
        } finally {
          processor.writeUnlock();
        }
      }
    }
  }

  /** push delete operation down to all virtual storage group processors */
  public void delete(
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
      TimePartitionFilter timePartitionFilter)
      throws
          IOException { // 此时已经确定了该时间序列的存储组，可是由于设备路径可能还处于未知（即是通配符，如path：root.ln.*.*.*）,因此不能确定具体的虚拟存储组，必须要遍历该存储组下的所有虚拟存储器
    for (StorageGroupProcessor storageGroupProcessor :
        virtualStorageGroupProcessor) { // 遍历该真实存储组下的每个虚拟存储组对应的StorageGroupProcessor
      if (storageGroupProcessor != null) {
        storageGroupProcessor.delete(
            path,
            startTime,
            endTime,
            planIndex,
            timePartitionFilter); // 利用虚拟存储组的storageGroupProcessor去执行删除操作
      }
    }
  }

  /** push countUpgradeFiles operation down to all virtual storage group processors */
  public int countUpgradeFiles() { // 使用该存储组的虚拟存储组管理类进行计算该存储组下待升级的TSFile文件数量
    int totalUpgradeFileNum = 0;
    for (StorageGroupProcessor storageGroupProcessor :
        virtualStorageGroupProcessor) { // 遍历使用该存储下的每个虚拟存储组计算对应的待升级的TSFile文件数量
      if (storageGroupProcessor != null) {
        totalUpgradeFileNum += storageGroupProcessor.countUpgradeFiles();
      }
    }

    return totalUpgradeFileNum;
  }

  /** push upgradeAll operation down to all virtual storage group processors */
  public void upgradeAll() { // 使用该存储组的虚拟存储组管理器进行升级文件
    for (StorageGroupProcessor storageGroupProcessor :
        virtualStorageGroupProcessor) { // 使用该存储组下的每个虚拟存储组的StorageGroupProcessor对象进行升级文件
      if (storageGroupProcessor != null) {
        storageGroupProcessor.upgrade();
      }
    }
  }

  public void getResourcesToBeSettled(
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.addSettleFilesToList(
            seqResourcesToBeSettled, unseqResourcesToBeSettled, tsFilePaths);
      }
    }
  }

  /** push mergeAll operation down to all virtual storage group processors */
  public void mergeAll(boolean isFullMerge) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.merge(isFullMerge);
      }
    }
  }

  /** push syncDeleteDataFiles operation down to all virtual storage group processors */
  public void syncDeleteDataFiles() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.syncDeleteDataFiles();
      }
    }
  }

  /** push setTTL operation down to all virtual storage group processors */
  public void setTTL(long dataTTL) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.setDataTTL(dataTTL);
      }
    }
  }

  /** push deleteStorageGroup operation down to all virtual storage group processors */
  public void deleteStorageGroup(String path) {
    for (StorageGroupProcessor processor : virtualStorageGroupProcessor) {
      if (processor != null) {
        processor.deleteFolder(path);
      }
    }
  }

  /** push getAllClosedStorageGroupTsFile operation down to all virtual storage group processors */
  public void getAllClosedStorageGroupTsFile(
      PartialPath storageGroupName, Map<PartialPath, Map<Long, List<TsFileResource>>> ret) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        List<TsFileResource> allResources = storageGroupProcessor.getSequenceFileTreeSet();
        allResources.addAll(storageGroupProcessor.getUnSequenceFileList());
        for (TsFileResource tsfile : allResources) {
          if (!tsfile.isClosed()) {
            continue;
          }
          long partitionNum = tsfile.getTimePartition();
          Map<Long, List<TsFileResource>> storageGroupFiles =
              ret.computeIfAbsent(storageGroupName, n -> new HashMap<>());
          storageGroupFiles.computeIfAbsent(partitionNum, n -> new ArrayList<>()).add(tsfile);
        }
      }
    }
  }

  /** push setPartitionVersionToMax operation down to all virtual storage group processors */
  public void setPartitionVersionToMax(long partitionId, long newMaxVersion) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.setPartitionFileVersionToMax(partitionId, newMaxVersion);
      }
    }
  }

  /** push removePartitions operation down to all virtual storage group processors */
  public void removePartitions(TimePartitionFilter filter) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.removePartitions(filter);
      }
    }
  }

  /**
   * push getWorkingStorageGroupPartitions operation down to all virtual storage group processors
   */
  public void getWorkingStorageGroupPartitions(
      String storageGroupName, Map<String, List<Pair<Long, Boolean>>> res) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        List<Pair<Long, Boolean>> partitionIdList = new ArrayList<>();
        for (TsFileProcessor tsFileProcessor :
            storageGroupProcessor.getWorkSequenceTsFileProcessors()) {
          Pair<Long, Boolean> tmpPair = new Pair<>(tsFileProcessor.getTimeRangeId(), true);
          partitionIdList.add(tmpPair);
        }

        for (TsFileProcessor tsFileProcessor :
            storageGroupProcessor.getWorkUnsequenceTsFileProcessors()) {
          Pair<Long, Boolean> tmpPair = new Pair<>(tsFileProcessor.getTimeRangeId(), false);
          partitionIdList.add(tmpPair);
        }

        res.put(storageGroupName, partitionIdList);
      }
    }
  }

  /** release resource of direct wal buffer */
  public void releaseWalDirectByteBufferPool() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.releaseWalDirectByteBufferPool();
      }
    }
  }

  /** only for test */
  public void reset() {
    Arrays.fill(virtualStorageGroupProcessor, null);
  }

  public void setSettling(boolean settling) {
    isSettling.set(settling);
  }

  public AtomicBoolean getIsSettling() {
    return isSettling;
  }
}
