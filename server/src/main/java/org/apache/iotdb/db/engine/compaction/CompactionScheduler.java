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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CompactionScheduler schedules and submits the compaction task periodically, and it counts the
 * total number of running compaction task. There are three compaction strategy: BALANCE,
 * INNER_CROSS, CROSS_INNER. Difference strategies will lead to different compaction preferences.
 * For different types of compaction task(e.g. InnerSpaceCompaction), CompactionScheduler will call
 * the corresponding {@link org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector
 * selector} according to the compaction machanism of the task(e.g. LevelCompaction,
 * SizeTiredCompaction), and the selection and submission process is carried out in the {@link
 * AbstractCompactionSelector#selectAndSubmit() selectAndSubmit()} in selector.
 */
// 此类是调度类，用于根据系统预设的合并优先级策略去选择待合并的一批批TsFile并为每一批文件创建一个合并任务（空间内合并、跨空间合并）线程放进CompactionTaskManager的等待队列里
public class CompactionScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // fullStorageGroupName -> timePartition -> compactionCount
  private static volatile Map<String, Map<Long, Long>>
      compactionCountInPartition = // 存放的是每个完整存储组（物理存储组+虚拟存储组）下的每个分区里当下正在合并的任务线程数量
      new ConcurrentHashMap<>();

  // 根据指定虚拟存储组下的TsFileManager和时间分区，根据系统预设的合并优先级策略选取文件创建一个或多个合并任务线程并放入CompactionTaskManager的等待队列里
  public static void scheduleCompaction(TsFileManager tsFileManager, long timePartition) {
    tsFileManager.readLock();
    try {
      TsFileResourceList sequenceFileList = // 该存储组下顺序文件列表 //Todo:此处是空的
          tsFileManager.getSequenceListByTimePartition(timePartition);
      TsFileResourceList unsequenceFileList = // 该存储组下乱序文件列表  //Todo:此处是空的
          tsFileManager.getUnsequenceListByTimePartition(timePartition);
      CompactionPriority compactionPriority = config.getCompactionPriority();
      if (compactionPriority == CompactionPriority.BALANCE) {
        doCompactionBalancePriority(
            tsFileManager.getStorageGroupName(),
            tsFileManager.getVirtualStorageGroup(),
            tsFileManager.getStorageGroupDir(),
            timePartition,
            tsFileManager,
            sequenceFileList,
            unsequenceFileList);
      } else if (compactionPriority == CompactionPriority.INNER_CROSS) {
        doCompactionInnerCrossPriority(
            tsFileManager.getStorageGroupName(),
            tsFileManager.getVirtualStorageGroup(),
            tsFileManager.getStorageGroupDir(),
            timePartition,
            tsFileManager,
            sequenceFileList,
            unsequenceFileList);
      } else if (compactionPriority == CompactionPriority.CROSS_INNER) {
        doCompactionCrossInnerPriority(
            tsFileManager.getStorageGroupName(),
            tsFileManager.getVirtualStorageGroup(),
            tsFileManager.getStorageGroupDir(),
            timePartition,
            tsFileManager,
            sequenceFileList,
            unsequenceFileList);
      }
    } finally {
      tsFileManager.readUnlock();
    }
  }

  private static void doCompactionBalancePriority(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    boolean taskSubmitted = true;
    // 系统预设的合并任务线程的最大并行数量
    int concurrentCompactionThread = config.getConcurrentCompactionThread();
    while (taskSubmitted
        && CompactionTaskManager.getInstance().getExecutingTaskCount()
            < concurrentCompactionThread) {
      taskSubmitted =
          tryToSubmitInnerSpaceCompactionTask(
              logicalStorageGroupName,
              virtualStorageGroupName,
              timePartition,
              tsFileManager,
              sequenceFileList,
              true,
              new InnerSpaceCompactionTaskFactory());
      taskSubmitted =
          tryToSubmitInnerSpaceCompactionTask(
                  logicalStorageGroupName,
                  virtualStorageGroupName,
                  timePartition,
                  tsFileManager,
                  unsequenceFileList,
                  false,
                  new InnerSpaceCompactionTaskFactory())
              | taskSubmitted;
      taskSubmitted =
          tryToSubmitCrossSpaceCompactionTask(
                  logicalStorageGroupName,
                  virtualStorageGroupName,
                  storageGroupDir,
                  timePartition,
                  sequenceFileList,
                  unsequenceFileList,
                  new CrossSpaceCompactionTaskFactory())
              | taskSubmitted;
    }
  }

  private static void doCompactionInnerCrossPriority(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        sequenceFileList,
        true,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        unsequenceFileList,
        false,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitCrossSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        storageGroupDir,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        new CrossSpaceCompactionTaskFactory());
  }

  private static void doCompactionCrossInnerPriority(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    tryToSubmitCrossSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        storageGroupDir,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        new CrossSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        sequenceFileList,
        true,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        unsequenceFileList,
        false,
        new InnerSpaceCompactionTaskFactory());
  }

  // 根据给定的TsFile列表创建文件选择器，并从该虚拟存储组下该分区的所有顺序或者乱序文件里从0层（空间内合并的层数）开始至最高层依次寻找所有文件，当文件数量或者大小到达系统预设值则放入任务队列里，然后为这些队列里的每个文件列表创建一个合并任务线程并放入CompactionTaskManager的合并任务等待队列里
  public static boolean tryToSubmitInnerSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    if ((!config.isEnableSeqSpaceCompaction() && sequence)
        || (!config.isEnableUnseqSpaceCompaction() && !sequence)) {
      return false;
    }

    // 获取InnerCompaction空间内合并对应的文件选择器
    AbstractInnerSpaceCompactionSelector innerSpaceCompactionSelector =
        config
            .getInnerCompactionStrategy()
            .getCompactionSelector(
                logicalStorageGroupName,
                virtualStorageGroupName,
                timePartition,
                tsFileManager,
                tsFileResources,
                sequence,
                taskFactory);
    return innerSpaceCompactionSelector
        .selectAndSubmit(); // 从该虚拟存储组下该分区的所有顺序或者乱序文件里从0层（空间内合并的层数）开始至最高层依次寻找所有文件，当文件数量或者大小到达系统预设值则放入任务队列里，然后为这些队列里的每个文件列表创建一个合并任务线程并放入CompactionTaskManager的合并任务管理队列里
  }

  private static boolean tryToSubmitCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      CrossSpaceCompactionTaskFactory taskFactory) {
    if (!config.isEnableCrossSpaceCompaction()) {
      return false;
    }
    // 获取CrossCompaction跨空间合并对应的文件选择器
    AbstractCrossSpaceCompactionSelector crossSpaceCompactionSelector =
        config
            .getCrossCompactionStrategy()
            .getCompactionSelector(
                logicalStorageGroupName,
                virtualStorageGroupName,
                storageGroupDir,
                timePartition,
                sequenceFileList,
                unsequenceFileList,
                taskFactory);
    return crossSpaceCompactionSelector.selectAndSubmit();
  }

  public static Map<String, Map<Long, Long>> getCompactionCountInPartition() {
    return compactionCountInPartition;
  }

  // 往当前存储组的当前时间分区里的合并线程数量加1
  public static void addPartitionCompaction(String fullStorageGroupName, long timePartition) {
    synchronized (compactionCountInPartition) {
      compactionCountInPartition
          .computeIfAbsent(fullStorageGroupName, l -> new HashMap<>())
          .put(
              timePartition,
              compactionCountInPartition.get(fullStorageGroupName).getOrDefault(timePartition, 0L)
                  + 1);
    }
  }

  public static void decPartitionCompaction(String fullStorageGroupName, long timePartition) {
    synchronized (compactionCountInPartition) {
      if (!compactionCountInPartition.containsKey(fullStorageGroupName)
          || !compactionCountInPartition.get(fullStorageGroupName).containsKey(timePartition)) {
        return;
      }
      compactionCountInPartition
          .get(fullStorageGroupName)
          .put(
              timePartition,
              compactionCountInPartition.get(fullStorageGroupName).get(timePartition) - 1);
    }
  }

  // 判断该全路径存储器（物理存储组+虚拟存储组）下的该时间分区里是否正在合并文件
  public static boolean isPartitionCompacting(String fullStorageGroupName, long timePartition) {
    synchronized (compactionCountInPartition) {
      return compactionCountInPartition
              .computeIfAbsent(fullStorageGroupName, l -> new HashMap<>())
              .getOrDefault(timePartition, 0L)
          > 0L;
    }
  }
}
