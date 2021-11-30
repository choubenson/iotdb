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
package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CompactionRecoverCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * CompactionRecoverTask execute the recover process for all compaction task sequentially, including
 * InnerCompactionTask in sequence/unsequence space, CrossSpaceCompaction.
 */
public class CompactionRecoverTask implements Callable<Void> { // 这个是跨空间合并的恢复线程
  private static final Logger logger = LoggerFactory.getLogger(CompactionRecoverTask.class);
  private CompactionRecoverCallBack compactionRecoverCallBack;
  private TsFileManager tsFileManager;
  private String logicalStorageGroupName;
  private String virtualStorageGroupId;

  public CompactionRecoverTask(
      CompactionRecoverCallBack compactionRecoverCallBack,
      TsFileManager tsFileManager,
      String logicalStorageGroupName,
      String virtualStorageGroupId) {
    this.compactionRecoverCallBack = compactionRecoverCallBack;
    this.tsFileManager = tsFileManager;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupId = virtualStorageGroupId;
  }

  @Override
  public Void call() throws Exception {
    logger.info("recovering cross compaction");
    recoverCrossCompaction();
    logger.info("try to synchronize CompactionScheduler");
    CompactionScheduler.decPartitionCompaction(
        logicalStorageGroupName + "-" + virtualStorageGroupId, 0);
    compactionRecoverCallBack.call();
    logger.info(
        "recover task finish, current compaction thread is {}",
        CompactionTaskManager.getInstance().getExecutingTaskCount());
    return null;
  }

  private void recoverCrossCompaction() throws Exception {
    Set<Long> timePartitions = tsFileManager.getTimePartitions();
    List<String> sequenceDirs =
        DirectoryManager.getInstance().getAllSequenceFileFolders(); // 顺序目录列表，其实就一个，就是sequence
    for (String dir : sequenceDirs) {
      String storageGroupDir = // 虚拟存储组目录
          dir + File.separator + logicalStorageGroupName + File.separator + virtualStorageGroupId;
      for (Long timePartition : timePartitions) {
        String timePartitionDir = storageGroupDir + File.separator + timePartition; // 时间分区目录
        File[] compactionLogs = // 该分区目录下的所有合并日志文件
            InplaceCompactionLogger.findCrossSpaceCompactionLogs(timePartitionDir);
        for (File compactionLog : compactionLogs) {
          logger.info("calling cross compaction task");
          // 根据合并日志和对应的存储组创建跨空间合并恢复线程并执行合并的恢复，此处创建的其实是跨空间合并的任务InplaceCompactionRecoverTask
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getCrossCompactionStrategy()
              .getCompactionRecoverTask(
                  logicalStorageGroupName,
                  virtualStorageGroupId,
                  timePartition,
                  storageGroupDir,
                  tsFileManager.getSequenceListByTimePartition(timePartition),
                  tsFileManager.getUnsequenceListByTimePartition(timePartition),
                  1,
                  compactionLog)
              .call();
        }
      }
    }
  }
}
