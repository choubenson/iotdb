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
package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask.MERGE_SUFFIX;

public class InplaceCompactionTask extends AbstractCrossSpaceCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  // 跨空间合并的资源管理器
  protected CrossSpaceMergeResource mergeResource;
  protected String storageGroupDir;
  //Todo:重复了，此处是选中的待合并顺序和乱序文件
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  //该存储组该时间分区下的顺序和乱序文件列表
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unSeqTsFileResourceList;
  //跨空间 允许并行合并序列的线程数量
  protected int concurrentMergeCount;
  protected String logicalStorageGroupName;
  protected String virtualStorageGroupName;

  public InplaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      CrossSpaceMergeResource mergeResource,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      int concurrentMergeCount,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.mergeResource = mergeResource;
    this.storageGroupDir = storageGroupDir;
    this.seqTsFileResourceList = seqTsFileResourceList;
    this.unSeqTsFileResourceList = unSeqTsFileResourceList;
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
    this.concurrentMergeCount = concurrentMergeCount;
  }

  @Override
  protected void doCompaction() throws Exception {
    //此处是 logicalStorageGroupName + "-" + virtualStorageGroupName+"-"+currentTimestamps，比如"root.sg-0-1000210"
    String taskName = fullStorageGroupName + "-" + System.currentTimeMillis();
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            mergeResource,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            concurrentMergeCount,
            logicalStorageGroupName);
    mergeTask.call();
  }

  //合并任务完成后的回调函数：
  //1. 删除此次合并任务里所有乱序文件对应的本地tsfile文件、.resource文件和.mods文件
  //2. 遍历每个待合并顺序文件：（1）删除该顺序文件的临时目标文件（若合并过程出问题，则该顺序文件对应的临时目标文件可能没有被删除）（2）删除该已合并顺序文件的原有mods文件，并先后往合并过程中对该顺序文件和所有乱序文件产生的删除操作写到该已合并顺序文件的新mods文件
  //3. 删除所有合并完的顺序和乱序文件对应的.compaction.mods文件
  //4. 删除合并日志
  public void mergeEndAction(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    // todo: add
    LOGGER.info("{} a merge task is ending...", fullStorageGroupName);

    if (Thread.currentThread().isInterrupted() || unseqFiles.isEmpty()) {
      // merge task abort, or merge runtime exception arose, just end this merge
      LOGGER.info("{} a merge task abnormally ends", fullStorageGroupName);
      return;
    }
    //删除此次合并任务里所有乱序文件对应的本地tsfile文件、.resource文件和.mods文件
    removeUnseqFiles(unseqFiles);

    //遍历每个待合并顺序文件
    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      //1） get both seqFile lock and merge lock
      doubleWriteLock(seqFile);

      try {
        //2） 若合并过程出问题，则该顺序文件对应的临时目标文件可能没有被删除
        // if meet error(like file not found) in merge task, the .merge file may not be deleted
        File mergedFile =
            FSFactoryProducer.getFSFactory().getFile(seqFile.getTsFilePath() + MERGE_SUFFIX);
        if (mergedFile.exists()) {
          if (!mergedFile.delete()) {
            LOGGER.warn("Delete file {} failed", mergedFile);
          }
        }
        //3）删除该已合并顺序文件的原有mods文件，并先后往合并过程中对该顺序文件和所有乱序文件产生的删除操作写到该已合并顺序文件的新mods文件
        updateMergeModification(seqFile, unseqFiles);
      } finally {
        //4）释放两个锁
        doubleWriteUnlock(seqFile);
      }
    }

    try {
      //删除所有合并完的顺序和乱序文件对应的.compaction.mods文件
      removeMergingModification(seqFiles, unseqFiles);
      //删除合并日志
      Files.delete(mergeLog.toPath());
    } catch (IOException e) {
      LOGGER.error(
          "{} a merge task ends but cannot delete log {}", fullStorageGroupName, mergeLog.toPath());
    }

    LOGGER.info("{} a merge task ends", fullStorageGroupName);
  }

  //删除此次合并任务里所有乱序文件对应的本地tsfile文件、.resource文件和.mods文件
  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
    unSeqTsFileResourceList.writeLock();
    try {
      for (TsFileResource unSeqFileMerged : selectedUnSeqTsFileResourceList) {
        unSeqTsFileResourceList.remove(unSeqFileMerged);
      }
    } finally {
      unSeqTsFileResourceList.writeUnlock();
    }

    for (TsFileResource unseqFile : unseqFiles) {
      unseqFile.writeLock();
      try {
        unseqFile.remove();
      } finally {
        unseqFile.writeUnlock();
      }
    }
  }

  /** acquire the write locks of the resource , the merge lock and the compaction lock */
  private void doubleWriteLock(TsFileResource seqFile) {
    boolean fileLockGot;
    boolean compactionLockGot;
    while (true) {
      fileLockGot = seqFile.tryWriteLock();
      compactionLockGot = seqTsFileResourceList.tryWriteLock();

      if (fileLockGot && compactionLockGot) {
        break;
      } else {
        // did not get all of them, release the gotten one and retry
        if (compactionLockGot) {
          seqTsFileResourceList.writeUnlock();
        }
        if (fileLockGot) {
          seqFile.writeUnlock();
        }
      }
    }
  }

  private void doubleWriteUnlock(TsFileResource seqFile) {
    seqTsFileResourceList.writeUnlock();
    seqFile.writeUnlock();
  }

  //删除该已合并顺序文件的原有mods文件，并先后往合并过程中对该顺序文件和所有乱序文件产生的删除操作写到该已合并顺序文件的新mods文件
  private void updateMergeModification(TsFileResource seqFile, List<TsFileResource> unseqFiles) {
    try {
      // remove old modifications and write modifications generated during merge
      //删除已合并的顺序文件的旧mods文件
      seqFile.removeModFile();
      //将该已合并顺序文件在合并过程产生的删除错做写到其对应的新的.mods文件里
      ModificationFile compactionModificationFile = ModificationFile.getCompactionMods(seqFile);
      for (Modification modification : compactionModificationFile.getModifications()) {
        seqFile.getModFile().write(modification);
      }

      for (TsFileResource unseqFile : unseqFiles) {
        ModificationFile compactionUnseqModificationFile =
            ModificationFile.getCompactionMods(unseqFile);
        for (Modification modification : compactionUnseqModificationFile.getModifications()) {
          seqFile.getModFile().write(modification);
        }
      }
      try {
        seqFile.getModFile().close();
      } catch (IOException e) {
        LOGGER.error("Cannot close the ModificationFile {}", seqFile.getModFile().getFilePath(), e);
      }
    } catch (IOException e) {
      LOGGER.error(
          "{} cannot clean the ModificationFile of {} after cross space merge",
          fullStorageGroupName,
          seqFile.getTsFile(),
          e);
    }
  }

  //删除所有合并完的顺序和乱序文件对应的.compaction.mods文件
  private void removeMergingModification(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    try {
      for (TsFileResource seqFile : seqFiles) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
      for (TsFileResource unseqFile : unseqFiles) {
        ModificationFile.getCompactionMods(unseqFile).remove();
      }
    } catch (IOException e) {
      LOGGER.error("{} cannot remove merging modification ", fullStorageGroupName, e);
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof InplaceCompactionTask) {
      InplaceCompactionTask otherTask = (InplaceCompactionTask) other;
      if (!otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          || !otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }
}
