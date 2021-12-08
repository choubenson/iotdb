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
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.SOURCE_INFO;
import static org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger.TARGET_INFO;

/**
 * SizeTiredCompactionTask compact several inner space files selected by {@link
 * SizeTieredCompactionSelector} into one file.
 */
public class SizeTieredCompactionTask extends AbstractInnerSpaceCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  protected TsFileResourceList tsFileResourceList;
  protected TsFileManager tsFileManager;

  public SizeTieredCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartition,
        currentTaskNum,
        sequence,
        selectedTsFileResourceList);
    this.tsFileResourceList = tsFileResourceList;
    this.tsFileManager = tsFileManager;
  }

  //将所有的待合并文件在合并过程中产生的删除.compaction.mods文件里的内容读取出来写到目标文件的新.mods文件里
  public static void combineModsInCompaction(
      Collection<TsFileResource> mergeTsFiles, TsFileResource targetTsFile) throws IOException {
    //存放所有的待合并文件在合并过程中产生的删除操作
    List<Modification> modifications = new ArrayList<>();
    //遍历每个待合并文件
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      //创建该待合并文件所属的合并删除文件，为（xxx.tsfile.compaction.mods）
      try (ModificationFile sourceCompactionModificationFile =
          ModificationFile.getCompactionMods(mergeTsFile)) {
        //获取该旧的待合并文件在合并过程中的.compaction.mods文件里的所有删除操作加入列表里
        modifications.addAll(sourceCompactionModificationFile.getModifications());
        //删除.compaction.mods
        if (sourceCompactionModificationFile.exists()) {
          sourceCompactionModificationFile.remove();
        }
      }
      //获取该旧的待合并文件对应的.mods文件，若存在则删除
      ModificationFile sourceModificationFile = ModificationFile.getNormalMods(mergeTsFile);
      if (sourceModificationFile.exists()) {
        sourceModificationFile.remove();
      }
    }
    if (!modifications.isEmpty()) {
      //创建目标文件对应的新的.mods文件，并把合并过程中产生的所有删除操作写入到该文件里
      try (ModificationFile modificationFile = ModificationFile.getNormalMods(targetTsFile)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  @Override
  protected void doCompaction() throws Exception {
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    String targetFileName = // 获取目标新的文件名
        // 根据待合并的文件列表和是否顺序创建对应合并的新的一个TsFile，命名规则是：（1）若是顺序文件的空间内合并，则新生成的文件是“最小时间戳-最小版本号-空间内合并数+1-跨空间合并数”（2）若是乱序文件的空间内合并，则新生成的文件是“最大时间戳-最大版本号-空间内合并数+1-跨空间合并数”
        TsFileNameGenerator.getInnerCompactionFileName(selectedTsFileResourceList, sequence)
            .getName();
    TsFileResource targetTsFileResource = // 目标新的TsFileResource
        new TsFileResource(new File(dataDirectory + File.separator + targetFileName));
    LOGGER.info(
        "{} [Compaction] starting compaction task with {} files",
        fullStorageGroupName,
        selectedTsFileResourceList.size());
    File logFile = null;
    try {
      logFile = // 新目标文件对应的合并日志文件，“新目标文件名.compaction.log”
          new File(
              dataDirectory
                  + File.separator
                  + targetFileName
                  + SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
      SizeTieredCompactionLogger sizeTieredCompactionLogger = // 创建该合并任务对应的日志类，用于后续写入日志
          new SizeTieredCompactionLogger(logFile.getPath());
      for (TsFileResource resource : selectedTsFileResourceList) {
        // 往合并日志里先写入前缀"source_info"，再写入该待合并TsFile的重要属性（物理存储组名、虚拟存储组名、时间分区、是否顺序、文件名）
        sizeTieredCompactionLogger.logFileInfo(SOURCE_INFO, resource.getTsFile());
      }
      //往合并日志写入是否顺序
      sizeTieredCompactionLogger.logSequence(sequence);
      // 往日志里写入目标新文件的相关信息
      sizeTieredCompactionLogger.logFileInfo(TARGET_INFO, targetTsFileResource.getTsFile());
      LOGGER.info(
          "{} [Compaction] compaction with {}", fullStorageGroupName, selectedTsFileResourceList);
      // carry out the compaction
      //执行具体的空间内合并
      InnerSpaceCompactionUtils.compact(
          targetTsFileResource, selectedTsFileResourceList, fullStorageGroupName, true);
      LOGGER.info(
          "{} [SizeTiredCompactionTask] compact finish, close the logger", fullStorageGroupName);
      sizeTieredCompactionLogger.close();

      LOGGER.info(
          "{} [Compaction] compaction finish, start to delete old files", fullStorageGroupName);
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException(
            String.format("%s [Compaction] abort", fullStorageGroupName));
      }
      // get write lock for TsFileResource list with timeout
      try {
        tsFileManager.writeLockWithTimeout("size-tired compaction", 60_000);
      } catch (WriteLockFailedException e) {
        // if current compaction thread couldn't get writelock
        // a WriteLockFailException will be thrown, then terminate the thread itself
        LOGGER.warn(
            "{} [SizeTiredCompactionTask] failed to get write lock, abort the task and delete the target file {}",
            fullStorageGroupName,
            targetTsFileResource.getTsFile(),
            e);
        targetTsFileResource.getTsFile().delete();
        logFile.delete();
        throw new InterruptedException(
            String.format(
                "%s [Compaction] compaction abort because cannot acquire write lock",
                fullStorageGroupName));
      }
      try {
        // replace the old files with new file, the new is in same position as the old
        //合并完后从TsFileResourceManager里移除所有被合并的TsFile的TsFileResource
        for (TsFileResource resource : selectedTsFileResourceList) {
          TsFileResourceManager.getInstance().removeTsFileResource(resource);
        }
        //新的合并后的目标文件的TsFileResource，把他放到tsFileResourceList列表里合适的位置，并把他注册到TsFileResourceManager里
        tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
        TsFileResourceManager.getInstance().registerSealedTsFileResource(targetTsFileResource);
        //从tsFileResourceList列表里移除所有的待合并文件的TsFileResource
        for (TsFileResource resource : selectedTsFileResourceList) {
          tsFileResourceList.remove(resource);
        }
      } finally {
        tsFileManager.writeUnlock();
      }
      // delete the old files
      //移除并关闭指定TsFile文件的顺序阅读器，并删除所有TsFile的本地文件
      InnerSpaceCompactionUtils.deleteTsFilesInDisk(
          selectedTsFileResourceList, fullStorageGroupName);
      LOGGER.info(
          "{} [SizeTiredCompactionTask] old file deleted, start to rename mods file",
          fullStorageGroupName);
      //将所有的待合并文件在合并过程中产生的删除.compaction.mods文件里的内容读取出来写到目标文件的新.mods文件里
      combineModsInCompaction(selectedTsFileResourceList, targetTsFileResource);
      long costTime = System.currentTimeMillis() - startTime;
      LOGGER.info(
          "{} [SizeTiredCompactionTask] all compaction task finish, target file is {},"
              + "time cost is {} s",
          fullStorageGroupName,
          targetFileName,
          costTime / 1000);
      //删除合并日志文件
      if (logFile.exists()) {
        logFile.delete();
      }
    } finally {
      //把每个选中待合并文件的TsFileResource的isMerge变量设为false
      for (TsFileResource resource : selectedTsFileResourceList) {
        resource.setMerging(false);
      }
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof SizeTieredCompactionTask) {
      SizeTieredCompactionTask otherSizeTieredTask = (SizeTieredCompactionTask) other;
      if (!selectedTsFileResourceList.equals(otherSizeTieredTask.selectedTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    long minVersionNum = Long.MAX_VALUE;
    try {
      // 判断合并任务里的所有文件状态是否合格（即当文件正在被合并、文件还未被封口、本地文件不存在则说明该任务线程是不合格）
      for (TsFileResource resource : selectedTsFileResourceList) {
        if (resource.isMerging() | !resource.isClosed()
            || !resource.getTsFile().exists()) { // Todo:bug
          return false;
        }
        TsFileNameGenerator.TsFileName tsFileName =
            TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        if (tsFileName.getVersion() < minVersionNum) {
          minVersionNum = tsFileName.getVersion();
        }
      }
    } catch (IOException e) {
      LOGGER.error("CompactionTask exists while check valid", e);
    }
    for (TsFileResource resource : selectedTsFileResourceList) {
      resource.setMerging(true);
    }
    return true;
  }
}
