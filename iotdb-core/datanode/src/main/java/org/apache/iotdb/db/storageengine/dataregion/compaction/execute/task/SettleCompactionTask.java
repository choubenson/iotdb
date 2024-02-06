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
package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This settle task contains all_deleted files and partial_deleted files. The partial_deleted files
 * are divided into several groups, each group may contain one or several files. This task will do
 * the following two things respectively: 1. Settle all all_deleted files by deleting them directly.
 * 2. Settle partial_deleted files: put the files of each partial_deleted group into an invisible
 * innerCompactionTask, and then perform the cleanup work. The source files in a file group will be
 * compacted into a target file.
 */
public class SettleCompactionTask extends InnerSpaceCompactionTask {
  private List<TsFileResource> allDeletedFiles;
  private double allDeletedFileSize = 0;
  private double partialDeletedFileSize = 0;

  private int allDeletedSuccessNum = 0;

  private long totalModsFileSize;

  public SettleCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> allDeletedFiles,
      List<TsFileResource> partialDeletedFiles,
      boolean isSequence,
      ICompactionPerformer performer,
      long serialId) {
    super(timePartition, tsFileManager, partialDeletedFiles, isSequence, performer, serialId, true);
    this.allDeletedFiles = allDeletedFiles;
    allDeletedFiles.forEach(x -> allDeletedFileSize += x.getTsFileSize());
    partialDeletedFiles.forEach(
        x -> {
          partialDeletedFileSize += x.getTsFileSize();
          totalModsFileSize += x.getModFile().getSize();
        });
    this.hashCode = this.toString().hashCode();
  }

  public SettleCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, tsFileManager, logFile);
    settleFlag = true;
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allSourceFiles = new ArrayList<>(allDeletedFiles);
    allSourceFiles.addAll(selectedTsFileResourceList);
    return allSourceFiles;
  }

  @Override
  protected boolean doCompaction() {
    recoverMemoryStatus = true;
    boolean isSuccess;

    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    if (allDeletedFiles.isEmpty() && selectedTsFileResourceList.isEmpty()) {
      LOGGER.info(
          "{}-{} [Compaction] Settle compaction file list is empty, end it",
          storageGroupName,
          dataRegionId);
    }
    long startTime = System.currentTimeMillis();

    LOGGER.info(
        "{}-{} [Compaction] SettleCompaction task starts with {} all_deleted files "
            + "and {} partial_deleted files. "
            + "All_deleted files : {}, partial_deleted files : {} . "
            + "All_deleted files size is {} MB, "
            + "partial_deleted file size is {} MB. "
            + "Memory cost is {} MB.",
        storageGroupName,
        dataRegionId,
        allDeletedFiles.size(),
        selectedTsFileResourceList.size(),
        allDeletedFiles,
        selectedTsFileResourceList,
        allDeletedFileSize / 1024 / 1024,
        partialDeletedFileSize / 1024 / 1024,
        memoryCost == 0 ? 0 : (double) memoryCost / 1024 / 1024);

    List<TsFileResource> allSourceFiles = getAllSourceTsFiles();
    logFile =
        new File(
            allSourceFiles.get(0).getTsFile().getAbsolutePath()
                + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.logEmptyTargetFiles(allDeletedFiles);
      compactionLogger.logSourceFiles(selectedTsFileResourceList);
      if (!selectedTsFileResourceList.isEmpty()) {
        targetTsFileResource =
            TsFileNameGenerator.getSettleCompactionTargetFileResources(
                selectedTsFileResourceList, sequence);
        compactionLogger.logTargetFile(targetTsFileResource);
      }
      compactionLogger.force();

      isSuccess = settleWithAllDeletedFiles();
      // In order to prevent overlap of sequence files after settle task, partial_deleted files can
      // only be settled after all_deleted files are settled successfully, because multiple
      // partial_deleted files will be settled into one file.
      if (isSuccess) {
        settleWithPartialDeletedFiles(compactionLogger);
      }

      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      if (isSuccess) {
        LOGGER.info(
            "{}-{} [Compaction] SettleCompaction task finishes successfully, time cost is {} s, compaction speed is {} MB/s."
                + "All_Deleted files num is {} and partial_Deleted files num is {}.",
            storageGroupName,
            dataRegionId,
            String.format("%.2f", costTime),
            String.format(
                "%.2f",
                (allDeletedFileSize + partialDeletedFileSize) / 1024.0d / 1024.0d / costTime),
            allDeletedFiles.size(),
            selectedTsFileResourceList.size());
      } else {
        LOGGER.info(
            "{}-{} [Compaction] SettleCompaction task finishes with some error, time cost is {} s."
                + "All_Deleted files num is {} and there are {} files fail to delete.",
            storageGroupName,
            dataRegionId,
            String.format("%.2f", costTime),
            allDeletedFiles.size(),
            allSourceFiles.size() - allDeletedSuccessNum);
      }
    } catch (Exception e) {
      isSuccess = false;
      printLogWhenException(LOGGER, e);
      recover();
    } finally {
      releaseAllLocks();
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        printLogWhenException(LOGGER, e);
      }
      // may fail to set status if the status of target resource is DELETED
      if (targetTsFileResource != null) {
        targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    return isSuccess;
  }

  public boolean settleWithAllDeletedFiles() {
    if (allDeletedFiles.isEmpty()) {
      return true;
    }
    boolean isSuccess = true;
    for (TsFileResource resource : allDeletedFiles) {
      if (recoverMemoryStatus) {
        tsFileManager.remove(resource, resource.isSeq());
        if (resource.getModFile().exists()) {
          FileMetrics.getInstance().decreaseModFileNum(1);
          FileMetrics.getInstance().decreaseModFileSize(resource.getModFile().getSize());
        }
      }
      boolean res = deleteTsFileOnDisk(resource);
      if (res) {
        allDeletedSuccessNum++;
        LOGGER.debug(
            "Settle task deletes all dirty tsfile {} successfully.",
            resource.getTsFile().getAbsolutePath());
        if (recoverMemoryStatus) {
          FileMetrics.getInstance()
              .deleteTsFile(resource.isSeq(), Collections.singletonList(resource));
        }
      } else {
        LOGGER.error(
            "Settle task fail to delete all dirty tsfile {}.",
            resource.getTsFile().getAbsolutePath());
      }
      isSuccess = isSuccess && res;
    }
    return isSuccess;
  }

  /** Use inner compaction task to compact the partial_deleted files. */
  private void settleWithPartialDeletedFiles(SimpleCompactionLogger logger) throws Exception {
    if (selectedTsFileResourceList.isEmpty()) {
      return;
    }
    LOGGER.info(
        "{}-{} [Compaction] Start to settle {} {} partial_deleted filess, "
            + "total file size is {} MB",
        storageGroupName,
        dataRegionId,
        selectedTsFileResourceList.size(),
        sequence ? "Sequence" : "Unsequence",
        selectedFileSize / 1024 / 1024);
    long startTime = System.currentTimeMillis();
    compact(logger);
    double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
    LOGGER.info(
        "{}-{} [Compaction] Finish to settle {} {} partial_deleted files successfully , "
            + "target file is {},"
            + "time cost is {} s, "
            + "compaction speed is {} MB/s, {}",
        storageGroupName,
        dataRegionId,
        selectedTsFileResourceList.size(),
        sequence ? "Sequence" : "Unsequence",
        targetTsFileResource.getTsFile().getName(),
        String.format("%.2f", costTime),
        String.format("%.2f", selectedFileSize / 1024.0d / 1024.0d / costTime),
        summary);
  }

  @Override
  public void recover() {
    LOGGER.info(
        "{}-{} [Compaction][Recover] Start to recover settle compaction.",
        storageGroupName,
        dataRegionId);
    try {
      if (needRecoverTaskInfoFromLogFile) {
        recoverTaskInfoFromLogFile();
      }
      recoverAllDeletedFiles();
      recoverPartialDeletedFiles();
      LOGGER.info(
          "{}-{} [Compaction][Recover] Finish to recover settle compaction successfully.",
          storageGroupName,
          dataRegionId);
      if (needRecoverTaskInfoFromLogFile) {
        Files.deleteIfExists(logFile.toPath());
      }
    } catch (Exception e) {
      handleRecoverException(e);
    }
  }

  public void recoverAllDeletedFiles() {
    if (!settleWithAllDeletedFiles()) {
      throw new CompactionRecoverException("Failed to delete all_deleted source file.");
    }
  }

  private void recoverPartialDeletedFiles() throws IOException {
    if (shouldRollback()) {
      rollback();
    } else {
      finishTask();
    }
  }

  public void recoverTaskInfoFromLogFile() throws IOException {
    LOGGER.info(
        "{}-{} [Compaction][Recover] compaction log is {}",
        storageGroupName,
        dataRegionId,
        logFile);
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();

    allDeletedFiles = new ArrayList<>();
    selectedTsFileResourceList = new ArrayList<>();
    // recover source files, including all_deleted files and partial_deleted files
    sourceFileIdentifiers.forEach(
        x -> {
          File sourceFile = x.getFileFromDataDirsIfAnyAdjuvantFileExists();
          TsFileResource resource;
          if (sourceFile == null) {
            // source file has been deleted, create empty resource
            resource = new TsFileResource(new File(x.getFilePath()));
          } else {
            resource = new TsFileResource(sourceFile);
          }
          if (deletedTargetFileIdentifiers.contains(x)) {
            allDeletedFiles.add(resource);
          } else {
            selectedTsFileResourceList.add(resource);
          }
        });

    // recover target file
    recoverTargetResource(targetFileIdentifiers, deletedTargetFileIdentifiers);
  }

  @Override
  public CompactionTaskType getCompactionTaskType() {
    return CompactionTaskType.SETTLE;
  }

  public List<TsFileResource> getAllDeletedFiles() {
    return allDeletedFiles;
  }

  public List<TsFileResource> getPartialDeletedFiles() {
    return selectedTsFileResourceList;
  }

  public double getAllDeletedFileSize() {
    return allDeletedFileSize;
  }

  public double getPartialDeletedFileSize() {
    return partialDeletedFileSize;
  }

  public long getTotalModsSize() {
    return totalModsFileSize;
  }

  @Override
  public String toString() {
    return storageGroupName
        + "-"
        + dataRegionId
        + "-"
        + timePartition
        + " all_deleted file num is "
        + allDeletedFiles.size()
        + ", partial_deleted file num is "
        + selectedTsFileResourceList.size()
        + ", all_deleted files is "
        + allDeletedFiles
        + ", partial_deleted files is "
        + selectedTsFileResourceList;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SettleCompactionTask)) {
      return false;
    }

    return equalsOtherTask((SettleCompactionTask) other);
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof SettleCompactionTask)) {
      return false;
    }
    SettleCompactionTask otherSettleCompactionTask = (SettleCompactionTask) otherTask;
    return this.allDeletedFiles.equals(otherSettleCompactionTask.allDeletedFiles)
        && this.selectedTsFileResourceList.equals(
            otherSettleCompactionTask.selectedTsFileResourceList)
        && this.performer.getClass().isInstance(otherSettleCompactionTask.performer);
  }
}
