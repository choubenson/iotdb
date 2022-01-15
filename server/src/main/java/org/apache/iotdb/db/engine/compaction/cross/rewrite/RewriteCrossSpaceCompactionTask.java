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
package org.apache.iotdb.db.engine.compaction.cross.rewrite;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.MAGIC_STRING;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger.STR_UNSEQ_FILES;

public class RewriteCrossSpaceCompactionTask extends AbstractCrossSpaceCompactionTask {

  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");
  protected String storageGroupDir;
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected String logicalStorageGroupName;
  protected String virtualStorageGroupName;
  protected TsFileManager tsFileManager;
  private TsFileResourceList seqTsFileResourceList;
  private TsFileResourceList unseqTsFileResourceList;
  private File logFile;

  private List<TsFileResource> targetTsfileResourceList;
  private List<TsFileResource> holdReadLockList = new ArrayList<>();
  private List<TsFileResource> holdWriteLockList = new ArrayList<>();
  private boolean getWriteLockOfManager = false;
  private final long ACQUIRE_WRITE_LOCK_TIMEOUT =
      IoTDBDescriptor.getInstance().getConfig().getCompactionAcquireWriteLockTimeout();

  String storageGroupName;

  public RewriteCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      String storageGroupDir,
      TsFileManager tsFileManager,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unseqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.storageGroupDir = storageGroupDir;
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
    this.tsFileManager = tsFileManager;
    this.seqTsFileResourceList = seqTsFileResourceList;
    this.unseqTsFileResourceList = unseqTsFileResourceList;
  }

  @Override
  protected void doCompaction() throws Exception {
    try {
      executeCompaction();
    } catch (Throwable throwable) {
      // catch throwable instead of exception to handle OOM errors
      CrossSpaceCompactionExceptionHandler.handleException(
          storageGroupName,
          logFile,
          targetTsfileResourceList,
          seqTsFileResourceList,
          unseqTsFileResourceList,
          tsFileManager);
      throw throwable;
    } finally {
      releaseAllLock();
      resetCompactionStatus();
      if (getWriteLockOfManager) {
        tsFileManager.writeUnlock();
      }
    }
  }

  private void executeCompaction()
      throws IOException, StorageEngineException, MetadataException, InterruptedException,
          WriteProcessException {
    long startTime = System.currentTimeMillis();
    targetTsfileResourceList =
        TsFileNameGenerator.getCrossCompactionTargetFileResources(selectedSeqTsFileResourceList);

    if (targetTsfileResourceList.isEmpty()
        && selectedSeqTsFileResourceList.isEmpty()
        && selectedUnSeqTsFileResourceList.isEmpty()) {
      return;
    }

    logger.info(
        "{}-crossSpaceCompactionTask start. Sequence files : {}, unsequence files : {}",
        storageGroupName,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList);
    logFile = new File(storageGroupDir, RewriteCrossSpaceCompactionLogger.MERGE_LOG_NAME);
    try (RewriteCrossSpaceCompactionLogger compactionLogger =
        new RewriteCrossSpaceCompactionLogger(logFile)) {
      // print the path of the temporary file first for priority check during recovery
      compactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);
      compactionLogger.logFiles(selectedSeqTsFileResourceList, STR_SEQ_FILES);
      compactionLogger.logFiles(selectedUnSeqTsFileResourceList, STR_UNSEQ_FILES);
      CompactionUtils.compact(
          selectedSeqTsFileResourceList,
          selectedUnSeqTsFileResourceList,
          targetTsfileResourceList,
          storageGroupName);
      // indicates that the merge is complete and needs to be cleared
      // the result can be reused during a restart recovery
      compactionLogger.logStringInfo(MAGIC_STRING);

      CompactionUtils.moveToTargetFile(targetTsfileResourceList, false, storageGroupName);

      releaseReadAndLockWrite(selectedSeqTsFileResourceList);
      releaseReadAndLockWrite(selectedUnSeqTsFileResourceList);

      combineModsFiles();
      try {
        tsFileManager.writeLockWithTimeout(
            "rewrite-cross-space compaction", ACQUIRE_WRITE_LOCK_TIMEOUT);
        getWriteLockOfManager = true;
      } catch (WriteLockFailedException e) {
        // if current compaction thread couldn't get write lock
        // a WriteLockFailException will be thrown, then terminate the thread itself
        logger.error(
            "{} [CrossSpaceCompactionTask] failed to get write lock, abort the task.",
            fullStorageGroupName,
            e);
        throw new InterruptedException(
            String.format(
                "%s [Compaction] compaction abort because cannot acquire write lock",
                fullStorageGroupName));
      }

      deleteOldFiles(selectedSeqTsFileResourceList);
      deleteOldFiles(selectedUnSeqTsFileResourceList);
      removeCompactionModification();

      updateTsFileResource();
      logger.info(
          "{}-crossSpaceCompactionTask Costs {} s",
          storageGroupName,
          (System.currentTimeMillis() - startTime) / 1000);
    }
  }

  private void updateTsFileResource() throws IOException {
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      TsFileResourceManager.getInstance().removeTsFileResource(resource);
    }
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      TsFileResourceManager.getInstance().removeTsFileResource(resource);
    }
    for (TsFileResource targetTsFileResource : targetTsfileResourceList) {
      seqTsFileResourceList.keepOrderInsert(targetTsFileResource);
      TsFileResourceManager.getInstance().registerSealedTsFileResource(targetTsFileResource);
    }
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      seqTsFileResourceList.remove(resource);
    }
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      unseqTsFileResourceList.remove(resource);
    }
  }

  private void combineModsFiles() throws IOException {
    Map<String, TsFileResource> seqFileInfoMap = new HashMap<>();
    for (TsFileResource tsFileResource : selectedSeqTsFileResourceList) {
      seqFileInfoMap.put(
          TsFileNameGenerator.increaseCrossCompactionCnt(tsFileResource.getTsFile()).getName(),
          tsFileResource);
    }
    for (TsFileResource tsFileResource : targetTsfileResourceList) {
      updateCompactionModification(
          tsFileResource,
          seqFileInfoMap.get(tsFileResource.getTsFile().getName()),
          selectedUnSeqTsFileResourceList);
    }
  }

  private boolean addReadLock(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (tsFileResource.isMerging() | !tsFileResource.isClosed()
          || !tsFileResource.getTsFile().exists()
          || tsFileResource.isDeleted()) {
        releaseAllLock();
        return false;
      }
      tsFileResource.readLock();
      holdReadLockList.add(tsFileResource);
    }
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.setMerging(true);
    }
    return true;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return addReadLock(selectedSeqTsFileResourceList)
        && addReadLock(selectedUnSeqTsFileResourceList);
  }

  private void releaseReadAndLockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readUnlock();
      holdReadLockList.remove(tsFileResource);
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  private void releaseAllLock() {
    for (TsFileResource tsFileResource : holdReadLockList) {
      tsFileResource.readUnlock();
    }
    holdReadLockList.clear();
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
    }
    holdWriteLockList.clear();
  }

  private void resetCompactionStatus() {
    for (TsFileResource tsFileResource : selectedSeqTsFileResourceList) {
      tsFileResource.setMerging(false);
    }
    for (TsFileResource tsFileResource : selectedUnSeqTsFileResourceList) {
      tsFileResource.setMerging(false);
    }
  }

  void deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
      tsFileResource.setDeleted(true);
      tsFileResource.remove();
      logger.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  private void removeCompactionModification() {
    try {
      for (TsFileResource seqFile : selectedSeqTsFileResourceList) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
      for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
        ModificationFile.getCompactionMods(unseqFile).remove();
      }
    } catch (IOException e) {
      logger.error("{} cannot remove merging modification ", fullStorageGroupName, e);
    }
  }

  private void updateCompactionModification(
      TsFileResource targetFile, TsFileResource seqFile, List<TsFileResource> unseqFiles) {
    try {
      // write mods in the seq file
      if (seqFile != null) {
        ModificationFile seqCompactionModificationFile =
            ModificationFile.getCompactionMods(seqFile);
        for (Modification modification : seqCompactionModificationFile.getModifications()) {
          targetFile.getModFile().write(modification);
        }
      }
      // write mods in all un-seq files
      for (TsFileResource unseqFile : unseqFiles) {
        ModificationFile compactionUnseqModificationFile =
            ModificationFile.getCompactionMods(unseqFile);
        for (Modification modification : compactionUnseqModificationFile.getModifications()) {
          targetFile.getModFile().write(modification);
        }
      }
      try {
        targetFile.getModFile().close();
      } catch (IOException e) {
        logger.error(
            "Cannot close the ModificationFile {}", targetFile.getModFile().getFilePath(), e);
      }
    } catch (IOException e) {
      logger.error(
          "{} cannot clean the ModificationFile of {} after cross space merge",
          fullStorageGroupName,
          targetFile.getTsFile(),
          e);
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof RewriteCrossSpaceCompactionTask) {
      RewriteCrossSpaceCompactionTask otherTask = (RewriteCrossSpaceCompactionTask) other;
      if (!otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          || !otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }
}
