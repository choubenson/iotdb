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

package org.apache.iotdb.db.engine.compaction.cross.inplace.task;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeContext;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator.increaseCrossCompactionCnt;

/**
 * MergeFileTask merges the merge temporary files with the seqFiles, either move the merged chunks
 * in the temp files into the seqFiles or move the unmerged chunks into the merge temp files,
 * depending on which one is the majority.
 */
public class MergeFileTask {

  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");

  private String taskName;
  private CrossSpaceMergeContext context;
  private InplaceCompactionLogger inplaceCompactionLogger;
  private CrossSpaceMergeResource resource;
  private List<TsFileResource> unmergedFiles;

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private int currentMergeIndex;
  private String currMergeFile;

  MergeFileTask(
      String taskName,
      CrossSpaceMergeContext context,
      InplaceCompactionLogger inplaceCompactionLogger,
      CrossSpaceMergeResource resource,
      List<TsFileResource> unmergedSeqFiles) {
    this.taskName = taskName;
    this.context = context;
    this.inplaceCompactionLogger = inplaceCompactionLogger;
    this.resource = resource;
    this.unmergedFiles = unmergedSeqFiles;
  }

  //遍历每个已合并完的顺序文件：
  //（1）若该旧文件已合并的Chunk数量大于等于未被合并的Chunk数量，则将旧的合并完后的顺序文件里尚未被合并的序列的所有Chunk数据写到临时目标文件里（给旧文件里每个未被合并的序列在临时目标文件里新开一个ChunkGroup然后顺序写入该未被合并序列的每个Chunk，然后关闭该ChunkGroup），删除旧的顺序文件，并把临时目标tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
  //（2）若该旧文件已合并的Chunk数量小于未被合并的Chunk数量，则过滤掉旧顺序文件里已被合并的Chunk，并把新的临时目标文件里的每个设备ChunkGroup下的所有Chunk写到旧文件里（对每个设备新开ChunkGroup），并把旧tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
  // 注意：此方法执行完后，所有的已合并完的旧顺序文件会被删除
  void mergeFiles() throws IOException {
    // decide whether to write the unmerged chunks to the merge files or to move the merged chunks
    // back to the origin seqFile's
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} files", taskName, unmergedFiles.size());
    }
    long startTime = System.currentTimeMillis();
    //遍历每个已合并完的顺序文件
    for (int i = 0; i < unmergedFiles.size(); i++) {
      TsFileResource seqFile = unmergedFiles.get(i);
      currentMergeIndex = i;
      currMergeFile = seqFile.getTsFilePath();

      //该待合并顺序文件里并不是所有序列都需要被合并，只有与乱序文件发生冲突存在相同的序列才需要被合并重写到临时目标文件里
      int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
      int unmergedChunkNum = context.getUnmergedChunkCnt().getOrDefault(seqFile, 0);
      if (mergedChunkNum >= unmergedChunkNum) {
        // move the unmerged data to the new file
        if (logger.isInfoEnabled()) {
          logger.info(
              "{} moving unmerged data of {} to the merged file, {} merged chunks, {} "
                  + "unmerged chunks",
              taskName,
              seqFile.getTsFile().getName(),
              mergedChunkNum,
              unmergedChunkNum);
        }
        //将旧的合并完后的顺序文件里尚未被合并的序列的所有Chunk数据写到临时目标文件里（给旧文件里每个未被合并的序列在临时目标文件里新开一个ChunkGroup然后顺序写入该未被合并序列的每个Chunk，然后关闭该ChunkGroup），删除旧的顺序文件，并把临时目标tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
        moveUnmergedToNew(seqFile);
      } else {
        // move the merged data to the old file
        if (logger.isInfoEnabled()) {
          logger.info(
              "{} moving merged data of {} to the old file {} merged chunks, {} "
                  + "unmerged chunks",
              taskName,
              seqFile.getTsFile().getName(),
              mergedChunkNum,
              unmergedChunkNum);
        }
        //过滤掉旧的合并完的顺序文件里已被合并的Chunk，并把新的临时目标文件里的每个设备ChunkGroup下的所有Chunk写到旧文件里（对每个设备新开ChunkGroup），并把旧tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
        moveMergedToOld(seqFile);
      }

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }

      logProgress();
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} has merged all files after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
    inplaceCompactionLogger.logMergeEnd();
  }

  private void logProgress() {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} has merged {}, processed {}/{} files",
          taskName,
          currMergeFile,
          currentMergeIndex + 1,
          unmergedFiles.size());
    }
  }

  public String getProgress() {
    return String.format(
        "Merging %s, processed %d/%d files",
        currMergeFile, currentMergeIndex + 1, unmergedFiles.size());
  }

  /**
   * 过滤掉旧的合并完的顺序文件里已被合并的Chunk，并把新的临时目标文件里的每个设备ChunkGroup下的所有Chunk写到旧文件里（对每个设备新开ChunkGroup），并把旧tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
   *
   * @param seqFile 旧的合并完的顺序文件
   * @throws IOException
   */
  private void moveMergedToOld(TsFileResource seqFile) throws IOException {
    int mergedChunkNum = context.getMergedChunkCnt().getOrDefault(seqFile, 0);
    if (mergedChunkNum == 0) {
      resource.removeFileAndWriter(seqFile);
      return;
    }

    seqFile.writeLock();
    try {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }

      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());

      resource.removeFileReader(seqFile);
      //旧的合并完的顺序文件的TsFileIOWriter
      TsFileIOWriter oldFileWriter = getOldFileWriter(seqFile);

      // filter the chunks that have been merged
      //过滤掉该旧的合并完的顺序文件里已经被合并的那些Chunk（即不在chunkStartTimes里的chunk）。循环遍历当前旧的已合并完的顺序TsFile里每个ChunkGroup，再遍历该ChunkGroup里的每个Chunk，若 当前Chunk对应的序列路径不是未被合并的（即已经被合并了） 或者 当前Chunk对应的序列路径是未被合并的但是当前Chunk的起始时间不在chunkStartTimes里（有可能该序列的某些chunk被合并了，但是某些chunk没有overlap），则移除其ChunkMetadata，若该ChunkGroup里的所有Chunk都过滤掉了，则移除该ChunkGroupMetadata
      oldFileWriter.filterChunks(new HashMap<>(context.getUnmergedChunkStartTimes().get(seqFile)));

      //临时目标文件的TSFileIOWriter
      RestorableTsFileIOWriter newFileWriter = resource.getMergeFileWriter(seqFile);
      newFileWriter.close();
      //临时目标文件的顺序读取器
      try (TsFileSequenceReader newFileReader =
          new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
        //整理并获取临时目标文件里每个设备对应所有传感器的所有ChunkMetadataList
        Map<String, List<ChunkMetadata>> chunkMetadataListInChunkGroups =
            newFileWriter.getDeviceChunkMetadataMap();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} find {} merged chunk groups", taskName, chunkMetadataListInChunkGroups.size());
        }
        //遍历临时目标文件里每个设备对应所有传感器的所有ChunkMetadataList
        for (Map.Entry<String, List<ChunkMetadata>> entry :
            chunkMetadataListInChunkGroups.entrySet()) {
          //设备ID
          String deviceId = entry.getKey();
          //该设备下的所有ChunkMetadata
          List<ChunkMetadata> chunkMetadataList = entry.getValue();
          //先在旧合并完的顺序TSFileIOWriter里开启一个该设备的ChunkGroup，将临时目标文件里该设备下所有序列的所有Chunk追加写到旧的合并完的顺序文件的TSFileIOWriter里，然后关闭该设备的ChunkGroup
          writeMergedChunkGroup(chunkMetadataList, deviceId, newFileReader, oldFileWriter);

          if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            oldFileWriter.close();
            restoreOldFile(seqFile);
            return;
          }
        }
      }
      //将合并后的临时目标文件里每个设备的起始结束时间更新到旧的合并完的顺序TsFileResource里，用于后续把该旧TsFileResource文件移动到最终目标文件的.resource文件。具体：首先从TsFileIOWriter获取该文件里每个设备的起始和结束时间，更新到跨空间合并资源管理器里，最后更新到该文件的TsFileResource对象里
      updateStartTimeAndEndTime(seqFile, oldFileWriter);
      //关闭此旧的合并完的顺序文件，写入索引区内容和其它内容
      oldFileWriter.endFile();
      updatePlanIndexes(seqFile);
      //序列化该旧文件的TsFileResource到本地文件
      seqFile.serialize();
      inplaceCompactionLogger.logFileMergeEnd();
      logger.debug("{} moved merged chunks of {} to the old file", taskName, seqFile);

      //删除临时目标的本地文件
      if (!newFileWriter.getFile().delete()) {
        logger.warn("Delete file {} failed", newFileWriter.getFile());
      }
      // change tsFile name
      File nextMergeVersionFile = increaseCrossCompactionCnt(seqFile.getTsFile());
      fsFactory.moveFile(seqFile.getTsFile(), nextMergeVersionFile);
      fsFactory.moveFile(
          fsFactory.getFile(seqFile.getTsFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
          fsFactory.getFile(
              nextMergeVersionFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      seqFile.setFile(nextMergeVersionFile);
    } catch (Exception e) {
      restoreOldFile(seqFile);
      throw e;
    } finally {
      seqFile.writeUnlock();
    }
  }

  //将合并后的临时目标文件里每个设备的起始结束时间更新到旧的合并完的顺序TsFileResource里，用于后续把该旧TsFileResource文件移动到最终目标文件的.resource文件。具体：首先从TsFileIOWriter获取该文件里每个设备的起始和结束时间，更新到跨空间合并资源管理器里，最后更新到该文件的TsFileResource对象里
  private void updateStartTimeAndEndTime(TsFileResource seqFile, TsFileIOWriter fileWriter) {
    // TODO change to get one timeseries block each time
    //获取该合并后的临时目标文件里每个设备对应所有传感器的所有ChunkMetadataList
    Map<String, List<ChunkMetadata>> deviceChunkMetadataListMap =
        fileWriter.getDeviceChunkMetadataMap();
    for (Entry<String, List<ChunkMetadata>> deviceChunkMetadataListEntry :
        deviceChunkMetadataListMap.entrySet()) {
      String device = deviceChunkMetadataListEntry.getKey();
      for (IChunkMetadata chunkMetadata : deviceChunkMetadataListEntry.getValue()) {
        resource.updateStartTime(seqFile, device, chunkMetadata.getStartTime());
        resource.updateEndTime(seqFile, device, chunkMetadata.getEndTime());
      }
    }
    // update all device start time and end time of the resource
    //更新该顺序TsFile的所有设备ID的开始和结束时间
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap = resource.getStartEndTime(seqFile);
    for (Entry<String, Pair<Long, Long>> deviceStartEndTimePairEntry :
        deviceStartEndTimePairMap.entrySet()) {
      String device = deviceStartEndTimePairEntry.getKey();
      Pair<Long, Long> startEndTimePair = deviceStartEndTimePairEntry.getValue();
      seqFile.putStartTime(device, startEndTimePair.left);
      seqFile.putEndTime(device, startEndTimePair.right);
    }
  }

  /**
   * Restore an old seq file which is being written new chunks when exceptions occur or the task is
   * aborted.
   */
  private void restoreOldFile(TsFileResource seqFile) throws IOException {
    RestorableTsFileIOWriter oldFileRecoverWriter =
        new RestorableTsFileIOWriter(seqFile.getTsFile());
    if (oldFileRecoverWriter.hasCrashed() && oldFileRecoverWriter.canWrite()) {
      oldFileRecoverWriter.endFile();
    } else {
      oldFileRecoverWriter.close();
    }
  }

  /** Open an appending writer for an old seq file so we can add new chunks to it. */
  private TsFileIOWriter getOldFileWriter(TsFileResource seqFile) throws IOException {
    TsFileIOWriter oldFileWriter;
    try {
      oldFileWriter = new ForceAppendTsFileWriter(seqFile.getTsFile());
      inplaceCompactionLogger.logFileMergeStart(
          seqFile.getTsFile(), ((ForceAppendTsFileWriter) oldFileWriter).getTruncatePosition());
      logger.debug("{} moving merged chunks of {} to the old file", taskName, seqFile);
      ((ForceAppendTsFileWriter) oldFileWriter).doTruncate();
    } catch (TsFileNotCompleteException e) {
      // this file may already be truncated if this merge is a system reboot merge
      oldFileWriter = new RestorableTsFileIOWriter(seqFile.getTsFile());
    }
    return oldFileWriter;
  }

  private void updatePlanIndexes(TsFileResource seqFile) {
    // as the new file contains data of other files, track their plan indexes in the new file
    // so that we will be able to compare data across different IoTDBs that share the same index
    // generation policy
    // however, since the data of unseq files are mixed together, we won't be able to know
    // which files are exactly contained in the new file, so we have to record all unseq files
    // in the new file
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      seqFile.updatePlanIndexes(unseqFile);
    }
  }

  /**
   * 先在旧合并完的顺序TSFileIOWriter里开启一个该设备的ChunkGroup，将临时目标文件里该设备下所有序列的所有Chunk追加写到旧的合并完的顺序文件的TSFileIOWriter里，然后关闭该设备的ChunkGroup
   *
   * @param chunkMetadataList 临时目标文件里该设备下的所有ChunkMetadata
   * @param device
   * @param reader  临时目标文件的顺序读取器
   * @param fileWriter  旧的合并完的顺序文件的TSFileIOWriter
   * @throws IOException
   */
  private void writeMergedChunkGroup(
      List<ChunkMetadata> chunkMetadataList,
      String device,
      TsFileSequenceReader reader,
      TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(device);
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      fileWriter.writeChunk(chunk, chunkMetaData);
      context.incTotalPointWritten(chunkMetaData.getNumOfPoints());
    }
    fileWriter.endChunkGroup();
  }



  /**Todo:改进
   * 将旧的合并完后的顺序文件里尚未被合并的序列的所有Chunk数据写到临时目标文件里（给旧文件里每个未被合并的序列在临时目标文件里新开一个ChunkGroup然后顺序写入该未被合并序列的每个Chunk，然后关闭该ChunkGroup），删除旧的顺序文件，并把临时目标tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。具体：
   * 1. 若该旧的已合并完的顺序文件里，未被合并的Chunk数量大于0，则给旧文件里每个未被合并的序列在临时目标文件里新开一个ChunkGroup然后顺序写入该未被合并序列的每个Chunk，然后关闭该ChunkGroup。具体：
   *    1）遍历该旧顺序文件里每个未合并序列路径及其对应的在该文件里每个Chunk的开始时间：
   *       (1) 开启一个新的设备ChunkGroup(临时目标文件里可能已经存在该设备ID的ChunkGroup)，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
   *       (2) 依照每个chunk的开始时间（chunkStartTimes列表里每个chunk时间是从小到大排序），依次把对应的chunk写入到临时目标文件的RestorableTsFileIOWriter里
   *       (3) 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
   * 2. 将合并后的临时目标文件里每个设备的起始结束时间更新到旧的合并完的顺序TsFileResource里，用于后续把该旧TsFileResource文件移动到最终目标文件的.resource文件。具体：首先从TsFileIOWriter获取该文件里每个设备的起始和结束时间，更新到跨空间合并资源管理器里，最后更新到该文件的TsFileResource对象里
   * 3. 从跨空间合并资源对象里移除此旧的合并完的顺序文件
   * 4. 结束此新的目标文件（.tsfile.merge），写入索引区内容和其它内容
   * 5. 将旧顺序文件加写锁，然后将其本地文件删掉，把临时目标文件（.tsfile.merge）移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)，并把旧顺序文件的.resource文件移动到最终目标文件的.resource文件，并将旧的顺序文件的TsfileResource设置新的最终目标文件，因此该seqFile变成最终目标文件的TsFileResource。最终释放该新最终目标文件的TsFileResource的写锁。
   *
   *
   * @param seqFile 旧的已合并完的顺序文件
   * @throws IOException
   */
  private void moveUnmergedToNew(TsFileResource seqFile) throws IOException {
    //该已合并完后的顺序文件里，所有未被合并的序列的每个Chunk的开始时间
    Map<PartialPath, List<Long>> fileUnmergedChunkStartTimes =
        context.getUnmergedChunkStartTimes().get(seqFile);
    //该顺序文件的临时目标文件的FileWriter
    RestorableTsFileIOWriter fileWriter = resource.getMergeFileWriter(seqFile);

    inplaceCompactionLogger.logFileMergeStart(fileWriter.getFile(), fileWriter.getFile().length());
    logger.debug("{} moving unmerged chunks of {} to the new file", taskName, seqFile);

    int unmergedChunkNum = context.getUnmergedChunkCnt().getOrDefault(seqFile, 0);

    if (unmergedChunkNum > 0) {
      for (Entry<PartialPath, List<Long>> entry : fileUnmergedChunkStartTimes.entrySet()) {
        //当前未被合并序列
        PartialPath path = entry.getKey();
        //该未被合并序列在当前顺序文件里每个Chunk的开始时间
        List<Long> chunkStartTimes = entry.getValue();
        if (chunkStartTimes.isEmpty()) {
          continue;
        }

        //该顺序文件里该序列的chunkMetadataList
        List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(path, seqFile);

        if (logger.isDebugEnabled()) {
          logger.debug("{} find {} unmerged chunks", taskName, chunkMetadataList.size());
        }

        // 开启一个新的设备ChunkGroup(临时目标文件里可能已经存在该设备ID的ChunkGroup)，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
        fileWriter.startChunkGroup(path.getDevice());
        //依照每个chunk的起始时间（chunkStartTimes列表里每个chunk时间是从小到大排序），依次把对应的chunk写入到临时目标文件的RestorableTsFileIOWriter里
        writeUnmergedChunks(
            chunkStartTimes, chunkMetadataList, resource.getFileReader(seqFile), fileWriter);

        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        // 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
        fileWriter.endChunkGroup();
      }
    }
    //将合并后的临时目标文件里每个设备的起始结束时间更新到旧的合并完的顺序TsFileResource里，用于后续把该旧TsFileResource文件移动到最终目标文件的.resource文件。具体：首先从TsFileIOWriter获取该文件里每个设备的起始和结束时间，更新到跨空间合并资源管理器里，最后更新到该文件的TsFileResource对象里
    updateStartTimeAndEndTime(seqFile, fileWriter);
    //移除此旧的合并完的顺序文件
    resource.removeFileReader(seqFile);
    //结束此新的目标文件（.tsfile.merge），写入索引区内容和其它内容
    fileWriter.endFile();

    updatePlanIndexes(seqFile);

    seqFile.writeLock();
    try {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }

      // 将该旧TsFile文件对应的TsFileResource对象里的内容序列化写到本地的.resource文件里
      seqFile.serialize();
      inplaceCompactionLogger.logFileMergeEnd();
      logger.debug("{} moved unmerged chunks of {} to the new file", taskName, seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());

      // change tsFile name
      //删除旧文件
      if (!seqFile.getTsFile().delete()) {
        logger.warn("Delete file {} failed", seqFile.getTsFile());
      }
      //根据旧的顺序文件名返回新的最终目标文件，该最终目标文件名里跨空间合并次数比旧文件增加了1
      File nextMergeVersionFile = increaseCrossCompactionCnt(seqFile.getTsFile());
      //将该顺序文件的临时目标文件（.tsfile.merge）移动到最终目标文件。此处会删除旧的临时目标文件
      fsFactory.moveFile(fileWriter.getFile(), nextMergeVersionFile);
      //将该顺序文件的临时目标文件对应的resource文件（.tsfile.merge.resource）移动到最终目标文件的resource
      fsFactory.moveFile(
          fsFactory.getFile(seqFile.getTsFile().getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
          fsFactory.getFile(
              nextMergeVersionFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
      //将旧的顺序文件的TsfileResource设置新的最终目标文件，因此该seqFile变成最终目标文件的TsFileResource
      seqFile.setFile(nextMergeVersionFile);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  //依照每个chunk的起始时间（chunkStartTimes列表里每个chunk时间是从小到大排序），依次把对应的chunk写入到临时目标文件的RestorableTsFileIOWriter里
  private long writeUnmergedChunks(
      List<Long> chunkStartTimes,
      List<ChunkMetadata> chunkMetadataList,
      TsFileSequenceReader reader,
      RestorableTsFileIOWriter fileWriter)
      throws IOException {
    long maxVersion = 0;
    int chunkIdx = 0;
    for (Long startTime : chunkStartTimes) {
      for (; chunkIdx < chunkMetadataList.size(); chunkIdx++) {
        ChunkMetadata metaData = chunkMetadataList.get(chunkIdx);
        if (metaData.getStartTime() == startTime) {
          Chunk chunk = reader.readMemChunk(metaData);
          fileWriter.writeChunk(chunk, metaData);
          maxVersion = metaData.getVersion() > maxVersion ? metaData.getVersion() : maxVersion;
          context.incTotalPointWritten(metaData.getNumOfPoints());
          break;
        }
      }

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return maxVersion;
      }
    }
    return maxVersion;
  }
}
