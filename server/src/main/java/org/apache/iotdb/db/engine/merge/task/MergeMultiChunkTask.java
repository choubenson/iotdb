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

package org.apache.iotdb.db.engine.merge.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.selector.IMergePathSelector;
import org.apache.iotdb.db.engine.merge.selector.NaivePathSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

public class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeMultiChunkTask.class);
  private static int minChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();

  private MergeLogger mergeLogger;
  private List<PartialPath> unmergedSeries;

  private String taskName;
  private MergeResource resource;
  private TimeValuePair[] currTimeValuePairs;
  private boolean fullMerge;

  private MergeContext mergeContext;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<PartialPath> currMergingPaths = new ArrayList<>();
  // need to be cleared every device
  private final Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
      measurementChunkMetadataListMapIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManagement.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));
  // need to be cleared every device
  private final Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
      chunkMetadataListCacheForMerge =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManagement.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  private String storageGroupName;

  public MergeMultiChunkTask(
      MergeContext context,
      String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource,
      boolean fullMerge,
      List<PartialPath> unmergedSeries,
      int concurrentMergeSeriesNum,
      String storageGroupName) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  void mergeSeries() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<PartialPath> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        currMergingPaths = pathSelector.next();
        mergePaths();
        resource.clearChunkWriterCache();
        if (Thread.interrupted()) {
          logger.info("MergeMultiChunkTask {} aborted", taskName);
          Thread.currentThread().interrupt();
          return;
        }
        mergedSeriesCnt += currMergingPaths.size();
        logMergeProgress();
      }
      measurementChunkMetadataListMapIteratorCache.clear();
      chunkMetadataListCacheForMerge.clear();
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} all series are merged after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 10.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

  public String getProgress() {
    return String.format("Processed %d/%d series", mergedSeriesCnt, unmergedSeries.size());
  }

  private void mergePaths() throws IOException, MetadataException {
    // 1. 读取所有待合并序列在所有乱序文件里的所有Chunk，依次放入List<Chunk>[]
    // 2. 创建所有待合并序列的乱序数据点Reader并返回,将某待合并序列在所有乱序文件的所有Chunk的第一个数据点放入heap优先级队列里（越后面的Chunk说明数据越新，因此优先级越高）
    IPointReader[] unseqReaders = resource.getUnseqReaders(currMergingPaths);

    //初始化所有待合并序列的第一个优先级最高的乱序数据点
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders[i].hasNextTimeValuePair()) {
        currTimeValuePairs[i] = unseqReaders[i].currentTimeValuePair();
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private String getMaxSensor(List<PartialPath> sensors) {
    String maxSensor = sensors.get(0).getMeasurement();
    for (int i = 1; i < sensors.size(); i++) {
      if (maxSensor.compareTo(sensors.get(i).getMeasurement()) < 0) {
        maxSensor = sensors.get(i).getMeasurement();
      }
    }
    return maxSensor;
  }

  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders)
      throws IOException, MetadataException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    // all paths in one call are from the same device
    String deviceId = currMergingPaths.get(0).getDevice();
    long currDeviceMinTime = currTsFile.getStartTime(deviceId);

    for (PartialPath path : currMergingPaths) {
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    for (TimeValuePair timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<ChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];
    Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
        measurementChunkMetadataListMapIteratorCache.computeIfAbsent(
            fileSequenceReader,
            (tsFileSequenceReader -> {
              try {
                return tsFileSequenceReader.getMeasurementChunkMetadataListMapIterator(deviceId);
              } catch (IOException e) {
                logger.error(
                    "unseq compaction task {}, getMeasurementChunkMetadataListMapIterator meets error. iterator create failed.",
                    taskName,
                    e);
                return null;
              }
            }));
    if (measurementChunkMetadataListMapIterator == null) {
      return;
    }

    String lastSensor = getMaxSensor(currMergingPaths);
    String currSensor = null;
    Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap = new TreeMap<>();
    // find all sensor to merge in order, if exceed, then break
    while (currSensor == null || currSensor.compareTo(lastSensor) < 0) {
      measurementChunkMetadataListMap =
          chunkMetadataListCacheForMerge.computeIfAbsent(
              fileSequenceReader, tsFileSequenceReader -> new TreeMap<>());
      // if empty, get measurementChunkMetadataList block to use later
      if (measurementChunkMetadataListMap.isEmpty()) {
        // if do not have more sensor, just break
        if (measurementChunkMetadataListMapIterator.hasNext()) {
          measurementChunkMetadataListMap.putAll(measurementChunkMetadataListMapIterator.next());
        } else {
          break;
        }
      }

      Iterator<Entry<String, List<ChunkMetadata>>> measurementChunkMetadataListEntryIterator =
          measurementChunkMetadataListMap.entrySet().iterator();
      while (measurementChunkMetadataListEntryIterator.hasNext()) {
        Entry<String, List<ChunkMetadata>> measurementChunkMetadataListEntry =
            measurementChunkMetadataListEntryIterator.next();
        currSensor = measurementChunkMetadataListEntry.getKey();

        // fill modifications and seqChunkMetas to be used later
        for (int i = 0; i < currMergingPaths.size(); i++) {
          if (currMergingPaths.get(i).getMeasurement().equals(currSensor)) {
            modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
            seqChunkMeta[i] = measurementChunkMetadataListEntry.getValue();
            modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
            for (ChunkMetadata chunkMetadata : seqChunkMeta[i]) {
              resource.updateStartTime(currTsFile, deviceId, chunkMetadata.getStartTime());
              resource.updateEndTime(currTsFile, deviceId, chunkMetadata.getEndTime());
            }

            if (Thread.interrupted()) {
              Thread.currentThread().interrupt();
              return;
            }
            break;
          }
        }

        // current sensor larger than last needed sensor, just break out to outer loop
        if (currSensor.compareTo(lastSensor) > 0) {
          break;
        } else {
          measurementChunkMetadataListEntryIterator.remove();
        }
      }
    }
    // update measurementChunkMetadataListMap
    chunkMetadataListCacheForMerge.put(fileSequenceReader, measurementChunkMetadataListMap);

    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);

    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile, false);
    for (PartialPath path : currMergingPaths) {
      MeasurementSchema schema = IoTDB.metaManager.getSeriesSchema(path);
      mergeFileWriter.addSchema(path, schema);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten =
        mergeChunks(
            deviceId,
            seqChunkMeta,
            isLastFile,
            fileSequenceReader,
            unseqReaders,
            mergeFileWriter,
            currTsFile);
    if (dataWritten) {
      mergeFileWriter.endChunkGroup();
      currTsFile.updateStartTime(deviceId, currDeviceMinTime);
    }
  }

  private List<Integer> filterNoDataPaths(List[] seqChunkMeta, int seqFileIdx) {
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if ((seqChunkMeta[i] == null || seqChunkMeta[i].isEmpty())
          && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePairs[i] != null)) {
        continue;
      }
      ret.add(i);
    }
    return ret;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean mergeChunks(
      String deviceId,
      List<ChunkMetadata>[] seqChunkMeta, //所有待合并序列在该顺序文件里待ChunkMetadataList
      boolean isLastFile,
      TsFileSequenceReader reader,
      IPointReader[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter,
      TsFileResource currFile)
      throws IOException {
    int[] ptWrittens = new int[seqChunkMeta.length];
    int mergeChunkSubTaskNum =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    //存放所有待合并序列在该顺序文件里的ChunkMetadatalist,元素是（待合并序列index，该序列在该顺序文件里待ChunkMetadatalist）
    MetaListEntry[] metaListEntries = new MetaListEntry[currMergingPaths.size()];
    PriorityQueue<Integer>[] chunkIdxHeaps = new PriorityQueue[mergeChunkSubTaskNum];

    // if merge path is smaller than mergeChunkSubTaskNum, will use merge path number.
    // so thread are not wasted.
    if (currMergingPaths.size() < mergeChunkSubTaskNum) {
      mergeChunkSubTaskNum = currMergingPaths.size();
    }

    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkIdxHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (int i = 0; i < currMergingPaths.size(); i++) {
      chunkIdxHeaps[idx % mergeChunkSubTaskNum].add(i);
      if (seqChunkMeta[i] == null || seqChunkMeta[i].isEmpty()) {
        //可能在待合并序列不存在于该顺序文件里
        continue;
      }

      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      entry.next();
      metaListEntries[i] = entry;
      idx++;
      ptWrittens[i] = 0;
    }

    mergedChunkNum.set(0);
    unmergedChunkNum.set(0);

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      futures.add(
          MergeManager.getINSTANCE()
              .submitChunkSubTask(
                  new MergeChunkHeapTask(
                      chunkIdxHeaps[i],
                      metaListEntries,
                      ptWrittens,
                      reader,
                      mergeFileWriter,
                      unseqReaders,
                      currFile,
                      isLastFile,
                      currFile.getEndTime(deviceId),
                      i)));

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException e) {
        logger.error("MergeChunkHeapTask interrupted", e);
        Thread.currentThread().interrupt();
        return false;
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    // add merge and unmerged chunk statistic
    mergeContext
        .getMergedChunkCnt()
        .compute(
            currFile,
            (tsFileResource, anInt) ->
                anInt == null ? mergedChunkNum.get() : anInt + mergedChunkNum.get());
    mergeContext
        .getUnmergedChunkCnt()
        .compute(
            currFile,
            (tsFileResource, anInt) ->
                anInt == null ? unmergedChunkNum.get() : anInt + unmergedChunkNum.get());

    return mergedChunkNum.get() > 0;
  }

  /**
   * merge a sequence chunk SK
   *
   * <p>1. no need to write the chunk to .merge file when: isn't full merge & there isn't unclosed
   * chunk before & SK is big enough & SK isn't overflowed & SK isn't modified
   *
   * <p>
   *
   * <p>2. write SK to .merge.file without compressing when: is full merge & there isn't unclosed
   * chunk before & SK is big enough & SK isn't overflowed & SK isn't modified
   *
   * <p>3. other cases: need to unCompress the chunk and write 3.1 SK isn't overflowed 3.2 SK is
   * overflowed
   */
  // 1. 对该顺序Chunk里的所有page里的每个数据点与对应序列的乱序数据点去重写入ChunkWriter（此处该序列消耗掉的乱序数据点时间戳<=该顺序page的结束时间）
  // 2. 将该顺序Chunk的结束时间或者若是最后一个顺序Chunk,则将该顺序device的endTime前的该序列乱序数据点都写入该目标顺序Chunk里。
  // 3. 若目标Chunk足够大，则刷到目标文件，返回0。否则返回还未被刷盘的总共写入目标ChunkWriter的数据点数量
  @SuppressWarnings("java:S2445") // avoid writing the same writer concurrently
  private int mergeChunkV2(
      ChunkMetadata currMeta,
      boolean chunkOverflowed,
      boolean chunkTooSmall,
      boolean isLastChunk,
      long resourceEndTime,
      Chunk chunk,
      int lastUnclosedChunkPoint,
      int pathIdx,
      TsFileIOWriter mergeFileWriter,
      IPointReader unseqReader,
      IChunkWriter chunkWriter,
      TsFileResource currFile)
      throws IOException {
    int unclosedChunkPoint = lastUnclosedChunkPoint;
    boolean chunkModified =
        (currMeta.getDeleteIntervalList() != null && !currMeta.getDeleteIntervalList().isEmpty());

    // no need to write the chunk to .merge file
    if (!fullMerge
        && lastUnclosedChunkPoint == 0
        && !chunkTooSmall
        && !chunkOverflowed
        && !chunkModified) {
      unmergedChunkNum.incrementAndGet();
      mergeContext
          .getUnmergedChunkStartTimes()
          .get(currFile)
          .get(currMergingPaths.get(pathIdx))
          .add(currMeta.getStartTime());
      return 0;
    }

    // write SK to .merge.file without compressing
    if (fullMerge
        && lastUnclosedChunkPoint == 0
        && !chunkTooSmall
        && !chunkOverflowed
        && !chunkModified) {
      synchronized (mergeFileWriter) {
        mergeFileWriter.writeChunk(chunk, currMeta);
      }
      mergeContext.incTotalPointWritten(currMeta.getNumOfPoints());
      mergeContext.incTotalChunkWritten();
      mergedChunkNum.incrementAndGet();
      return 0;
    }

    // 3.1 SK isn't overflowed, just uncompress and write sequence chunk
    if (!chunkOverflowed) {
      //该顺序chunk与乱序没有overflow，则直接解该顺序chunk，把符合条件的数据点写到目标ChunkWriter
      unclosedChunkPoint += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergedChunkNum.incrementAndGet();
    } else {
      // 3.2 SK is overflowed, uncompress sequence chunk and merge with unseq chunk, then write
      // 1. 对该顺序Chunk里的所有page里的每个数据点与对应序列的乱序数据点去重写入ChunkWriter（此处该序列消耗掉的乱序数据点时间戳<=该顺序page的结束时间）
      // 2. 将该顺序Chunk的结束时间或者若是最后一个顺序Chunk,则将该顺序device的endTime前的该序列乱序数据点都写入该目标顺序Chunk里。返回总共写入目标Chunk的数据点数量
      unclosedChunkPoint +=
          writeChunkWithUnseq(
              chunk,
              chunkWriter,
              unseqReader,
              isLastChunk ? resourceEndTime + 1 : currMeta.getEndTime(),
              pathIdx);
      mergedChunkNum.incrementAndGet();
    }

    // update points written statistics
    mergeContext.incTotalPointWritten((long) unclosedChunkPoint - lastUnclosedChunkPoint);
    if (minChunkPointNum > 0 && unclosedChunkPoint >= minChunkPointNum
        || unclosedChunkPoint > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (mergeFileWriter) {
        chunkWriter.writeToFileWriter(mergeFileWriter);
      }
      unclosedChunkPoint = 0;
    }
    return unclosedChunkPoint;
  }

  //timeLimit为该序列在该顺序文件的该Chunk的结束时间；若是该顺序文件的最后一个Chunk，则结束时间是该顺序文件该device的endTime
  //将该顺序Chunk的结束时间或者若是最后一个顺序Chunk,则将该顺序device的endTime前的该序列乱序数据点都写入该目标顺序Chunk里
  private int writeRemainingUnseq(
      IChunkWriter chunkWriter, IPointReader unseqReader, long timeLimit, int pathIdx)
      throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null
        && currTimeValuePairs[pathIdx].getTimestamp() < timeLimit) {
      //Todo:这里也有bug，若该序列有多个时间戳相同的乱序数据点，则都会被写入到顺序文件里
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      unseqReader.nextTimeValuePair();
      currTimeValuePairs[pathIdx] =
          unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
    }
    return ptWritten;
  }

  // 1. 对该顺序Chunk里的所有page里的每个数据点与对应序列的乱序数据点去重写入ChunkWriter（此处该序列消耗掉的乱序数据点时间戳<=该顺序page的结束时间）
  // 2. 将该顺序Chunk的结束时间或者若是最后一个顺序Chunk,则将该顺序device的endTime前的该序列乱序数据点都写入该目标顺序Chunk里。返回总共写入目标Chunk的数据点数量
  private int writeChunkWithUnseq(
      Chunk chunk,
      IChunkWriter chunkWriter,
      IPointReader unseqReader,
      long chunkLimitTime, //该序列在该顺序文件的该Chunk的结束时间；若是该顺序文件的最后一个Chunk，则结束时间是该顺序文件该device的endTime
      int pathIdx)
      throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      // 对该顺序page里的每个数据点与对应序列的乱序数据点去重写入ChunkWriter（此处该序列消耗掉的乱序数据点时间戳<=该顺序page的结束时间），返回总共写入的数据点数量
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    //将该顺序Chunk的结束时间或者若是最后一个顺序Chunk,则将该顺序device的endTime前的该序列乱序数据点都写入该目标顺序Chunk里
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  // 对该顺序page里的每个数据点与对应序列的乱序数据点去重写入ChunkWriter（此处该序列消耗掉的乱序数据点时间戳<=该顺序page的结束时间），返回总共写入的数据点数量
  private int mergeWriteBatch(
      BatchData batchData, IChunkWriter chunkWriter, IPointReader unseqReader, int pathIdx)
      throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      boolean overwriteSeqPoint = false;
      // unseq point.time <= sequence point.time, write unseq point
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() <= time) {
        //Todo:这里有bug，若序列在所有乱序文件中出现时间戳相同的几个点，则这几个点都会被写入顺序文件里
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        if (currTimeValuePairs[pathIdx].getTimestamp() == time) {
          overwriteSeqPoint = true;
        }
        unseqReader.nextTimeValuePair();
        currTimeValuePairs[pathIdx] =
            unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
        cnt++;
      }
      // unseq point.time > sequence point.time, write seq point
      if (!overwriteSeqPoint) {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }

  public class MergeChunkHeapTask implements Callable<Void> {

    private PriorityQueue<Integer> chunkIdxHeap;

    //存放当前所有待合并序列在该顺序文件里的ChunkMetadatalist,元素是（待合并序列index，该序列在该顺序文件里待ChunkMetadatalist）
    private MetaListEntry[] metaListEntries;
    private int[] ptWrittens;
    private TsFileSequenceReader reader;
    private RestorableTsFileIOWriter mergeFileWriter;
    private IPointReader[] unseqReaders;
    private TsFileResource currFile;
    private boolean isLastFile;
    private int taskNum;

    //当前顺序文件的该设备的结束时间
    private long endTimeOfCurrentResource;

    private int totalSeriesNum;

    public MergeChunkHeapTask(
        PriorityQueue<Integer> chunkIdxHeap,
        MetaListEntry[] metaListEntries,
        int[] ptWrittens,
        TsFileSequenceReader reader,
        RestorableTsFileIOWriter mergeFileWriter,
        IPointReader[] unseqReaders,
        TsFileResource currFile,
        boolean isLastFile,
        long endTimeOfCurrentResource,
        int taskNum) {
      this.chunkIdxHeap = chunkIdxHeap;
      this.metaListEntries = metaListEntries;
      this.ptWrittens = ptWrittens;
      this.reader = reader;
      this.mergeFileWriter = mergeFileWriter;
      this.unseqReaders = unseqReaders;
      this.currFile = currFile;
      this.isLastFile = isLastFile;
      this.taskNum = taskNum;
      this.totalSeriesNum = chunkIdxHeap.size();
      this.endTimeOfCurrentResource = endTimeOfCurrentResource;
    }

    @Override
    public Void call() throws Exception {
      mergeChunkHeap();
      return null;
    }

    @SuppressWarnings("java:S2445") // avoid reading the same reader concurrently
    private void mergeChunkHeap() throws IOException, MetadataException {
      //按字典序从小到大依次对该子线程分配到的每个序列在该顺序文件里的每个chunk与乱序点进行去重写入到目标ChunkWriter，若足够大则将其刷盘，也有可能还未刷盘
      while (!chunkIdxHeap.isEmpty()) {
        int pathIdx = chunkIdxHeap.poll();
        PartialPath path = currMergingPaths.get(pathIdx);
        MeasurementSchema measurementSchema = IoTDB.metaManager.getSeriesSchema(path);
        // 若该顺序文件里该序列有多个chunk，则此处获取的ChunkWriter是在合并上个顺序chunk时候的目标ChunkWriter，里面还有未刷盘的数据点
        IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        if (metaListEntries[pathIdx] != null) {//若该顺序文件里存在该待合并序列
          MetaListEntry metaListEntry = metaListEntries[pathIdx];
          ChunkMetadata currMeta = metaListEntry.current();
          boolean isLastChunk = !metaListEntry.hasNext();
          // 判断当前待合并序列的是否有与该顺序文件的当前Chunk有否overlap，true则后续要解chunk，否则可以不解chunk
          // 当最小的乱序数据点时间戳<=顺序文件里该Chunk的结束时间，返回true
          // 若是最后一个chunk且乱序点时间小于该顺序文件该device的EndTime，返回true
          boolean chunkOverflowed =
              MergeUtils.isChunkOverflowed(
                  currTimeValuePairs[pathIdx], currMeta, isLastChunk, endTimeOfCurrentResource);
          // 判断是否太小，若否说明足够大，则后续可以直接将该顺序Chunk刷盘，无需解chunk；若为true，则后续要解该顺序Chunk：
          // 若在重写上个顺序Chunk的时候还有数据点仍留在目标ChunkWriter未被刷盘，则直接返回true，后续要解chunk；
          // 若当前顺序Chunk点很少且不是最后一个chunk,则返回true，后续要解chunk
          boolean chunkTooSmall =
              MergeUtils.isChunkTooSmall(
                  ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

          // 该待合并序列在该顺序文件里的当前Chunk
          Chunk chunk;
          synchronized (reader) {
            chunk = reader.readMemChunk(currMeta);
          }
          // 1. 对该顺序Chunk里的所有page里的每个数据点与对应序列的乱序数据点去重写入ChunkWriter（此处该序列消耗掉的乱序数据点时间戳<=该顺序page的结束时间）
          // 2. 将该顺序Chunk的结束时间或者若是最后一个顺序Chunk,则将该顺序device的endTime前的该序列乱序数据点都写入该目标顺序Chunk里。
          // 3. 若目标Chunk足够大，则刷到目标文件，返回0。否则返回还未被刷盘的总共写入目标ChunkWriter的数据点数量
          ptWrittens[pathIdx] =
              mergeChunkV2(
                  currMeta,
                  chunkOverflowed,
                  chunkTooSmall,
                  isLastChunk,
                  endTimeOfCurrentResource,
                  chunk,
                  ptWrittens[pathIdx],
                  pathIdx,
                  mergeFileWriter,
                  unseqReaders[pathIdx],
                  chunkWriter,
                  currFile);

          if (!isLastChunk) {
            metaListEntry.next();
            //若该序列在该顺序文件里还有chunk，则继续合并该序列
            chunkIdxHeap.add(pathIdx);
            continue;
          }
        }

        //若已重写完该序列在该顺序文件里的最后一个chunk了（此处目标ChunkWriter可能因为不够大还未被刷盘），则

        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        //Todo:由于0.12之前选文件的bug，例如有顺序文件 1 2 3，乱序文件4，可是由于1 3正在被合并，导致该跨空间合并只选择了2 4，这就会导致合并出来的目标顺序文件与1 3都有重叠
        //Todo:例如上述的例子，若序列S0在乱序范围是50～250，而2的该device范围是100～199，这里的逻辑就会导致（1）若当前是最后一个顺序文件，199后的数据点就会被写到对应的目标顺序文件里，导致该目标文件可能与后面的顺序文件有overlap (2)若当前并非最后一个顺序文件，则会导致199后的数据点被丢弃了
        if (isLastFile && currTimeValuePairs[pathIdx] != null) {
          ptWrittens[pathIdx] +=
              writeRemainingUnseq(chunkWriter, unseqReaders[pathIdx], Long.MAX_VALUE, pathIdx);
          mergedChunkNum.incrementAndGet();
        }
        // the last merged chunk may still be smaller than the threshold, flush it anyway
        if (ptWrittens[pathIdx] > 0) {
          synchronized (mergeFileWriter) {
            chunkWriter.writeToFileWriter(mergeFileWriter);
          }
        }
      }
    }

    public String getStorageGroupName() {
      return storageGroupName;
    }

    public String getTaskName() {
      return taskName + "_" + taskNum;
    }

    public String getProgress() {
      return String.format(
          "Processed %d/%d series", totalSeriesNum - chunkIdxHeap.size(), totalSeriesNum);
    }
  }
}
