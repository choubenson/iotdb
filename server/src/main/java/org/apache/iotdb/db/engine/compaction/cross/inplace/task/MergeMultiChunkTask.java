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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeContext;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.IMergePathSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.NaivePathSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
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

//合并多个Chunk的任务类，注意：它仅仅是一个类，可以理解为一个工具类，不是一个线程
public class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");
  private static int minChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();

  private InplaceCompactionLogger inplaceCompactionLogger;
  //该物理存储组上所有的序列，他们被视为未被合并的
  private List<PartialPath> unmergedSeries;

  private String taskName;
  //跨空间合并资源管理器
  private CrossSpaceMergeResource resource;
  //每个序列在乱序文件的当前数据点数组，数量为当前待被合并的序列的数量
  private TimeValuePair[] currTimeValuePairs;
  private boolean fullMerge;

  //跨空间合并上下文
  private CrossSpaceMergeContext mergeContext;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  //跨空间 允许并行合并序列的线程数量
  private int concurrentMergeSeriesNum;
  //当前正在被合并的时间序列
  private List<PartialPath> currMergingPaths = new ArrayList<>();
  // need to be cleared every device
  //存放每个顺序TsFile的顺序阅读器和 某一设备ID下每个传感器ID和对应的ChunkMetadata列表的遍历器，使用该遍历器可以获取该文件的该设备在泽嵩树的下一个TimeseriesMetadata节点上的所有一条条TimeseriesMetadata对应传感器measurementId和各自对应的ChunkMetadata列表
  private final Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
      measurementChunkMetadataListMapIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManager.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));
  // need to be cleared every device
  //待被合并的数据，存放待被合并顺序文件阅读器 和 当前待合并顺序文件里当前设备在泽嵩树上某一个TimeseriesMetadata节点上的所有 measurementId和对应的ChunkMetadataList
  private final Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
      chunkMetadataListCacheForMerge =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManager.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  private String storageGroupName;

  public MergeMultiChunkTask(
      CrossSpaceMergeContext context,
      String taskName,
      InplaceCompactionLogger inplaceCompactionLogger,
      CrossSpaceMergeResource mergeResource,
      boolean fullMerge,
      List<PartialPath> unmergedSeries,
      int concurrentMergeSeriesNum,
      String storageGroupName) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.inplaceCompactionLogger = inplaceCompactionLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  void mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    //遍历每个待合并顺序文件
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      // record the unmergeChunkStartTime for each sensor in each file
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    //根据设备种类把传来的序列路径进行分开存放
    List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    //依次遍历该存储组下 每个设备下的所有序列路径
    for (List<PartialPath> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      //根据 当前存储组下的该设备下的所有序列路径 和 允许获取的最大序列数量 去创建时间序列选择器
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        //获取下批时间序列路径为当前正在被合并的序列，数量为maxSeriesNum个
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
    inplaceCompactionLogger.logAllTsEnd();
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

  private void mergePaths() throws IOException {
    //往日志里写入当前正准备被合并的时间序列路径
    inplaceCompactionLogger.logTSStart(currMergingPaths);
    //首先获取每个序列在所有待合并乱序文件里的所有Chunk，并以此创建每个序列的乱序的序列合并阅读器，依次放入数组里并返回。
    IPointReader[] unseqReaders = resource.getUnseqReaders(currMergingPaths);
    //初始化乱序文件当前的数据点数组，数量为当前待被合并的序列的数量
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    //遍历每个待合并序列，初始化每个序列的当前数据点数组
    for (int i = 0; i < currMergingPaths.size(); i++) {
      //若当前待合并序列还有下个数据点
      if (unseqReaders[i].hasNextTimeValuePair()) {
        currTimeValuePairs[i] = unseqReaders[i].currentTimeValuePair();
      }
    }

    //遍历每个待合并顺序文件
    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    inplaceCompactionLogger.logTSEnd();
  }

  //获取指定序列里最大的传感器measurementId
  private String getMaxSensor(List<PartialPath> sensors) {
    String maxSensor = sensors.get(0).getMeasurement();
    for (int i = 1; i < sensors.size(); i++) {
      if (maxSensor.compareTo(sensors.get(i).getMeasurement()) < 0) {
        maxSensor = sensors.get(i).getMeasurement();
      }
    }
    return maxSensor;
  }

  /**
   *
   * @param seqFileIdx 在乱序合并资源管理器里第几个待合并顺序文件
   * @param unseqReaders  当前所有待合并序列的乱序序列阅读器
   * @throws IOException
   */
  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders) throws IOException {
    //指定的待合并顺序文件，它也是合并写入的目标文件
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    // all paths in one call are from the same device
    String deviceId = currMergingPaths.get(0).getDevice();
    //当前顺序文件里的当前设备的最小时间
    long currDeviceMinTime = currTsFile.getStartTime(deviceId);

    //遍历每个当前待合并序列，
    for (PartialPath path : currMergingPaths) {
      //初始化当前TsFile上，未被合并的序列的每个Chunk的开始时间，此时每个Chunk的开始时间还是一个空列表
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    //获取当前设备的最小时间
    for (TimeValuePair timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }
    //当前待合并顺序文件是否是最后一个顺序文件
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    //当前待合并顺序文件的顺序阅读器
    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    //数组，存放每个待合并序列的删除操作List
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    //数组，存放每个待合并序列的在当前待合并顺序文件的ChunkMetadataList
    List<ChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];
    //初始化当前待合并顺序文件阅读器和对应的每个传感器ID和对应的ChunkMetadata列表的遍历器
    Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
        measurementChunkMetadataListMapIteratorCache.computeIfAbsent(
            fileSequenceReader,
            (tsFileSequenceReader -> {
              try {
                //返回指定设备ID下每个传感器ID和对应的ChunkMetadata列表的遍历器，使用该遍历器可以获取该文件的该设备在泽嵩树的下一个TimeseriesMetadata节点上的所有一条条TimeseriesMetadata对应传感器measurementId和各自对应的ChunkMetadata列表
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

    // need the max sensor in lexicographic order
    //获取指定序列里最大的传感器measurementId
    String lastSensor = getMaxSensor(currMergingPaths);
    String currSensor = null;
    //存放当前待合并顺序文件里当前设备在泽嵩树上某一个TimeseriesMetadata节点上的所有 measurementId和对应的ChunkMetadataList
    Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap = new TreeMap<>();
    // find all sensor to merge in order, if exceed, then break
    while (currSensor == null || currSensor.compareTo(lastSensor) < 0) {
      //初始化measurementChunkMetadataListMap，使用遍历器往里放入当前待合并顺序文件的该设备在泽嵩树的下一个TimeseriesMetadata节点上的所有一条条TimeseriesMetadata对应传感器measurementId和各自对应的ChunkMetadata列表
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

      //创建measurementChunkMetadataListMap的遍历器，用于获取该变量的一个个元素
      Iterator<Entry<String, List<ChunkMetadata>>> measurementChunkMetadataListEntryIterator =
          measurementChunkMetadataListMap.entrySet().iterator();
      //循环遍历measurementChunkMetadataListMap的一个个元素，即当前待合并顺序文件里当前设备在泽嵩树上某一个TimeseriesMetadata节点上的 “一个个measurementId”和“对应的ChunkMetadataList”
      while (measurementChunkMetadataListEntryIterator.hasNext()) {
        Entry<String, List<ChunkMetadata>> measurementChunkMetadataListEntry =
            measurementChunkMetadataListEntryIterator.next();
        //当前measurementId
        currSensor = measurementChunkMetadataListEntry.getKey();

        //初始化当前传感器currSensor在该待合并顺序文件里的删除操作和符合条件的ChunkMetadataList,放到modifications和seqChunkMeta数组合适的位置里
        // fill modifications and seqChunkMetas to be used later
        //循环遍历每个待合并序列
        for (int i = 0; i < currMergingPaths.size(); i++) {
          //若序列为当前measurementId
          if (currMergingPaths.get(i).getMeasurement().equals(currSensor)) {
            modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
            seqChunkMeta[i] = measurementChunkMetadataListEntry.getValue();
            //根据给定的对该序列的删除操作列表和该序列的ChunkMetadata列表，若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)
            modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
            //更新跨空间资源管理器里该待合并顺序TsFile的该设备ID的开始和结束时间
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

    //遍历每个当前待合并序列，判断它们在当前待合并顺序文件里是否存在数据，若不存在则过滤掉，返回未被过滤的序列的索引列表。（注意：若当前是最后一个顺序待合并文件且某不存在某序列，可是待合并乱序文件里存在，则不会过滤掉此序列）
    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);

    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    //创建目标文件的写入TsFileIOWriter
    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    //遍历每个待合并序列
    for (PartialPath path : currMergingPaths) {
      IMeasurementSchema schema = resource.getSchema(path);
      mergeFileWriter.addSchema(path, schema);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    // 开启一个新的设备ChunkGroup，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten =
        mergeChunks(
            seqChunkMeta, //数组，存放每个待合并序列的在当前待合并(目标)顺序文件的ChunkMetadataList，若某个序列已被全部删除，则其对应的元素是个空List，代表没有任何的ChunkMetadata
            isLastFile, //当前待合并顺序文件(也是目标文件)是否是最后一个顺序文件
            fileSequenceReader,  //当前待合并（目标）顺序文件的顺序阅读器
            unseqReaders,    //当前所有待合并序列的乱序序列阅读器
            mergeFileWriter,  //目标文件的writer
            currTsFile);    //目标文件的TsFileResource
    if (dataWritten) {
      mergeFileWriter.endChunkGroup();
      inplaceCompactionLogger.logFilePosition(mergeFileWriter.getFile());
      currTsFile.updateStartTime(deviceId, currDeviceMinTime);
    }
  }

  /**
   *
   * @param seqChunkMeta 数组，存放每个待合并序列的在当前待合并顺序文件的ChunkMetadataList
   * @param seqFileIdx 跨空间合并资源管理器里待合并顺序文件列表中的索引
   * @return 返回的是未被过滤的、需要进行合并的
   */
  //遍历每个当前待合并序列，判断它们在当前待合并顺序文件里是否存在数据，若不存在则过滤掉，返回未被过滤的序列的索引列表。（注意：若当前是最后一个顺序待合并文件且某不存在某序列，可是待合并乱序文件里存在，则不会过滤掉此序列）
  private List<Integer> filterNoDataPaths(List[] seqChunkMeta, int seqFileIdx) {
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      //若当前待合并序列在当前待合并顺序文件里不存在则
      if ((seqChunkMeta[i] == null || seqChunkMeta[i].isEmpty())
          && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePairs[i] != null)) {
        continue;
      }
      ret.add(i);
    }
    return ret;
  }

  /**
   *
   * @param seqChunkMeta //数组，存放每个待合并序列的在当前待合并(目标)顺序文件的ChunkMetadataList，若某个序列已被全部删除，则其对应的元素是个空List，代表没有任何的ChunkMetadata
   * @param isLastFile  //当前待合并顺序文件(也是目标文件)是否是最后一个顺序文件
   * @param reader    //当前待合并（目标）顺序文件的顺序阅读器
   * @param unseqReaders     //当前所有待合并序列的乱序序列阅读器
   * @param mergeFileWriter    //目标文件的writer
   * @param currFile    //目标文件的TsFileResource
   * @return
   * @throws IOException
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean mergeChunks(
      List<ChunkMetadata>[] seqChunkMeta,
      boolean isLastFile,
      TsFileSequenceReader reader,
      IPointReader[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter,
      TsFileResource currFile)
      throws IOException {
    //若第i个序列在该目标文件里存在Chunk，则该数组第i个位置标记为0
    int[] ptWrittens = new int[seqChunkMeta.length];
    //执行跨空间Chunk合并的子任务数量
    int mergeChunkSubTaskNum =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    //该数组存放每个待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该目标文件里的ChunkMetadataList）,若该目标文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null
    MetaListEntry[] metaListEntries = new MetaListEntry[currMergingPaths.size()];
    //每个子合并任务的优先级队列，该队列存放着待合并序列的索引
    PriorityQueue<Integer>[] chunkIdxHeaps = new PriorityQueue[mergeChunkSubTaskNum];

    // if merge path is smaller than mergeChunkSubTaskNum, will use merge path number.
    // so thread are not wasted.
    if (currMergingPaths.size() < mergeChunkSubTaskNum) {
      mergeChunkSubTaskNum = currMergingPaths.size();
    }

    //初始化chunkIdxHeaps里每个元素的优先级队列
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkIdxHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    //（1）初始化chunkIdxHeap每个子合并任务的队列：给每个子合并任务的队列里依次放入不同序列的索引，如有5个子合并任务和10个序列，则第1到第5个合并任务的队列里分别是{(1,6),(2,7),(3,8),(4,9),(5,10)}
    //（2）初始化metaListEntries：若该目标文件存在该序列，则创建该序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该目标文件里的ChunkMetadataList），并放入metaListEntries具体索引位置
    //（3）初始化ptWrittens：若该目标文件存在该序列，则把具体位置标记0
    for (int i = 0; i < currMergingPaths.size(); i++) {
      chunkIdxHeaps[idx % mergeChunkSubTaskNum].add(i);
      if (seqChunkMeta[i] == null || seqChunkMeta[i].isEmpty()) {
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
                      chunkIdxHeaps[i], //每个子合并任务的优先级队列，该队列存放着待合并序列的索引
                      metaListEntries,
                      ptWrittens,
                      reader,
                      mergeFileWriter,
                      unseqReaders,
                      currFile,
                      isLastFile,
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
  @SuppressWarnings("java:S2445") // avoid writing the same writer concurrently
  private int mergeChunkV2(
      ChunkMetadata currMeta,
      boolean chunkOverflowed,
      boolean chunkTooSmall,
      Chunk chunk,
      int lastUnclosedChunkPoint,
      int pathIdx,
      TsFileIOWriter mergeFileWriter,
      IPointReader unseqReader,
      ChunkWriterImpl chunkWriter,
      TsFileResource currFile)
      throws IOException {
    int unclosedChunkPoint = lastUnclosedChunkPoint;
    //判断当前Chunk是否有数据被删除修改过
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
      unclosedChunkPoint += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergedChunkNum.incrementAndGet();
    } else {
      // 3.2 SK is overflowed, uncompress sequence chunk and merge with unseq chunk, then write
      unclosedChunkPoint +=
          writeChunkWithUnseq(chunk, chunkWriter, unseqReader, currMeta.getEndTime(), pathIdx);
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

  private int writeRemainingUnseq(
      ChunkWriterImpl chunkWriter, IPointReader unseqReader, long timeLimit, int pathIdx)
      throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null
        && currTimeValuePairs[pathIdx].getTimestamp() < timeLimit) {
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      unseqReader.nextTimeValuePair();
      currTimeValuePairs[pathIdx] =
          unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
    }
    return ptWritten;
  }

  private int writeChunkWithUnseq(
      Chunk chunk,
      ChunkWriterImpl chunkWriter,
      IPointReader unseqReader,
      long chunkLimitTime,
      int pathIdx)
      throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  private int mergeWriteBatch(
      BatchData batchData, ChunkWriterImpl chunkWriter, IPointReader unseqReader, int pathIdx)
      throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      boolean overwriteSeqPoint = false;
      // unseq point.time <= sequence point.time, write unseq point
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() <= time) {
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

    //当前子合并任务的优先级队列，该队列存放着待合并序列的索引
    private PriorityQueue<Integer> chunkIdxHeap;
    //该数组存放每个待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该目标文件里的ChunkMetadataList）,若该目标文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null
    private MetaListEntry[] metaListEntries;
    //若第i个序列在该目标文件里存在Chunk，则该数组第i个位置标记为0
    private int[] ptWrittens;
    //当前待合并（目标）顺序文件的顺序阅读器
    private TsFileSequenceReader reader;
    //目标文件的writer
    private RestorableTsFileIOWriter mergeFileWriter;
    //当前所有待合并序列的乱序序列阅读器
    private IPointReader[] unseqReaders;
    //当前顺序待合并（目标）文件的TsFileResource
    private TsFileResource currFile;
    //当前顺序文件是否是最后一个待合并顺序文件
    private boolean isLastFile;
    private int taskNum;

    private int totalSeriesNum;

    /**
     *
     * @param chunkIdxHeap 每个子合并任务的优先级队列，该队列存放着待合并序列的索引
     * @param metaListEntries 该数组存放每个待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该目标文件里的ChunkMetadataList）,若该目标文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null
     * @param ptWrittens  若第i个序列在该目标文件里存在Chunk，则该数组第i个位置标记为0
     * @param reader   当前待合并（目标）顺序文件的顺序阅读器
     * @param mergeFileWriter  目标文件的writer
     * @param unseqReaders  当前所有待合并序列的乱序序列阅读器
     * @param currFile  当前顺序待合并（目标）文件的TsFileResource
     * @param isLastFile  当前顺序文件是否是最后一个待合并顺序文件
     * @param taskNum  Todo：合并子任务的索引
     */
    public MergeChunkHeapTask(
        PriorityQueue<Integer> chunkIdxHeap,
        MetaListEntry[] metaListEntries,
        int[] ptWrittens,
        TsFileSequenceReader reader,
        RestorableTsFileIOWriter mergeFileWriter,
        IPointReader[] unseqReaders,
        TsFileResource currFile,
        boolean isLastFile,
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
    }

    @Override
    public Void call() throws Exception {
      mergeChunkHeap();
      return null;
    }

    @SuppressWarnings("java:S2445") // avoid reading the same reader concurrently
    private void mergeChunkHeap() throws IOException {
      while (!chunkIdxHeap.isEmpty()) {
        //获取当前子合并任务的最高优先级序列的索引位置
        int pathIdx = chunkIdxHeap.poll();
        //获取该最高优先级的序列路径
        PartialPath path = currMergingPaths.get(pathIdx);
        //获取该序列的传感器配置类schema对象
        IMeasurementSchema measurementSchema = resource.getSchema(path);
        // chunkWriter will keep the data in memory
        //根据传感器配置类对象获取该传感器的ChunkWriter
        ChunkWriterImpl chunkWriter = resource.getChunkWriter(measurementSchema);
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        if (metaListEntries[pathIdx] != null) {
          //获取该最高优先级待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该目标文件里的ChunkMetadataList）
          MetaListEntry metaListEntry = metaListEntries[pathIdx];
          //获取该待合并序列在该目标文件里的下一个ChunkMetadata
          ChunkMetadata currMeta = metaListEntry.current();
          //判断该ChunkMetadata是否是待合并序列在该目标文件里的最后一个Chunk
          boolean isLastChunk = !metaListEntry.hasNext();
          //判断给定待合并序列的在乱序文件里的当前数据点是否与顺序目标文件里的该序列的该ChunkMetadata指向的Chunk重叠（若该序列的 乱序数据点时间<=顺序Chunk的结束时间，则为有重叠）
          boolean chunkOverflowed =
              MergeUtils.isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
          //判断当前顺序目标文件里的该Chunk的数据点数量是否太少，小于系统预设的数量100000
          boolean chunkTooSmall =
              MergeUtils.isChunkTooSmall(
                  ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

          Chunk chunk;
          synchronized (reader) {
            //从顺序目标文件里读出当前Chunk
            chunk = reader.readMemChunk(currMeta);
          }
          ptWrittens[pathIdx] =
              mergeChunkV2(
                  currMeta,
                  chunkOverflowed,
                  chunkTooSmall,
                  chunk,
                  ptWrittens[pathIdx],
                  pathIdx,
                  mergeFileWriter,
                  unseqReaders[pathIdx],
                  chunkWriter,
                  currFile);

          if (!isLastChunk) {
            metaListEntry.next();
            chunkIdxHeap.add(pathIdx);
            continue;
          }
        }
        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
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
