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
  //系统预设chunk最少数据点数量
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
    //遍历每个待合并顺序文件（为某个虚拟存储组下的某个时间分区里的待合并顺序文件）
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      // record the unmergeChunkStartTime for each sensor in each file
      //初始化，该变量存放每个TsFile上，未被合并的序列的每个Chunk的开始时间
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    //根据设备种类把传来的序列路径进行分开存放
    List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    //依次遍历该存储组下 每个设备下的所有序列路径
    for (List<PartialPath> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      //根据 当前存储组下的该设备下的所有序列路径 和 允许获取的最大序列数量 去创建序列选择器
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        //获取下批序列路径为当前正在被合并的序列，数量为maxSeriesNum个
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

    // 该序列在不同乱序文件里的所有chunk是被依次追加到该阅读器里的，要注意的是：某一序列在不同的乱序文件他们的时间戳并不是递增的，只能保证一个序列在同一个乱序文件里的一个ChunkGroup里是递增的，但是不同ChunkGroup之间就可能会出现overlap,更不用说文件之间，更有可能出现overlap
    //首先获取每个序列在所有待合并乱序文件里的所有Chunk，并以此创建每个序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
    IPointReader[] unseqReaders = resource.getUnseqReaders(currMergingPaths);
    //初始化待合并序列的当前数据点数组（在乱序文件里的），数量为当前待被合并的序列的数量
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
   * @param unseqReaders  当前所有待合并序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
   * @throws IOException
   */
  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders) throws IOException {
    //指定的待合并顺序文件
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    // all paths in one call are from the same device
    String deviceId = currMergingPaths.get(0).getDevice();
    //当前顺序文件里的当前设备的最小时间
    long currDeviceMinTime = currTsFile.getStartTime(deviceId);

    //遍历每个当前待合并序列，
    for (PartialPath path : currMergingPaths) {
      //该变量存放每个TsFile上，未被合并的序列的每个Chunk的开始时间。初始化当前TsFile上，未被合并的序列的每个Chunk的开始时间，此时每个Chunk的开始时间还是一个空列表
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    //从每个序列的当前数据点里，更新获取当前设备的最小时间
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
    //数组，存放每个待合并序列的在当前待合并顺序文件的ChunkMetadataList，有可能某个待合并序列根本就不存在于该待合并顺序文件里，则对应的元素是个空list
    List<ChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];
    //初始化当前待合并顺序文件下每个传感器ID和对应的ChunkMetadata列表的遍历器，使用该遍历器可以获取该文件的该设备在泽嵩树的下一个TimeseriesMetadata节点上的一批传感器ID和对应的ChunkMetadataList
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
        //当前传感器ID：为当前文件在当前设备下泽嵩树上某一TimeseriesMetadata节点里的某一个measurementId
        currSensor = measurementChunkMetadataListEntry.getKey();

        //初始化当前传感器currSensor在该待合并顺序文件里的删除操作和符合条件的ChunkMetadataList,放到modifications和seqChunkMeta数组合适的位置里
        // fill modifications and seqChunkMetas to be used later
        //循环遍历每个待合并序列
        for (int i = 0; i < currMergingPaths.size(); i++) {
          //若当前遍历的序列为当前TsFile该设备在泽嵩树上当前TimeseriesMetadata节点上的当前measurementId，则
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
    //存放当前待合并文件顺序阅读器和变量，该变量存放当前待合并顺序文件里当前设备在泽嵩树上某一个TimeseriesMetadata节点上的所有 measurementId和对应的ChunkMetadataList
    // update measurementChunkMetadataListMap
    chunkMetadataListCacheForMerge.put(fileSequenceReader, measurementChunkMetadataListMap);

    //判断所有待合并序列若都不存在于当前待合并顺序文件里，则直接返回
    //遍历每个当前待合并序列，判断它们在当前待合并顺序文件里是否存在数据，若不存在则过滤掉，返回未被过滤的序列的索引列表。（注意：若seqChunkMeta某个位置为空列表则说明该文件不存在该待合并序列，则该位置不被计入返回值里；若当前是最后一个顺序待合并文件且某不存在某序列，可是待合并乱序文件里存在，则不会过滤掉此序列）
    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);
    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    //创建临时目标文件的写入TsFileIOWriter。注意：跨空间合并的临时目标文件是"顺序文件名.tsfile.merge" ，即xxx.tsfile.merge！！
    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    //遍历每个待合并序列
    for (PartialPath path : currMergingPaths) {
      IMeasurementSchema schema = resource.getSchema(path);
      mergeFileWriter.addSchema(path, schema);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    // 开启一个新的设备ChunkGroup，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
    mergeFileWriter.startChunkGroup(deviceId);
    /**
     *
     * 1，创建系统预设数量（4个）的创建子合并任务线程（一个待合并顺序文件可能对应好几个该子合并线程，每个子线程用于合并不同的待合并序列，即把该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并），将当前所有的待合并序列平均分散地放入到每个合并子任务里的优先级队列里。（若待合并序列数量少于4个，则创建的子合并任务数量为序列数量即可）
     * 2. 该待合并顺序文件的所有合并子任务开始并行执行，即将该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并，具体操作如下：
     *      依优先级从队列里遍历每个待合并序列：
     *        1）遍历该序列在该待合并顺序文件里的每个Chunk:
     *           (1) 获取该序列的顺序chunk的一些相关属性，如对应的chunkMetadata、是否是该文件的最后一个chunk、该序列该所有待合并乱序里是否有数据点与该顺序chunk overlap、该chunk是否数据点太少等
     *           (2) 接着开始对该序列顺序chunk进行合并重写，即把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
     *                (2.1) 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
     *                      (2.2.1) 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
     *                      (2.2.2)
     *                          (2.2.2.1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
     *                          (2.2.2.2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
     *                              （2.2.2.2.1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
     *                              （2.2.2.2.2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
     *                      (2.2.3) 若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
     *            (3) 当已经是最后一个待合并顺序文件且完成该顺序文件里该序列数据的重写合并了（其实此处没有写入），可是该序列仍然存在乱序数据未被写入，说明所有待合并顺序文件都不存在该序列，则把该序列在乱序文件里的所有数据点先写到对应目标文件ChunkWriter里（下一步再追加写入目标文件的TsFileIOWriter里）
     *            (4) flush当前目标文件里该序列对应ChunkWriter的缓存到目标文件的TsFileIOWriter里
     * 3. 等待所有的子合并任务执行完毕，返回是否合并成功，若有该顺序文件有一个chunk被合并，则成功。
     */
    boolean dataWritten =
        mergeChunks(
            seqChunkMeta, //数组，存放每个待合并序列的在当前待合并顺序文件的ChunkMetadataList，若某个序列已被全部删除或该待合并顺序文件根本不存在此序列，则其对应的元素是个空List，代表没有任何的ChunkMetadata
            isLastFile, //当前待合并顺序文件是否是最后一个顺序文件
            fileSequenceReader,  //当前该待合并顺序文件的顺序阅读器
            unseqReaders,    //当前所有序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
            mergeFileWriter,  //目标文件（.tsfile.merge）的writer
            currTsFile);    //待合并顺序文件的TsFileResource
    //若合并成功，则
    if (dataWritten) {
      // 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
      mergeFileWriter.endChunkGroup();
      //往日志里写目标文件目前的文件长度是多少
      inplaceCompactionLogger.logFilePosition(mergeFileWriter.getFile());
      //更新当前被合并源顺序文件的该设备的起始时间
      currTsFile.updateStartTime(deviceId, currDeviceMinTime);
    }
  }

  /**
   *
   * @param seqChunkMeta 数组，存放每个待合并序列的在当前待合并顺序文件的ChunkMetadataList
   * @param seqFileIdx 跨空间合并资源管理器里待合并顺序文件列表中的索引
   * @return 返回的是未被过滤的、需要进行合并的
   */
  //遍历每个当前待合并序列，判断它们在当前待合并顺序文件里是否存在数据，若不存在则过滤掉，返回未被过滤的序列的索引列表。（注意：若seqChunkMeta某个位置为空列表则说明该文件不存在该待合并序列，则该位置不被计入返回值里；若当前是最后一个顺序待合并文件且某不存在某序列，可是待合并乱序文件里存在，则不会过滤掉此序列）
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
   * 1，创建系统预设数量（4个）的创建子合并任务线程（一个待合并顺序文件可能对应好几个该子合并线程，每个子线程用于合并不同的待合并序列，即把该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并），将当前所有的待合并序列平均分散地放入到每个合并子任务里的优先级队列里。（若待合并序列数量少于4个，则创建的子合并任务数量为序列数量即可）
   * 2. 该待合并顺序文件的所有合并子任务开始并行执行，即将该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并，具体操作如下：
   *      依优先级从队列里遍历每个待合并序列：
   *        1）遍历该序列在该待合并顺序文件里的每个Chunk:
   *           (1) 获取该序列的顺序chunk的一些相关属性，如对应的chunkMetadata、是否是该文件的最后一个chunk、该序列该所有待合并乱序里是否有数据点与该顺序chunk overlap、该chunk是否数据点太少等
   *           (2) 接着开始对该序列顺序chunk进行合并重写，即把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
   *                (2.1) 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
   *                      (2.2.1) 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
   *                      (2.2.2)
   *                          (2.2.2.1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
   *                          (2.2.2.2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
   *                              （2.2.2.2.1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
   *                              （2.2.2.2.2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
   *                      (2.2.3) 若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
   *            (3) 当已经是最后一个待合并顺序文件且完成该顺序文件里该序列数据的重写合并了（其实此处没有写入），可是该序列仍然存在乱序数据未被写入，说明所有待合并顺序文件都不存在该序列，则把该序列在乱序文件里的所有数据点先写到对应目标文件ChunkWriter里（下一步再追加写入目标文件的TsFileIOWriter里）
   *            (4) flush当前目标文件里该序列对应ChunkWriter的缓存到目标文件的TsFileIOWriter里
   * 3. 等待所有的子合并任务执行完毕，返回是否合并成功，若有该顺序文件有一个chunk被合并，则成功。
   *
   * @param seqChunkMeta //数组，存放每个待合并序列的在当前待合并顺序文件的ChunkMetadataList，若某个序列已被全部删除，则其对应的元素是个空List，代表没有任何的ChunkMetadata
   * @param isLastFile  //当前待合并顺序文件是否是最后一个顺序文件
   * @param reader    //当前待合并顺序文件的顺序阅读器
   * @param unseqReaders     //当前所有待合并序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
   * @param mergeFileWriter    //目标文件（.tsfile.merge）的writer
   * @param currFile    //待合并顺序文件的TsFileResource
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
    //存放该序列还未被刷到目标文件TsFileIoWriter的缓存，即仍存留在目标文件该序列的ChunkWriter里的数据点数量。初始时，若第i个序列在该待合并文件里存在Chunk，则该数组第i个位置标记为0
    int[] ptWrittens = new int[seqChunkMeta.length];
    //执行跨空间Chunk合并的子任务数量
    int mergeChunkSubTaskNum =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    //该数组存放每个待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该该合并顺序文件里的ChunkMetadataList）,若该待合并顺序文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null
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
    //（2）初始化metaListEntries：若该待合并顺序文件存在该序列，则创建该序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该待合并文件里的ChunkMetadataList），并放入metaListEntries具体索引位置
    //（3）初始化ptWrittens：若该待合并顺序文件存在该序列，则把具体位置标记0
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
      //子合并任务线程，一个待合并顺序文件可能对应好几个该子合并线程，每个子线程用于合并不同的待合并序列，即把该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并
      /**
       * 该合并子任务用于将该任务对应的待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并，具体操作如下：
*      * 1. 依优先级从队列里遍历每个待合并序列：
*      *    1）遍历该序列在该待合并顺序文件里的每个Chunk:
*      *       (1) 获取该序列的顺序chunk的一些相关属性，如对应的chunkMetadata、是否是该文件的最后一个chunk、该序列该所有待合并乱序里是否有数据点与该顺序chunk overlap、该chunk是否数据点太少等
*      *       (2) 接着开始对该序列顺序chunk进行合并重写，即把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
*      *           (2.1) 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
*      *               (2.2.1) 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
*      *               (2.2.2)
*      *                  (2.2.2.1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
*      *                  (2.2.2.2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
*      *                     （2.2.2.2.1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
*      *                     （2.2.2.2.2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
*      *               (2.2.3) 若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
*      *       (3) 当已经是最后一个待合并顺序文件且完成该顺序文件里该序列数据的重写合并了（其实此处没有写入），可是该序列仍然存在乱序数据未被写入，说明所有待合并顺序文件都不存在该序列，则把该序列在乱序文件里的所有数据点先写到对应目标文件ChunkWriter里（下一步再追加写入目标文件的TsFileIOWriter里）
*      *       (4) flush当前目标文件里该序列对应ChunkWriter的缓存到目标文件的TsFileIOWriter里
       */
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
        //等待该子合并任务线程执行完毕
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
  /**
   *
   * 把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
   * 1. 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
   * 2. 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
   * 3. 1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
   *    2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
   *       （1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
   *       （2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
   *    3）若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
   *
   * @param currMeta 该待合并序列在该顺序待合并文件里的下一个ChunkMetadata
   * @param chunkOverflowed 判断该序列在所有乱序文件里是否存在数据点其时间是小于等于该顺序Chunk的结束时间，当存在小于等于的数据则说明该序列的那些乱序文件里存在数据与该顺序Chunk有overlap。
   * @param chunkTooSmall 当前顺序待合并文件里currMeta指向的Chunk的数据点数量是否太少，小于系统预设的数量100000
   * @param chunk currMeta指向的chunk
   * @param lastUnclosedChunkPoint  若当前序列在当前顺序待合并文件里存在则为0，为该目标文件ChunkWriter还未刷盘的数据点数量
   * @param pathIdx 当前序列的索引
   * @param mergeFileWriter 临时目标文件的RetorableTsFileIOWriter
   * @param unseqReader 当前序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次读取优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
   * @param chunkWriter 该序列传感器的ChunkWriter
   * @param currFile  当前顺序待合并文件的TsFileResource
   * @return
   * @throws IOException
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
    //往目标文件写入该序列Chunk的ChunkWriter的数据点数量，先初始化为未刷盘的在ChunkWriter缓存里的数据点数量
    int unclosedChunkPoint = lastUnclosedChunkPoint;
    //判断当前Chunk是否有数据被删除修改过
    boolean chunkModified =
        (currMeta.getDeleteIntervalList() != null && !currMeta.getDeleteIntervalList().isEmpty());

    // no need to write the chunk to .merge file
    //若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写
    if (!fullMerge
        && lastUnclosedChunkPoint == 0
        && !chunkTooSmall
        && !chunkOverflowed
        && !chunkModified) {
      unmergedChunkNum.incrementAndGet();
      //给该待合并顺序TsFile上的该未被合并的序列增加一个Chunk的开始时间
      mergeContext
          .getUnmergedChunkStartTimes()
          .get(currFile)
          .get(currMergingPaths.get(pathIdx))
          .add(currMeta.getStartTime());
      return 0;
    }

    // write SK to .merge.file without compressing
    //若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里
    if (fullMerge
        && lastUnclosedChunkPoint == 0
        && !chunkTooSmall
        && !chunkOverflowed
        && !chunkModified) {
      synchronized (mergeFileWriter) {
        //将指定的Chunk（header+data）追加写入到该TsFile的out输出流里，并把该Chunk的ChunkMetadata加到当前写操作的ChunkGroup对应的所有ChunkMetadata列表里
        mergeFileWriter.writeChunk(chunk, currMeta);
      }
      //往跨空间合并环境类里增加当前写入的Chunk包含的数据点数量
      mergeContext.incTotalPointWritten(currMeta.getNumOfPoints());
      //往跨空间合并环境类里增加一个Chunk的写入数量
      mergeContext.incTotalChunkWritten();
      mergedChunkNum.incrementAndGet();
      return 0;
    }

    // 3.1 SK isn't overflowed, just uncompress and write sequence chunk
    //若当前序列在乱序文件里不存在数据与顺序Chunk有overlap
    if (!chunkOverflowed) {
      //将该待合并顺序文件里的某一待合并序列传感器的Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
      unclosedChunkPoint += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergedChunkNum.incrementAndGet();
    } else {
      //当前序列在乱序文件里存在数据与顺序Chunk有overlap
      // 3.2 SK is overflowed, uncompress sequence chunk and merge with unseq chunk, then write
      //将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
      //（1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
      //（2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
      unclosedChunkPoint +=
          writeChunkWithUnseq(chunk, chunkWriter, unseqReader, currMeta.getEndTime(), pathIdx);
      mergedChunkNum.incrementAndGet();
    }

    // update points written statistics
    //更新此次新写入目标文件该序列的数据点数量
    mergeContext.incTotalPointWritten((long) unclosedChunkPoint - lastUnclosedChunkPoint);
    //当该序列chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存
    if (minChunkPointNum > 0 && unclosedChunkPoint >= minChunkPointNum
        || unclosedChunkPoint > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (mergeFileWriter) {
        // 首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
        chunkWriter.writeToFileWriter(mergeFileWriter);
      }
      unclosedChunkPoint = 0;
    }
    return unclosedChunkPoint;
  }

  /**
   * 往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。（timeLimit为当前待合并顺序文件里的当前待合并序列的该Chunk的结束时间）
   *
   * @param chunkWriter 临时目标文件里该序列的ChunkWriter
   * @param unseqReader 当前序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次读取优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
   * @param timeLimit 当前待合并顺序文件里的当前待合并序列的该Chunk的结束时间
   * @param pathIdx 该序列的索引
   * @return
   * @throws IOException
   */
  private int writeRemainingUnseq(
      ChunkWriterImpl chunkWriter, IPointReader unseqReader, long timeLimit, int pathIdx)
      throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null
        && currTimeValuePairs[pathIdx].getTimestamp() < timeLimit) {
      //往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里下一个最高优先级的数据点
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      //该序列获取下一个在乱序文件里最高优先级的数据点
      unseqReader.nextTimeValuePair();
      currTimeValuePairs[pathIdx] =
          unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
    }
    return ptWritten;
  }

  /**
   * 将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
   * （1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
   * （2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
   *
   * @param chunk 当前待合并顺序文件里的当前待合并序列的某一个Chunk
   * @param chunkWriter 临时目标文件里该序列的ChunkWriter
   * @param unseqReader  当前序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次读取优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
   * @param chunkLimitTime 当前待合并顺序文件里的当前待合并序列的该Chunk的结束时间
   * @param pathIdx 当前序列的索引
   * @return
   * @throws IOException
   */
  private int writeChunkWithUnseq(
      Chunk chunk,
      ChunkWriterImpl chunkWriter,
      IPointReader unseqReader,
      long chunkLimitTime,
      int pathIdx)
      throws IOException {
    int cnt = 0;
    //创建该待合并顺序文件里的该待合并序列传感器的Chunk的阅读器
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    //遍历该顺序文件里该序列的该Chunk符合条件（未被删除）的所有page
    while (chunkReader.hasNextSatisfiedPage()) {
      //获取该Chunk的下一page里未被删除的数据
      BatchData batchData = chunkReader.nextPageData();
      //遍历当前待合并顺序文件里该序列的当前Chunk的当前page里的每个数据点：依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，此处要注意的是，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里，否则直接写如乱序文件里的该时间戳数据点即可（因为同个时间戳，乱序文件里的数据是比较新的）,返回写入的数据点数量
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    //该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。（timeLimit为当前待合并顺序文件里的当前待合并序列的该Chunk的结束时间）
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  /**
   *遍历当前待合并顺序文件里该序列的当前Chunk的当前page里的每个数据点：依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，此处要注意的是，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里，否则直接写如乱序文件里的该时间戳数据点即可（因为同个时间戳，乱序文件里的数据是比较新的）,返回写入的数据点数量
   *
   * @param batchData  该待合并顺序文件里的该待合并序列传感器的Chunk的某一个page数据
   * @param chunkWriter 临时目标文件里该序列的ChunkWriter
   * @param unseqReader 当前序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次读取优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
   * @param pathIdx 当前序列的索引
   * @return
   * @throws IOException
   */
  private int mergeWriteBatch(
      BatchData batchData, ChunkWriterImpl chunkWriter, IPointReader unseqReader, int pathIdx)
      throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      //判断该序列在所有乱序文件里是否有等于该顺序数据点时间戳的乱序数据
      boolean overwriteSeqPoint = false;

      // unseq point.time <= sequence point.time, write unseq point
      //将当前序列在乱序文件里所有 “小于等于该序列在该待合并顺序文件里该chunk的当前page的当前数据点” 的所有数据点写入临时目标文件里该序列的ChunkWriter里。
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() <= time) {
        //把乱序文件里该序列的当前数据点写入该序列在目标文件里的chunkwriter。 使用ChunkWriter将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        if (currTimeValuePairs[pathIdx].getTimestamp() == time) {
          overwriteSeqPoint = true;
        }
        //该序列获取下一个在乱序文件里最高优先级的数据点
        unseqReader.nextTimeValuePair();
        currTimeValuePairs[pathIdx] =
            unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
        cnt++;
      }
      // unseq point.time > sequence point.time, write seq point
      //若当前序列在所有乱序文件中没有数据点与当前顺序数据点时间戳相同，则此时往目标文件写入当前顺序数据点
      if (!overwriteSeqPoint) {
        //将batchData第i个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }

  //子合并任务线程，一个待合并顺序文件可能对应好几个该子合并线程，每个子线程用于合并不同的待合并序列，即把该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并
  public class MergeChunkHeapTask implements Callable<Void> {

    //当前子合并任务的优先级队列，该队列存放着待合并序列的索引，优先级是按照字典序升序排列的（字典序最小的优先级最高），如有5个子合并任务和10个序列，则第1到第5个合并任务的队列里分别是{(1,6),(2,7),(3,8),(4,9),(5,10)}
    private PriorityQueue<Integer> chunkIdxHeap;
    //该数组存放每个待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该待合并文件里的ChunkMetadataList）,若该待合并文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null
    private MetaListEntry[] metaListEntries;
    //存放该序列还未被刷到目标文件TsFileIoWriter的缓存，即仍存留在目标文件该序列的ChunkWriter里的数据点数量。初始时，若第i个序列在该待合并文件里存在Chunk，则该数组第i个位置标记为0
    private int[] ptWrittens;
    //当前待合并顺序文件的顺序阅读器
    private TsFileSequenceReader reader;
    //临时目标文件的writer
    private RestorableTsFileIOWriter mergeFileWriter;
    //当前所有待合并序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
    private IPointReader[] unseqReaders;
    //当前顺序待合并文件的TsFileResource
    private TsFileResource currFile;
    //当前顺序文件是否是最后一个待合并顺序文件
    private boolean isLastFile;

    //该合并子任务的索引，即该合并子任务是所有合并子任务中的第几个
    private int taskNum;

    //当前合并子任务有几个待合并序列
    private int totalSeriesNum;

    /**
     *
     * @param chunkIdxHeap 每个子合并任务的优先级队列，该队列存放着待合并序列的索引
     * @param metaListEntries 该数组存放每个待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该待合并文件里的ChunkMetadataList）,若该待合并文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null
     * @param ptWrittens  若第i个序列在该待合并文件里存在Chunk，则该数组第i个位置标记为0
     * @param reader   当前待合并顺序文件的顺序阅读器
     * @param mergeFileWriter  临时目标文件的writer
     * @param unseqReaders  当前所有待合并序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
     * @param currFile  当前顺序待合并文件的TsFileResource
     * @param isLastFile  当前顺序文件是否是最后一个待合并顺序文件
     * @param taskNum  该合并子任务的索引，即该合并子任务是所有合并子任务中的第几个
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

    /**
     * 该合并子任务用于将该任务对应的待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并，具体操作如下：
     * 1. 依优先级从队列里遍历每个待合并序列：
     *    1）遍历该序列在该待合并顺序文件里的每个Chunk:
     *       (1) 获取该序列的顺序chunk的一些相关属性，如对应的chunkMetadata、是否是该文件的最后一个chunk、该序列该所有待合并乱序里是否有数据点与该顺序chunk overlap、该chunk是否数据点太少等
     *       (2) 接着开始对该序列顺序chunk进行合并重写，即把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
     *           (2.1) 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
     *               (2.2.1) 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
     *               (2.2.2)
     *                  (2.2.2.1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
     *                  (2.2.2.2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
     *                     （2.2.2.2.1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
     *                     （2.2.2.2.2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
     *               (2.2.3) 若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
     *       (3) 当已经是最后一个待合并顺序文件且完成该顺序文件里该序列数据的重写合并了（其实此处没有写入），可是该序列仍然存在乱序数据未被写入，说明所有待合并顺序文件都不存在该序列，则把该序列在乱序文件里的所有数据点先写到对应目标文件ChunkWriter里（下一步再追加写入目标文件的TsFileIOWriter里）
     *       (4) flush当前目标文件里该序列对应ChunkWriter的缓存到目标文件的TsFileIOWriter里
     *
     * @throws IOException
     */
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
        //根据传感器配置类对象获取该序列传感器的ChunkWriter
        ChunkWriterImpl chunkWriter = resource.getChunkWriter(measurementSchema);
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        //依次合并该序列在该待合并顺序文件里的所有Chunk
        //若该待合并文件里存在该待合并序列  （待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该待合并文件里的ChunkMetadataList）,若该待合并文件里原先不存在某待合并序列，则该数组对应的索引位置元素为null）
        if (metaListEntries[pathIdx] != null) {
          //获取该最高优先级待合并序列的MetaListEntry对象（该对象存放该序列的索引和序列对应在该待合并文件里的ChunkMetadataList）
          MetaListEntry metaListEntry = metaListEntries[pathIdx];
          //获取该待合并序列在该待合并文件里的下一个ChunkMetadata
          ChunkMetadata currMeta = metaListEntry.current();
          //判断该ChunkMetadata是否是待合并序列在该待合并文件里的最后一个Chunk
          boolean isLastChunk = !metaListEntry.hasNext();
          //判断该序列在所有乱序文件里是否存在数据点其时间是小于等于该顺序Chunk的结束时间，当存在小于等于的数据则说明该序列的那些乱序文件里存在数据与该顺序Chunk有overlap。
          boolean chunkOverflowed =
              MergeUtils.isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
          //判断当前顺序待合并文件里的该Chunk的数据点数量是否太少，小于系统预设的数量100000
          boolean chunkTooSmall =
              MergeUtils.isChunkTooSmall(
                  ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

          Chunk chunk;
          synchronized (reader) {
            //从顺序待合并文件里读出当前Chunk
            chunk = reader.readMemChunk(currMeta);
          }
          //把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
          //    1. 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
          //    2. 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
          //    3. 1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
          //       2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit，返回写入目标文件的数据点数量。具体：
          //          （1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
          //          （2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
          //       3）若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
          ptWrittens[pathIdx] = //存放该序列还未被刷到目标文件TsFileIoWriter的缓存，即仍存留在目标文件该序列的ChunkWriter里的数据点数量
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

          //若在当前待合并顺序文件里，该序列的当前Chunk不是最后一个Chunk
          if (!isLastChunk) {
            //指针移到下一个Chunk
            metaListEntry.next();
            //再往chunkIdxHeap里增加该序列
            chunkIdxHeap.add(pathIdx);
            continue;
          }
        }
        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        //当已经是最后一个待合并顺序文件且完成该顺序文件里该序列数据的重写合并了（其实此处没有写入），可是该序列仍然存在乱序数据未被写入，说明所有待合并顺序文件都不存在该序列，则把该序列在乱序文件里的所有数据点先写到对应目标文件ChunkWriter里，后续再追加写入目标文件的TsFileIOWriter里
        if (isLastFile && currTimeValuePairs[pathIdx] != null) {
          //往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。（此处timeLimit为无穷大）
          ptWrittens[pathIdx] +=
              writeRemainingUnseq(chunkWriter, unseqReaders[pathIdx], Long.MAX_VALUE, pathIdx);
          mergedChunkNum.incrementAndGet();
        }
        // the last merged chunk may still be smaller than the threshold, flush it anyway
        //flush当前目标文件里该序列对应ChunkWriter的缓存到目标文件的TsFileIOWriter里
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
