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

package org.apache.iotdb.db.engine.compaction.inner.utils;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MergeFileStrategy;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

public class InnerSpaceCompactionUtils {

  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");
  public static final String COMPACTION_LOG_SUFFIX = ".compaction_log";

  private InnerSpaceCompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  //根据每个待合并TsFile顺序读取器和其某sensor传感器的对应ChunkMetadataList，将该sensor在不同待合并文件里的所有Chunk和ChunkMetadata合并到第一个Chunk和第一个chunkMetadata里，返回合并后的chunk和chunkMetadata
  private static Pair<ChunkMetadata, Chunk> readByAppendPageMerge(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap) throws IOException {
    ChunkMetadata newChunkMetadata = null;
    Chunk newChunk = null;
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        //根据ChunkMetadata读取其对应的Chunk
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        if (newChunkMetadata == null) {
          newChunkMetadata = chunkMetadata;
          newChunk = chunk;
        } else {
          //将参数的chunk的内容合并到当前chunk对象，即把待合并的参数chunk的chunkData部分追加到当前chunk的chunkData后，并更新该chunk的ChunkHeader。要注意的是，若参数chunk或者当前chunk只有一个page，则需要为其补上自己的pageStatistics，因为合并后的当前新chunk一定会至少有两个page（若当前Chunk只有一个page，则合并是把新的chunk的page追加当作新的page追加到当前chunk的原有page后）
          newChunk.mergeChunk(chunk);
          newChunkMetadata.mergeChunkMetadata(chunkMetadata);
        }
      }
    }
    return new Pair<>(newChunkMetadata, newChunk);
  }

  //依次遍历每个待合并TsFile：
  // （1）获取对该TsFile对该序列的所有删除操作，并修改chunkMetadataList（若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)）
  // （2）遍历该文件里的该sensor的所有ChunkMetadata，获取该Chunk对应的读取器和该Chunk每个page里的数据点读取器，把该Chunk所有符合条件（未被删除、满足过滤器）的数据点读取出来装进timeValuePairMap
  private static void readByDeserializePageMerge(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap,
      Map<Long, TimeValuePair> timeValuePairMap,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath)
      throws IOException {
    //遍历每个待被合并文件的顺序读取器和该sensor的ChunkMetadataList
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      //获取该TsFile对该序列的所有删除操作，然后修改或删除chunkMetadata，具体操作是：若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)
      modifyChunkMetaDataWithCache(reader, chunkMetadataList, modificationCache, seriesPath);
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        //创建该TsFile的该Chunk对应的读取器
        IChunkReader chunkReader = new ChunkReaderByTimestamp(reader.readMemChunk(chunkMetadata));
        //若该Chunk还有下个page
        while (chunkReader.hasNextSatisfiedPage()) {
          //获取该page每个数据点的读取器
          IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
          //若该page还有下个数据点
          while (batchIterator.hasNextTimeValuePair()) {
            //读取下个数据点并放入timeValuePairMap里
            TimeValuePair timeValuePair = batchIterator.nextTimeValuePair();
            timeValuePairMap.put(timeValuePair.getTimestamp(), timeValuePair);
          }
        }
      }
    }
  }

  /**
   * When chunk is large enough, we do not have to merge them any more. Just read chunks and write
   * them to the new file directly.
   */
  // 当该sensor传感器在原先待合并的所有TsFile里的所有Chunk的数据点数量都大于系统预设Chunk数据点数量，则直接往目标文件里依次追加写入该sensor的这些Chunk，写入的同时用限制器限流，并更新目标文件该设备的开始和结束时间
  public static void writeByAppendChunkMerge(
      String device,
      RateLimiter compactionWriteRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry, //某一个measurementId对应的在不同文件的<TsFileSequenceReader,chunkmetadataList>
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer)
      throws IOException {
    Map<TsFileSequenceReader, List<ChunkMetadata>> readerListMap = entry.getValue() ;
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> readerListEntry :
        readerListMap.entrySet()) {
      TsFileSequenceReader reader = readerListEntry.getKey();
      List<ChunkMetadata> chunkMetadataList = readerListEntry.getValue();
      // read chunk and write it to new file directly
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        //根据ChunkMetadata读取其对应的Chunk
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        //通过限制器的令牌数，来限制访问流量
        MergeManager.mergeRateLimiterAcquire(
            compactionWriteRateLimiter,
            (long) chunk.getHeader().getDataSize() + chunk.getData().position());
        //将指定的Chunk（header+data）写入到该TsFile的out输出流里，并把该Chunk的ChunkMetadata加到当前写操作的ChunkGroup对应的所有ChunkMetadata类对象列表里
        writer.writeChunk(chunk, chunkMetadata);
        //更新目标文件里该设备的开始和结束时间
        targetResource.updateStartTime(device, chunkMetadata.getStartTime());
        targetResource.updateEndTime(device, chunkMetadata.getEndTime());
      }
    }
  }

  // 当该sensor传感器在原先待合并的所有TsFile里存在一个以上Chunk的数据点数量小于系统预设Chunk数据点数量 但是 所有Chunk的数据点数量都大于系统预设page数据点数量，则把该sensor在不同待合并文件里的所有Chunk和ChunkMetadata合并到第一个Chunk和ChunkMetadata里，并用目标文件的writer将合并后的新Chunk写入到目标文件里
  public static void writeByAppendPageMerge(
      String device,
      RateLimiter compactionWriteRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry, //某一个measurementId对应的在不同文件的<TsFileSequenceReader,chunkmetadataList>
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer)
      throws IOException {
    //根据每个待合并TsFile顺序读取器和其某sensor传感器的对应ChunkMetadataList，将该sensor在不同待合并文件里的所有Chunk和ChunkMetadata合并到第一个Chunk和第一个chunkMetadata里，返回合并后的chunk和chunkMetadata
    Pair<ChunkMetadata, Chunk> chunkPair = readByAppendPageMerge(entry.getValue());
    ChunkMetadata newChunkMetadata = chunkPair.left;
    Chunk newChunk = chunkPair.right;
    if (newChunkMetadata != null && newChunk != null) {
      // wait for limit write
      //通过限制器的令牌数，来限制访问流量
      MergeManager.mergeRateLimiterAcquire(
          compactionWriteRateLimiter,
          (long) newChunk.getHeader().getDataSize() + newChunk.getData().position());
      //将指定的newChunk（header+data）写入到该TsFile的out输出流里，并把该Chunk的ChunkMetadata加到当前写操作的ChunkGroup对应的所有ChunkMetadata类对象列表里
      writer.writeChunk(newChunk, newChunkMetadata);
      targetResource.updateStartTime(device, newChunkMetadata.getStartTime());
      targetResource.updateEndTime(device, newChunkMetadata.getEndTime());
    }
  }

  //获取每个待合并文件的删除操作，根据每个文件对该序列的删除操作获取该sensor所有符合条件的数据点，然后创建目标文件对应该sensor的chunkWriterImpl，依次把所有数据点写入，最后刷到目标文件writer里的缓存里
  public static void writeByDeserializePageMerge(
      String device,
      RateLimiter compactionRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry, //某一个measurementId对应的在不同文件的<TsFileSequenceReader,chunkmetadataList>
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer,
      Map<String, List<Modification>> modificationCache)
      throws IOException, IllegalPathException {
    Map<Long, TimeValuePair> timeValuePairMap = new TreeMap<>();
    Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap = entry.getValue();
    //依次遍历每个待合并TsFile：
    // （1）获取对该TsFile对该序列的所有删除操作，并修改chunkMetadataList（若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)）
    // （2）遍历该文件里的该sensor的所有ChunkMetadata，获取该Chunk对应的读取器和该Chunk每个page里的数据点读取器，把该Chunk所有符合条件（未被删除、满足过滤器）的数据点读取出来装进timeValuePairMap
    readByDeserializePageMerge(
        readerChunkMetadataMap,
        timeValuePairMap,
        modificationCache,
        new PartialPath(device, entry.getKey()));
    boolean isChunkMetadataEmpty = true;
    //判断每个待合并TsFile的该sensor的ChunkMetadataList是否未空，若为空，则说明该文件不存在此sensor。只要有一个待合并文件的该sensor的ChunkmetadataList不为空，则会继续往下做写操作
    for (List<ChunkMetadata> chunkMetadataList : readerChunkMetadataMap.values()) {
      if (!chunkMetadataList.isEmpty()) {
        isChunkMetadataEmpty = false;
        break;
      }
    }
    if (isChunkMetadataEmpty) {
      return;
    }
    //若存在某个待合并文件的该sensor的ChunkmetadataList不为空，则创建目标文件的该sensor的ChunkWriterImpl写入对象
    ChunkWriterImpl chunkWriter;
    try {
      chunkWriter =
          new ChunkWriterImpl(
              IoTDB.metaManager.getSeriesSchema(new PartialPath(device, entry.getKey())), true);
    } catch (MetadataException e) {
      // this may caused in IT by restart
      logger.error("{} get schema {} error, skip this sensor", device, entry.getKey(), e);
      return;
    }
    //遍历该sensor的每个数据点，把所有的数据点写到对应的pageWriter的缓存，并判断是否开启新的page
    for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
      // 使用ChunkWriter将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
      writeTVPair(timeValuePair, chunkWriter);
      targetResource.updateStartTime(device, timeValuePair.getTimestamp());
      targetResource.updateEndTime(device, timeValuePair.getTimestamp());
    }
    // wait for limit write
    MergeManager.mergeRateLimiterAcquire(
        compactionRateLimiter, chunkWriter.estimateMaxSeriesMemSize());
    // 首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
    chunkWriter.writeToFileWriter(writer);
  }

  //对TsFileResource列表里每个TsFile初始化其对应的顺序读取器放入tsFileSequenceReaderMap对象（<TsFile绝对路径，顺序读取器对象>）里，并获取并返回所有TsFile包含的所有设备ID
  private static Set<String> getTsFileDevicesSet(
      List<TsFileResource> subLevelResources,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,//<TsFile绝对路径，顺序读取器对象>
      String storageGroup)
      throws IOException {
    Set<String> tsFileDevicesSet = new HashSet<>();
    for (TsFileResource levelResource : subLevelResources) {
      //根据指定TsFileResource，创建其对应的TsFileSequenceReader对象放进tsFileSequenceReaderMap里<TsFile绝对路径，顺序读取器对象>
      TsFileSequenceReader reader =
          buildReaderFromTsFileResource(levelResource, tsFileSequenceReaderMap, storageGroup);
      if (reader == null) {
        continue;
      }
      //将该TsFile里的所有设备ID放入tsFileDevicesSet对象里
      tsFileDevicesSet.addAll(reader.getAllDevices());
    }
    return tsFileDevicesSet;
  }

  //返回该设备ID下是否存在下一个传感器ID和对应的ChunkMetadata列表，若某个文件的该设备还存在下个传感器，则返回true
  private static boolean hasNextChunkMetadataList(
      Collection<Iterator<Map<String, List<ChunkMetadata>>>> iteratorSet) { //参数是每个待合并文件在该设备ID下的传感器遍历器
    boolean hasNextChunkMetadataList = false;
    for (Iterator<Map<String, List<ChunkMetadata>>> iterator : iteratorSet) {
      hasNextChunkMetadataList = hasNextChunkMetadataList || iterator.hasNext();
    }
    return hasNextChunkMetadataList;
  }

  /**
   * @param targetResource the target resource to be merged to
   * @param tsFileResources the source resource to be merged
   * @param storageGroup the storage group name
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void compact(
      TsFileResource targetResource,  //目标TsFile
      List<TsFileResource> tsFileResources, //待被合并的所有TsFile
      String storageGroup,
      boolean sequence)
      throws IOException, IllegalPathException {
    //<TsFile绝对路径，顺序读取器对象>
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    RestorableTsFileIOWriter writer = null;
    try {
      writer =
          new RestorableTsFileIOWriter(
              targetResource.getTsFile()); // 根据目标文件创建其RestorableTsFileIOWriter
      //用于后续存放每个待合并TsFile对应各自的所有删除操作
      Map<String, List<Modification>> modificationCache = new HashMap<>();
      RateLimiter compactionWriteRateLimiter = // 获取合并写入的流量限制器
          MergeManager.getINSTANCE().getMergeWriteRateLimiter();
      //1. 对TsFileResource列表里每个TsFile初始化其对应的顺序读取器放入tsFileSequenceReaderMap对象（<TsFile绝对路径，顺序读取器对象>）里，并获取并返回所有TsFile包含的所有设备ID
      Set<String> tsFileDevicesMap =
          getTsFileDevicesSet(tsFileResources, tsFileSequenceReaderMap, storageGroup);

      //2. 遍历所有待合并的TsFile的所有设备ID
      for (String device : tsFileDevicesMap) {
        //2.1 开启一个新的设备ChunkGroup，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
        writer.startChunkGroup(device);
        // tsfile -> measurement -> List<ChunkMetadata>
        //该设备ID下待被合并的Chunk，存放每个待合并文件的顺序读取器，以及每个待合并文件在该设备ID下的传感器ID和对应的ChunkMetadata列表,即<TsFileSequenceReader,<measurementId,ChunkMetadataList>>
        Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>> chunkMetadataListCacheForMerge =
            new TreeMap<>(
                (o1, o2) ->
                    TsFileManager.compareFileName(
                        new File(o1.getFileName()), new File(o2.getFileName())));
        // tsfile -> iterator to get next Map<Measurement, List<ChunkMetadata>>
        //存放每个待合并文件的顺序读取器，以及每个待合并文件在该设备ID下的传感器遍历器，使用该遍历器可以获取该文件的该设备下的下一个传感器ID和对应的ChunkMetadata列表
        Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
            chunkMetadataListIteratorCache =
                new TreeMap<>(
                    (o1, o2) ->
                        TsFileManager.compareFileName(
                            new File(o1.getFileName()), new File(o2.getFileName())));

        //2.2 遍历每个待合并文件：
        // （1）创建每个待合并文件的“顺序读取器”和“指定设备下每个传感器ID和对应的ChunkMetadata列表的遍历器”放入chunkMetadataListIteratorCache里
        // （2）将每个待合并文件的“顺序读取器”和一个空map放进chunkMetadataListCacheForMerge里
        for (TsFileResource tsFileResource : tsFileResources) {
          //根据指定TsFileResource，创建其对应的TsFileSequenceReader对象放进tsFileSequenceReaderMap里<TsFile绝对路径，顺序读取器对象>,并返回指定TsFile的SequenceReader
          TsFileSequenceReader reader =
              buildReaderFromTsFileResource(tsFileResource, tsFileSequenceReaderMap, storageGroup);
          if (reader == null) {
            throw new IOException();
          }
          //返回指定设备ID下每个传感器ID和对应的ChunkMetadata列表的遍历器，使用该遍历器可以获取该文件的该设备下的下一个传感器ID和对应的ChunkMetadata列表
          Iterator<Map<String, List<ChunkMetadata>>> iterator =
              reader.getMeasurementChunkMetadataListMapIterator(device);
          //将该待合并文件的 顺序读取器 和 对应的设备下的每个传感器ID和对应的ChunkMetadata列表的遍历器 放入chunkMetadataListIteratorCache
          chunkMetadataListIteratorCache.put(reader, iterator);
          chunkMetadataListCacheForMerge.put(reader, new TreeMap<>());
        }

        //返回该设备ID下是否存在下一个传感器ID和对应的ChunkMetadata列表，若某个文件的该设备还存在下个传感器，则返回true
        //2.3 若有一个及以上的待合并文件的该设备ID存在下一个传感器，则
        while (hasNextChunkMetadataList(chunkMetadataListIteratorCache.values())) {
          String lastSensor = null;
          Set<String> allSensors = new HashSet<>();
          //2.3.1 遍历每个待合并文件：
          //（1）若chunkMetadataListCacheForMerge里该文件对应的仍为空，则使用chunkMetadataListIteratorCache里该文件对应的遍历器获取下一个该文件的该设备下的“所有传感器对应各自的ChunkMetadataList”，放进chunkMetadataListCacheForMerge里该文件对应的值
          //（2）获取所有文件中该设备下最小的measurementId放入lastSensor，并把所有待合并文件的该设备下所有传感器放入allsensors变量里
          for (Entry<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
              chunkMetadataListCacheForMergeEntry : chunkMetadataListCacheForMerge.entrySet()) {
            //该待被合并TsFile的顺序读取器
            TsFileSequenceReader reader = chunkMetadataListCacheForMergeEntry.getKey();
            //该待合并文件的该设备ID下的所有传感器ID和各自对应的ChunkMetadataList
            Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =  //Todo:此处应该是空，没必要写！
                chunkMetadataListCacheForMergeEntry.getValue();

            //获取or初始化该文件的该设备ID下的下一个传感器ID和对应的ChunkMetadataList 放入chunkMetadataListCacheForMerge里
            if (sensorChunkMetadataListMap.size() <= 0) {
              //通过该TsFile在该设备下的遍历器，判断该设备下是否还存在下个传感器，若还存在，则
              if (chunkMetadataListIteratorCache.get(reader).hasNext()) {
                //获取该文件的该设备的下个传感器ID和对应的ChunkMetadata列表，并放进chunkMetadataListCacheForMerge里
                sensorChunkMetadataListMap = chunkMetadataListIteratorCache.get(reader).next();
                chunkMetadataListCacheForMerge.put(reader, sensorChunkMetadataListMap);
              } else {
                continue;
              }
            }

            // get the min last sensor in the current chunkMetadata cache list for merge
            String maxSensor = Collections.max(sensorChunkMetadataListMap.keySet()); //获取最大的measurementId
            if (lastSensor == null) {
              lastSensor = maxSensor;
            } else {
              if (maxSensor.compareTo(lastSensor) < 0) {
                lastSensor = maxSensor;
              }
            }
            // get all sensor used later
            allSensors.addAll(sensorChunkMetadataListMap.keySet());
          }

          //返回该设备ID下是否存在下一个传感器ID和对应的ChunkMetadata列表，若某个文件的该设备还存在下个传感器，则返回true
          //2.3.2 若没有一个待合并文件在该设备下还存在下一个传感器，则lastSensor为allSensors里最大的measurementId
          // if there is no more chunkMetaData, merge all the sensors
          if (!hasNextChunkMetadataList(chunkMetadataListIteratorCache.values())) {
            lastSensor = Collections.max(allSensors);
          }

          //2.3.3 遍历所有待合并文件在该设备下的所有传感器ID
          for (String sensor : allSensors) {
            if (sensor.compareTo(lastSensor) <= 0) { //Todo:??
              //存放每个TsFile顺序读取器和对应待被合并的此sensor传感器的ChunkMetadata列表
              Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataListMap =
                  new TreeMap<>(
                      (o1, o2) ->
                          TsFileManager.compareFileName(
                              new File(o1.getFileName()), new File(o2.getFileName())));
              // find all chunkMetadata of a sensor
              //2.3.3.1 从chunkMetadataListCacheForMerge里遍历所有文件，判断每个文件在该设备下的所有传感器map里是否存在该sensor，若存在则将“该文件的顺序读取器”和“该文件该设备的该sensor的ChunkMetadataList”放入readerChunkMetadataListMap里
              for (Entry<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
                  chunkMetadataListCacheForMergeEntry : chunkMetadataListCacheForMerge.entrySet()) {
                TsFileSequenceReader reader = chunkMetadataListCacheForMergeEntry.getKey();
                Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =
                    chunkMetadataListCacheForMergeEntry.getValue();
                if (sensorChunkMetadataListMap.containsKey(sensor)) {
                  readerChunkMetadataListMap.put(reader, sensorChunkMetadataListMap.get(sensor));
                  sensorChunkMetadataListMap.remove(sensor);
                }
              }
              //创建临时entry变量,<该sensor的measurementID，<TsFileSequenceReader,ChunkMetadataList>>
              Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>>
                  sensorReaderChunkMetadataListEntry =
                      new DefaultMapEntry<>(sensor, readerChunkMetadataListMap);
              //2.3.3.2 若是乱序空间内合并
              if (!sequence) {
                //2.3.3.2.1 获取每个待合并文件的删除操作，根据每个文件对该序列的删除操作获取该sensor所有符合条件的数据点，然后创建目标文件对应该sensor的chunkWriterImpl，依次把所有数据点写入，最后刷到目标文件writer里的缓存里
                writeByDeserializePageMerge(
                    device,
                    compactionWriteRateLimiter,
                    sensorReaderChunkMetadataListEntry,
                    targetResource,
                    writer,
                    modificationCache);
              } else { //2.3.3.3 顺序空间内合并
                //那些待被合并TsFile的该sensor下的
                boolean isChunkEnoughLarge = true;
                boolean isPageEnoughLarge = true;
                BigInteger totalChunkPointNum = new BigInteger("0");  //该sensor的总数据点数量
                long totalChunkNum = 0; //所有文件下该sensor的Chunk总数量
                long maxChunkPointNum = Long.MIN_VALUE; //某Chunk最大数据点数量
                long minChunkPointNum = Long.MAX_VALUE; //某Chunk最小数据点数量

                //2.3.3.3.1 判断每个待被合并文件里的该sensor的每个Chunk里数据点数量是否超过系统预设的Chunk或者page数据点数量，分别用isChunkEnoughLarge和isPageEnoughLarge标记
                //遍历每个待被合并文件里该sensor传感器的ChunkMetadata列表
                for (List<ChunkMetadata> chunkMetadatas : readerChunkMetadataListMap.values()) {
                  //遍历该文件的该设备下的该sensor的每个ChunkMetadata
                  for (ChunkMetadata chunkMetadata : chunkMetadatas) {
                    //只要有某一Chunk的数据点数量小于系统预设的一个page的数据点数量，则isPageEnoughLarge为false
                    if (chunkMetadata.getNumOfPoints()
                        < IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getMergePagePointNumberThreshold()) {
                      isPageEnoughLarge = false;
                    }
                    //只要有某一Chunk的数据点数量小于系统预设的一个Chunk的数据点数量，则isChunkEnoughLarge为false
                    if (chunkMetadata.getNumOfPoints()
                        < IoTDBDescriptor.getInstance()
                            .getConfig()
                            .getMergeChunkPointNumberThreshold()) {
                      isChunkEnoughLarge = false;
                    }
                    totalChunkPointNum =
                        totalChunkPointNum.add(BigInteger.valueOf(chunkMetadata.getNumOfPoints()));
                    if (chunkMetadata.getNumOfPoints() > maxChunkPointNum) {
                      maxChunkPointNum = chunkMetadata.getNumOfPoints();
                    }
                    if (chunkMetadata.getNumOfPoints() < minChunkPointNum) {
                      minChunkPointNum = chunkMetadata.getNumOfPoints();
                    }
                  }
                  totalChunkNum += chunkMetadatas.size();
                }
                logger.debug(
                    "{} [Compaction] compacting {}.{}, max chunk num is {},  min chunk num is {},"
                        + " average chunk num is {}, using {} compaction",
                    storageGroup,
                    device,
                    sensor,
                    maxChunkPointNum,
                    minChunkPointNum,
                    totalChunkPointNum.divide(BigInteger.valueOf(totalChunkNum)).longValue(),
                    isChunkEnoughLarge ? "flushing chunk" : isPageEnoughLarge ? "merge chunk" : "");

                //2.3.3.3.2 根据指定TsFile获取其对应.mods文件里的所有修改（删除）操作列表，并存入modificationCache里<TsFile文件名，修改操作列表>。判断该文件是否有存在对指定device+sensor序列存在删除修改操作
                if (isFileListHasModifications(//若有对该device+sensor序列存在删除操作，则把下面两个置为false，因为可能删掉数据后数据点数量就没有那么多了
                    readerChunkMetadataListMap.keySet(), modificationCache, device, sensor)) {
                  isPageEnoughLarge = false;
                  isChunkEnoughLarge = false;
                }

                //2.3.3.3.3 根据该sensor传感器在原先待合并的所有TsFile里的所有Chunk的数据点数量是否都大于系统预设Chunk或page数据点数量，使用下面三种方法的一种将所有待合并文件里该设备的该sensor的所有Chunk合并写入目标文件writer的输出缓存里
                // if a chunk is large enough, it's page must be large enough too
                if (isChunkEnoughLarge) {
                  logger.debug(
                      "{} [Compaction] {} chunk enough large, use append chunk merge",
                      storageGroup,
                      sensor);
                  // append page in chunks, so we do not have to deserialize a chunk
                  // 当该sensor传感器在原先待合并的所有TsFile里的所有Chunk的数据点数量都大于系统预设Chunk数据点数量，则直接往目标文件里依次追加写入该sensor的这些Chunk，写入的同时用限制器限流，并更新目标文件该设备的开始和结束时间
                  writeByAppendChunkMerge(
                      device,
                      compactionWriteRateLimiter,
                      sensorReaderChunkMetadataListEntry,
                      targetResource,
                      writer);
                } else if (isPageEnoughLarge) {
                  logger.debug(
                      "{} [Compaction] {} page enough large, use append page merge",
                      storageGroup,
                      sensor);
                  // append page in chunks, so we do not have to deserialize a chunk
                  // 当该sensor传感器在原先待合并的所有TsFile里存在一个以上Chunk的数据点数量小于系统预设Chunk数据点数量 但是 所有Chunk的数据点数量都大于系统预设page数据点数量，则把该sensor在不同待合并文件里的所有Chunk和ChunkMetadata合并到第一个Chunk和ChunkMetadata里，并用目标文件的writer将合并后的新Chunk写入到目标文件里
                  writeByAppendPageMerge(
                      device,
                      compactionWriteRateLimiter,
                      sensorReaderChunkMetadataListEntry,
                      targetResource,
                      writer);
                } else {
                  logger.debug(
                      "{} [Compaction] {} page too small, use deserialize page merge",
                      storageGroup,
                      sensor);
                  // we have to deserialize chunks to merge pages
                  //获取每个待合并文件的删除操作，根据每个文件对该序列的删除操作获取该sensor所有符合条件的数据点，然后创建目标文件对应该sensor的chunkWriterImpl，依次把所有数据点写入，最后刷到目标文件writer里的缓存里
                  writeByDeserializePageMerge(
                      device,
                      compactionWriteRateLimiter,
                      sensorReaderChunkMetadataListEntry,
                      targetResource,
                      writer,
                      modificationCache);
                }
              }
            }
          }
        }
        // 2.4 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
        writer.endChunkGroup();
      }

      for (TsFileResource tsFileResource : tsFileResources) {
        targetResource.updatePlanIndexes(tsFileResource);
      }
      // 将该TsFile文件对应的TsFileResource对象里的内容序列化写到本地的.resource文件里
      targetResource.serialize();
      // 在向该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写完数据区的内容并flush到本地后，再往该缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）
      writer.endFile();
      targetResource.close();

    } finally {
      for (TsFileSequenceReader reader : tsFileSequenceReaderMap.values()) {
        reader.close();
      }
      if (writer != null && writer.canWrite()) {
        writer.close();
      }
    }
  }

  //根据指定TsFileResource，创建其对应的TsFileSequenceReader对象放进tsFileSequenceReaderMap里<TsFile绝对路径，顺序读取器对象>,并返回指定TsFile的SequenceReader
  private static TsFileSequenceReader buildReaderFromTsFileResource(
      TsFileResource levelResource,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,
      String storageGroup) {
    return tsFileSequenceReaderMap.computeIfAbsent(
        levelResource.getTsFile().getAbsolutePath(),
        path -> {
          try {
            if (levelResource.getTsFile().exists()) {
              return new TsFileSequenceReader(path);
            } else {
              logger.info("{} tsfile does not exist", path);
              return null;
            }
          } catch (IOException e) {
            logger.error(
                "Storage group {}, flush recover meets error. reader create failed.",
                storageGroup,
                e);
            return null;
          }
        });
  }

  //获取该TsFile对该序列的所有删除操作，然后修改或删除chunkMetadata，具体操作是：若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)
  private static void modifyChunkMetaDataWithCache(
      TsFileSequenceReader reader,
      List<ChunkMetadata> chunkMetadataList,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath) {
    //获取该顺序阅读器对应TsFile的所有删除操作
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            reader.getFileName(),
            fileName ->
                new LinkedList<>(
                    new ModificationFile(fileName + ModificationFile.FILE_SUFFIX)
                        .getModifications()));
    List<Modification> seriesModifications = new LinkedList<>();
    //获取该TsFile文件里对该序列的所有删除操作
    for (Modification modification : modifications) {
      if (modification.getPath().matchFullPath(seriesPath)) {
        seriesModifications.add(modification);
      }
    }
    //根据给定的对该序列的删除操作列表和该序列的ChunkMetadata列表，若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)
    modifyChunkMetaData(chunkMetadataList, seriesModifications);
  }

  //根据指定TsFile获取其对应.mods文件里的所有修改（删除）操作列表，并存入modificationCache里<TsFile文件名，修改操作列表>。判断该文件是否有存在对指定device+sensor序列存在删除修改操作
  private static boolean isFileListHasModifications(
      Set<TsFileSequenceReader> readers,
      Map<String, List<Modification>> modificationCache,
      String device,
      String sensor)
      throws IllegalPathException {
    PartialPath path = new PartialPath(device, sensor);
    for (TsFileSequenceReader reader : readers) {
      //根据指定TsFile获取其对应.mods文件里的所有修改（删除）操作列表，并存入modificationCache里<TsFile文件名，修改操作列表>
      List<Modification> modifications = getModifications(reader, modificationCache);
      for (Modification modification : modifications) {
        if (modification.getPath().matchFullPath(path)) {
          return true;
        }
      }
    }
    return false;
  }

  //根据指定TsFile获取其对应.mods文件里的所有修改（删除）操作列表，存入modificationCache里<TsFile文件名，修改操作列表>
  private static List<Modification> getModifications(
      TsFileSequenceReader reader, Map<String, List<Modification>> modificationCache) {
    return modificationCache.computeIfAbsent(
        reader.getFileName(),
        fileName ->
            new LinkedList<>(
                new ModificationFile(fileName + ModificationFile.FILE_SUFFIX).getModifications()));
  }

  public static void deleteTsFilesInDisk(
      Collection<TsFileResource> mergeTsFiles, String storageGroupName) {
    logger.info("{} [compaction] merge starts to delete real file ", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteTsFile(mergeTsFile);
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  public static void deleteTsFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  public static ICrossSpaceMergeFileSelector
      getCrossSpaceFileSelector( // 根据系统预设的合并策略，创建获取跨空间合并的文件选择器
      long budget, CrossSpaceMergeResource resource) {
    MergeFileStrategy strategy = IoTDBDescriptor.getInstance().getConfig().getMergeFileStrategy();
    switch (strategy) {
      case MAX_FILE_NUM:
        return new MaxFileMergeFileSelector(resource, budget);
      case MAX_SERIES_NUM:
        return new MaxSeriesMergeFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown CrossSpaceFileStrategy " + strategy);
    }
  }

  public static File[] findInnerSpaceCompactionLogs(String directory) {
    File timePartitionDir = new File(directory);
    if (timePartitionDir.exists()) {
      return timePartitionDir.listFiles(
          (dir, name) -> name.endsWith(SizeTieredCompactionLogger.COMPACTION_LOG_NAME));
    } else {
      return new File[0];
    }
  }
}
