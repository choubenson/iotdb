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
          newChunk.mergeChunk(chunk);
          newChunkMetadata.mergeChunkMetadata(chunkMetadata);
        }
      }
    }
    return new Pair<>(newChunkMetadata, newChunk);
  }

  private static void readByDeserializePageMerge(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap,
      Map<Long, TimeValuePair> timeValuePairMap,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath)
      throws IOException {
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      modifyChunkMetaDataWithCache(reader, chunkMetadataList, modificationCache, seriesPath);
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        IChunkReader chunkReader = new ChunkReaderByTimestamp(reader.readMemChunk(chunkMetadata));
        while (chunkReader.hasNextSatisfiedPage()) {
          IPointReader batchIterator = chunkReader.nextPageData().getBatchDataIterator();
          while (batchIterator.hasNextTimeValuePair()) {
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

  public static void writeByAppendPageMerge(
      String device,
      RateLimiter compactionWriteRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry, //某一个measurementId对应的在不同文件的<TsFileSequenceReader,chunkmetadataList>
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer)
      throws IOException {
    Pair<ChunkMetadata, Chunk> chunkPair = readByAppendPageMerge(entry.getValue());
    ChunkMetadata newChunkMetadata = chunkPair.left;
    Chunk newChunk = chunkPair.right;
    if (newChunkMetadata != null && newChunk != null) {
      // wait for limit write
      MergeManager.mergeRateLimiterAcquire(
          compactionWriteRateLimiter,
          (long) newChunk.getHeader().getDataSize() + newChunk.getData().position());
      writer.writeChunk(newChunk, newChunkMetadata);
      targetResource.updateStartTime(device, newChunkMetadata.getStartTime());
      targetResource.updateEndTime(device, newChunkMetadata.getEndTime());
    }
  }

  public static void writeByDeserializePageMerge(
      String device,
      RateLimiter compactionRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry,
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer,
      Map<String, List<Modification>> modificationCache)
      throws IOException, IllegalPathException {
    Map<Long, TimeValuePair> timeValuePairMap = new TreeMap<>();
    Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap = entry.getValue();
    readByDeserializePageMerge(
        readerChunkMetadataMap,
        timeValuePairMap,
        modificationCache,
        new PartialPath(device, entry.getKey()));
    boolean isChunkMetadataEmpty = true;
    for (List<ChunkMetadata> chunkMetadataList : readerChunkMetadataMap.values()) {
      if (!chunkMetadataList.isEmpty()) {
        isChunkMetadataEmpty = false;
        break;
      }
    }
    if (isChunkMetadataEmpty) {
      return;
    }
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
    for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
      writeTVPair(timeValuePair, chunkWriter);
      targetResource.updateStartTime(device, timeValuePair.getTimestamp());
      targetResource.updateEndTime(device, timeValuePair.getTimestamp());
    }
    // wait for limit write
    MergeManager.mergeRateLimiterAcquire(
        compactionRateLimiter, chunkWriter.estimateMaxSeriesMemSize());
    chunkWriter.writeToFileWriter(writer);
  }

  //对subLevelResources列表里每个TsFile初始化其对应的顺序读取器放入tsFileSequenceReaderMap对象（<TsFile绝对路径，顺序读取器对象>）里，并获取并返回所有TsFile包含的所有设备ID
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
      TsFileResource targetResource,
      List<TsFileResource> tsFileResources,
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
      Map<String, List<Modification>> modificationCache = new HashMap<>();
      RateLimiter compactionWriteRateLimiter = // 获取合并写入的流量限制器
          MergeManager.getINSTANCE().getMergeWriteRateLimiter();
      //对subLevelResources列表里每个TsFile初始化其对应的顺序读取器放入tsFileSequenceReaderMap对象（<TsFile绝对路径，顺序读取器对象>）里，并获取并返回所有TsFile包含的所有设备ID
      Set<String> tsFileDevicesMap =
          getTsFileDevicesSet(tsFileResources, tsFileSequenceReaderMap, storageGroup);

      //遍历所有待合并的TsFile的所有设备ID
      for (String device : tsFileDevicesMap) {
        writer.startChunkGroup(device);
        // tsfile -> measurement -> List<ChunkMetadata>
        //该设备ID下待被合并的Chunk，存放每个待合并文件的顺序读取器，以及每个待合并文件在该设备ID下的传感器ID和对应的ChunkMetadata列表
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
        while (hasNextChunkMetadataList(chunkMetadataListIteratorCache.values())) {
          String lastSensor = null;
          Set<String> allSensors = new HashSet<>();
          //遍历每个待合并文件的顺序读取器，以及每个待合并文件在该设备ID下的传感器ID和对应的ChunkMetadata列表
          for (Entry<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
              chunkMetadataListCacheForMergeEntry : chunkMetadataListCacheForMerge.entrySet()) {
            //该待被合并TsFile的顺序读取器
            TsFileSequenceReader reader = chunkMetadataListCacheForMergeEntry.getKey();
            //该文件的该设备下的所有传感器ID和各自对应的ChunkMetadataList
            Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =
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
          // if there is no more chunkMetaData, merge all the sensors
          if (!hasNextChunkMetadataList(chunkMetadataListIteratorCache.values())) {
            lastSensor = Collections.max(allSensors);
          }

          //遍历该设备下所有的设备传感器ID
          for (String sensor : allSensors) {
            if (sensor.compareTo(lastSensor) <= 0) {
              //存放每个TsFile顺序读取器和对应待被合并的此sensor传感器的ChunkMetadata列表
              Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataListMap =
                  new TreeMap<>(
                      (o1, o2) ->
                          TsFileManager.compareFileName(
                              new File(o1.getFileName()), new File(o2.getFileName())));
              // find all chunkMetadata of a sensor
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
              //<measurementID，<TsFileSequenceReader,ChunkMetadataList>>
              Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>>
                  sensorReaderChunkMetadataListEntry =
                      new DefaultMapEntry<>(sensor, readerChunkMetadataListMap);
              //若是乱序空间内合并
              if (!sequence) {
                writeByDeserializePageMerge(
                    device,
                    compactionWriteRateLimiter,
                    sensorReaderChunkMetadataListEntry,
                    targetResource,
                    writer,
                    modificationCache);
              } else { //顺序空间内合并
                //那些待被合并TsFile的该sensor下的
                boolean isChunkEnoughLarge = true;
                boolean isPageEnoughLarge = true;
                BigInteger totalChunkPointNum = new BigInteger("0");  //该sensor的总数据点数量
                long totalChunkNum = 0; //所有文件下该sensor的Chunk总数量
                long maxChunkPointNum = Long.MIN_VALUE; //某Chunk最大数据点数量
                long minChunkPointNum = Long.MAX_VALUE; //某Chunk最小数据点数量

                //判断每个待被合并文件里的该sensor的每个Chunk里数据点数量是否超过系统预设的Chunk或者page数据点数量，分别用isChunkEnoughLarge和isPageEnoughLarge标记
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

                //根据指定TsFile获取其对应.mods文件里的所有修改（删除）操作列表，并存入modificationCache里<TsFile文件名，修改操作列表>。判断该文件是否有存在对指定device+sensor序列存在删除修改操作
                if (isFileListHasModifications(//若有对该device+sensor序列存在删除操作，则把下面两个置为false，因为可能删掉数据后数据点数量就没有那么多了
                    readerChunkMetadataListMap.keySet(), modificationCache, device, sensor)) {
                  isPageEnoughLarge = false;
                  isChunkEnoughLarge = false;
                }

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
        writer.endChunkGroup();
      }

      for (TsFileResource tsFileResource : tsFileResources) {
        targetResource.updatePlanIndexes(tsFileResource);
      }
      targetResource.serialize();
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

  private static void modifyChunkMetaDataWithCache(
      TsFileSequenceReader reader,
      List<ChunkMetadata> chunkMetadataList,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath) {
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            reader.getFileName(),
            fileName ->
                new LinkedList<>(
                    new ModificationFile(fileName + ModificationFile.FILE_SUFFIX)
                        .getModifications()));
    List<Modification> seriesModifications = new LinkedList<>();
    for (Modification modification : modifications) {
      if (modification.getPath().matchFullPath(seriesPath)) {
        seriesModifications.add(modification);
      }
    }
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
