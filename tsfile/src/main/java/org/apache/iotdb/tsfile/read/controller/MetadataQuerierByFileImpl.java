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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader.LocateStatus;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class MetadataQuerierByFileImpl implements IMetadataQuerier { // 某个文件的元数据查询器

  // number of cache entries (path -> List<ChunkMetadata>)
  private static final int CACHED_ENTRY_NUMBER = 1000;

  private TsFileMetadata fileMetaData;  //TsFileMetadata对象，即该Tsfile的IndexOfTimeseriesIndex对象

  private LRUCache<Path, List<ChunkMetadata>> chunkMetaDataCache; //缓存，存放了该TsFile里每个时间序列对应的ChunkIndex集合，这样后续就可以直接从缓存里读取该时间序列的Chunk，而非从本地文件读，加快了读取速度

  private TsFileSequenceReader tsFileReader;  //该文件的顺序读取器

  /** Constructor of MetadataQuerierByFileImpl. */
  public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
    this.tsFileReader = tsFileReader;
    this.fileMetaData = tsFileReader.readFileMetadata();
    chunkMetaDataCache =
        new LRUCache<Path, List<ChunkMetadata>>(CACHED_ENTRY_NUMBER) {
          @Override
          public List<ChunkMetadata> loadObjectByKey(Path key) throws IOException {
            return loadChunkMetadata(key);
          }
        };
  }

  @Override
  public List<IChunkMetadata> getChunkMetaDataList(Path path) throws IOException {  //从chunkMetaDataCache缓存里获取该时间序列路径对应的ChunkIndex列表
    return new ArrayList<>(chunkMetaDataCache.get(path));
  }

  @Override
  public Map<Path, List<IChunkMetadata>> getChunkMetaDataMap(List<Path> paths) throws IOException {
    Map<Path, List<IChunkMetadata>> chunkMetaDatas = new HashMap<>();
    for (Path path : paths) {
      if (!chunkMetaDatas.containsKey(path)) {
        chunkMetaDatas.put(path, new ArrayList<>());
      }
      chunkMetaDatas.get(path).addAll(getChunkMetaDataList(path));
    }
    return chunkMetaDatas;
  }

  @Override
  public TsFileMetadata getWholeFileMetadata() {//获取该文件的TsFileMetadata对象，即IndexOfTimeseriesIndex索引内容
    return fileMetaData;
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void loadChunkMetaDatas(List<Path> paths) throws IOException {//将给定的时间序列路径列表获取其各自对应在该TsFile里的所有ChunkIndex放入chunkMetaDataCache缓存里。具体做法是：1. 首先将整理每个DeviceID对应有哪些MeasurementId  2.遍历每个设备ID和对应的传感器集合：（1）获得对应的TimeseriesIndex列表（2）对每个TimeseriesIndex获取其所有的ChunkIndex依次放入一个列表里（3）遍历所有的ChunkIndex列表，把属于该次遍历的传感器的ChunkIndex对象加入对应时间序列的缓存变量里
    // group measurements by device
    TreeMap<String, Set<String>> deviceMeasurementsMap = new TreeMap<>();//根据传来的时间序列路径列表，存放包含了哪些设备，以及每个设备对应了哪些传感器
    for (Path path : paths) { //遍历传来的时间序列路径,整理每个DeviceID对应有哪些MeasureId
      if (!deviceMeasurementsMap.containsKey(path.getDevice())) { //若deviceMeasurementsMap里不包含此设备，则新建一个
        deviceMeasurementsMap.put(path.getDevice(), new HashSet<>());
      }
      deviceMeasurementsMap.get(path.getDevice()).add(path.getMeasurement());//往deviceMeasurementsMap里对应设备的传感器集合里加入此传感器ID
    }

    Map<Path, List<ChunkMetadata>> tempChunkMetaDatas = new HashMap<>();//存放了每个时间序列对应的ChunkIndex集合

    int count = 0;//加入tempChunkMetaDatas里的ChunkIndex总数量，包括不同时间序列的
    boolean enough = false;

    for (Map.Entry<String, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {  //遍历路径里的每个设备和对应的传感器集合
      if (enough) {
        break;
      }
      String selectedDevice = deviceMeasurements.getKey();  //当前遍历的设备ID
      // s1, s2, s3
      Set<String> selectedMeasurements = deviceMeasurements.getValue(); //当前遍历设备对应的传感器ID集合
      List<String> devices = this.tsFileReader.getAllDevices();//获取该TsFile文件里的所有设备ID，放入list里并返回。具体做法是使用该TsFile的IndexOfTimeseriesIndex的第一个根索引节点进行递归查找其下的所有LEAF_DEVICE子节点，从而获取该根节点下的所有设备ID
      String[] deviceNames = devices.toArray(new String[0]);//将devices列表转为String数组来存储
      if (Arrays.binarySearch(deviceNames, selectedDevice) < 0) {//使用二分搜索法对设备数组进行搜索，查看是否存在当前遍历的设备ID，若为负数则不存在
        continue;
      }

      List<TimeseriesMetadata> timeseriesMetaDataList =
          tsFileReader.readTimeseriesMetadata(selectedDevice, selectedMeasurements);//根据当前遍历的设备ID和对应的传感器ID,获取该TsFile里对应的TimeseriesIndex对象列表
      List<ChunkMetadata> chunkMetadataList = new ArrayList<>();//依次存放了当前遍历的设备ID和传感器ID对应的所有TimeseriesIndex对象里的所有ChunkIndex对象
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetaDataList) {//遍历每个TimeseriesIndex对象
        chunkMetadataList.addAll(tsFileReader.readChunkMetaDataList(timeseriesMetadata));//从指定的TimeseriesIndex对象里获取对应的所有ChunkIndex，存入列表里
      }
      // d1
      for (ChunkMetadata chunkMetaData : chunkMetadataList) { //遍历所有TimeseriesIndex的所有chunkIndex对象
        String currentMeasurement = chunkMetaData.getMeasurementUid(); //获取此ChunkIndex对象对应的measurementId

        // s1
        if (selectedMeasurements.contains(currentMeasurement)) {//若当前遍历的设备ID对应的传感器列表里包含此ChunkIndex对应的measureId，则

          // d1.s1
          Path path = new Path(selectedDevice, currentMeasurement);//构造完整的时间序列路径

          // add into tempChunkMetaDatas
          if (!tempChunkMetaDatas.containsKey(path)) {
            tempChunkMetaDatas.put(path, new ArrayList<>());
          }
          tempChunkMetaDatas.get(path).add(chunkMetaData);//往tempChunkMetaDatas里该时间序列的ChunkIndex集合里接入该ChunkIndex

          // check cache size, stop when reading enough
          count++;  //加入的ChunkIndex数量
          if (count == CACHED_ENTRY_NUMBER) { //当加入的总数到达临界值，则停止加入
            enough = true;
            break;
          }
        }
      }
    }

    for (Map.Entry<Path, List<ChunkMetadata>> entry : tempChunkMetaDatas.entrySet()) {  //把tempChunkMetaDatas里的内容放入chunkMetaDataCache变量缓存里
      chunkMetaDataCache.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public TSDataType getDataType(Path path) throws IOException { //获取该时间序列对应的数据类型，若该TsFile不存在此时间序列则返回null,具体做法是获取其TimeseriesIndex对象里的所有ChunkIndex列表，然后拿第一个ChunkIndex获取其数据类型
    if (tsFileReader.getChunkMetadataList(path) == null//根据给定的时间序列path，获取其TimeseriesIndex对象里的所有ChunkIndex，并按照每个ChunkIndex的开始时间戳从小到大进行排序并返回
        || tsFileReader.getChunkMetadataList(path).isEmpty()) { //若该时间序列的ChunkIndex列表为空，则返回null
      return null;
    }
    return tsFileReader.getChunkMetadataList(path).get(0).getDataType();//
  }

  private List<ChunkMetadata> loadChunkMetadata(Path path) throws IOException {
    return tsFileReader.getChunkMetadataList(path);
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public List<TimeRange> convertSpace2TimePartition(
      List<Path> paths, long spacePartitionStartPos, long spacePartitionEndPos) throws IOException {
    if (spacePartitionStartPos > spacePartitionEndPos) {
      throw new IllegalArgumentException(
          "'spacePartitionStartPos' should not be larger than 'spacePartitionEndPos'.");
    }

    // (1) get timeRangesInCandidates and timeRangesBeforeCandidates by iterating
    // through the metadata
    ArrayList<TimeRange> timeRangesInCandidates = new ArrayList<>();
    ArrayList<TimeRange> timeRangesBeforeCandidates = new ArrayList<>();

    // group measurements by device

    TreeMap<String, Set<String>> deviceMeasurementsMap = new TreeMap<>();
    for (Path path : paths) {
      deviceMeasurementsMap
          .computeIfAbsent(path.getDevice(), key -> new HashSet<>())
          .add(path.getMeasurement());
    }
    for (Map.Entry<String, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {
      String selectedDevice = deviceMeasurements.getKey();
      Set<String> selectedMeasurements = deviceMeasurements.getValue();

      // measurement -> ChunkMetadata list
      Map<String, List<ChunkMetadata>> seriesMetadatas =
          tsFileReader.readChunkMetadataInDevice(selectedDevice);

      for (Entry<String, List<ChunkMetadata>> seriesMetadata : seriesMetadatas.entrySet()) {

        if (!selectedMeasurements.contains(seriesMetadata.getKey())) {
          continue;
        }

        for (IChunkMetadata chunkMetadata : seriesMetadata.getValue()) {
          LocateStatus location =
              checkLocateStatus(chunkMetadata, spacePartitionStartPos, spacePartitionEndPos);
          if (location == LocateStatus.after) {
            break;
          }

          if (location == LocateStatus.in) {
            timeRangesInCandidates.add(
                new TimeRange(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          } else {
            timeRangesBeforeCandidates.add(
                new TimeRange(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          }
        }
      }
    }

    // (2) sort and merge the timeRangesInCandidates
    ArrayList<TimeRange> timeRangesIn =
        new ArrayList<>(TimeRange.sortAndMerge(timeRangesInCandidates));
    if (timeRangesIn.isEmpty()) {
      return Collections.emptyList(); // return an empty list
    }

    // (3) sort and merge the timeRangesBeforeCandidates
    ArrayList<TimeRange> timeRangesBefore =
        new ArrayList<>(TimeRange.sortAndMerge(timeRangesBeforeCandidates));

    // (4) calculate the remaining time ranges
    List<TimeRange> resTimeRanges = new ArrayList<>();
    for (TimeRange in : timeRangesIn) {
      ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(timeRangesBefore));
      resTimeRanges.addAll(remains);
    }

    return resTimeRanges;
  }

  /**
   * Check the location of a given chunkGroupMetaData with respect to a space partition constraint.
   *
   * @param chunkMetaData the given chunkMetaData
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos the end position of the space partition
   * @return LocateStatus
   */
  public static LocateStatus checkLocateStatus(
      IChunkMetadata chunkMetaData, long spacePartitionStartPos, long spacePartitionEndPos) {
    long startOffsetOfChunk = chunkMetaData.getOffsetOfChunkHeader();
    if (spacePartitionStartPos <= startOffsetOfChunk && startOffsetOfChunk < spacePartitionEndPos) {
      return LocateStatus.in;
    } else if (startOffsetOfChunk < spacePartitionStartPos) {
      return LocateStatus.before;
    } else {
      return LocateStatus.after;
    }
  }

  @Override
  public void clear() {
    chunkMetaDataCache.clear();
  }
}
