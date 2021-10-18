/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TsFileSequenceReader implements AutoCloseable { // TsFile文件的顺序阅读器

  private static final Logger logger = LoggerFactory.getLogger(TsFileSequenceReader.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final String METADATA_INDEX_NODE_DESERIALIZE_ERROR =
      "Something error happened while deserializing MetadataIndexNode of file {}";
  protected String file;  //Todo:bug 此处是文件的绝对路径，可是若其文件夹层数小于4后面运行会报错
  protected TsFileInput tsFileInput;  //该TsFile对应的TsFileInput对象，用来执行底层真正地从本地文件读取内容并反序列化到对象的操作
  protected long fileMetadataPos; //该TsFile的IndexOfTimeseriesIndex索引的开始处所在的偏移位置
  protected int fileMetadataSize; //该TsFile的IndexOfTimeseriesIndex索引的字节大小
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  protected TsFileMetadata tsFileMetaData;  //该TsFile的IndexOfTimeseriesIndex索引
  // device -> measurement -> TimeseriesMetadata
  private Map<String, Map<String, TimeseriesMetadata>>
      cachedDeviceMetadata = // 存放某设备的某传感器的TimeseriesIndex
      new ConcurrentHashMap<>();
  private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private boolean cacheDeviceMetadata;
  private long minPlanIndex = Long.MAX_VALUE;
  private long maxPlanIndex = Long.MIN_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileSequenceReader(String file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is opened. {}", file, getClass().getName());
    }
    this.file = file;
    tsFileInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(file);
    try {
      if (loadMetadataSize) {
        loadMetadataSize();//加载初始化该TsFile顺序阅读器的IndexOfTimeseriesIndex的大小和开始偏移位置
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  // used in merge resource
  public TsFileSequenceReader(String file, boolean loadMetadata, boolean cacheDeviceMetadata)
      throws IOException {
    this(file, loadMetadata);
    this.cacheDeviceMetadata = cacheDeviceMetadata;
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param input given input
   */
  public TsFileSequenceReader(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReader(TsFileInput input, boolean loadMetadataSize) throws IOException {
    this.tsFileInput = input;
    try {
      if (loadMetadataSize) { // NOTE no autoRepair here
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input the input of a tsfile. The current position should be a marker and then a chunk
   *     Header, rather than the magic number
   * @param fileMetadataPos the position of the file metadata in the TsFileInput from the beginning
   *     of the input to the current position
   * @param fileMetadataSize the byte size of the file metadata in the input
   */
  public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    this.tsFileInput = input;
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  public void loadMetadataSize() throws IOException {//加载初始化该TsFile顺序阅读器的IndexOfTimeseriesIndex的大小和开始偏移位置
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES); //该缓存存放该TsFile的IndexOfTimeseriesIndex索引的大小
    if (readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {  //若TsFile文件尾部的字符串为MagicString
      tsFileInput.read(//从文件的metadataSize处位置开始，往metadataSize缓存里读取该TsFile的内容，返回读取的字节数
          metadataSize,
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES);
      metadataSize.flip();
      // read file metadata size and position
      fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);//从缓存里读取int型数据
      fileMetadataPos =
          tsFileInput.size()
              - TSFileConfig.MAGIC_STRING.getBytes().length
              - Integer.BYTES
              - fileMetadataSize;
    }
  }

  public long getFileMetadataPos() {
    return fileMetadataPos;
  }

  public int getFileMetadataSize() {
    return fileMetadataSize;
  }

  /** this function does not modify the position of the file reader. */
  public String readTailMagic() throws IOException {//使用TsFileInput对象读取该TsFile尾部的魔法字符串
    long totalSize = tsFileInput.size();//该TsFile的文件大小
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /** whether the file is a complete TsFile: only if the head magic and tail magic string exists. */
  public boolean isComplete() throws IOException {
    long size = tsFileInput.size();
    // TSFileConfig.MAGIC_STRING.getBytes().length * 2 for two magic string
    // Byte.BYTES for the file version number
    if (size >= TSFileConfig.MAGIC_STRING.getBytes().length * 2 + Byte.BYTES) {
      String tailMagic = readTailMagic();
      String headMagic = readHeadMagic();
      return tailMagic.equals(headMagic);
    } else {
      return false;
    }
  }

  /** this function does not modify the position of the file reader. */
  public String readHeadMagic() throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, 0);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /** this function reads version number and checks compatibility of TsFile. */
  public byte readVersionNumber() throws IOException {
    ByteBuffer versionNumberByte = ByteBuffer.allocate(Byte.BYTES);
    tsFileInput.read(versionNumberByte, TSFileConfig.MAGIC_STRING.getBytes().length);
    versionNumberByte.flip();
    return versionNumberByte.get();
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public TsFileMetadata readFileMetadata() throws IOException {//若当前顺序阅读器的tsFileMetaData为null，则使用tsFileInput对象读取对应TsFile里的IndexOfTimeseriesIndex索引的所有内容读到bytebuffer缓存里，并从buffer里进行读取反序列化成该顺序读取器里的TsFileMetadata对象
    try {
      if (tsFileMetaData == null) {
        tsFileMetaData =
            TsFileMetadata.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));//此处其实是把该TsFile的IndexOfTimeseriesIndex索引的所有内容读到bytebuffer缓存里，并从buffer里进行读取反序列化成TsFileMetadata对象并返回//使用tsFileInput对象读取对应TsFile里从position位置开始，共计size个字节的数据到ByteBuffer缓存里.
      }
    } catch (BufferOverflowException e) {
      logger.error("Something error happened while reading file metadata of file {}", file);
      throw e;
    }
    return tsFileMetaData;
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public BloomFilter readBloomFilter() throws IOException {
    readFileMetadata();
    return tsFileMetaData.getBloomFilter();
  }

  /**
   * this function reads measurements and TimeseriesMetaDatas in given device Thread Safe
   *
   * @param device name
   * @return the map measurementId -> TimeseriesMetaData in one device
   * @throws IOException io error
   */
  public Map<String, TimeseriesMetadata> readDeviceMetadata(String device) throws IOException {
    if (!cacheDeviceMetadata) {
      return readDeviceMetadataFromDisk(device);
    }

    cacheLock.readLock().lock();
    try {
      if (cachedDeviceMetadata.containsKey(device)) {
        return cachedDeviceMetadata.get(device);
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    cacheLock.writeLock().lock();
    try {
      if (cachedDeviceMetadata.containsKey(device)) {
        return cachedDeviceMetadata.get(device);
      }
      readFileMetadata();
      Map<String, TimeseriesMetadata> deviceMetadata = readDeviceMetadataFromDisk(device);
      cachedDeviceMetadata.put(device, deviceMetadata);
      return deviceMetadata;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private Map<String, TimeseriesMetadata> readDeviceMetadataFromDisk(String device)
      throws IOException {
    readFileMetadata();
    List<TimeseriesMetadata> timeseriesMetadataList =
        getDeviceTimeseriesMetadataWithoutChunkMetadata(device);
    Map<String, TimeseriesMetadata> deviceMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      deviceMetadata.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }
    return deviceMetadata;
  }

  public TimeseriesMetadata readTimeseriesMetadata(Path path, boolean ignoreNotExists)
      throws IOException {//根据时间序列路径path，获取该时间序列在该TsFile里的TimeseriesIndex索引对象。具体做法是：（1）先获取该序列对应设备的索引条目指向的传感器节点内容（2）根据序列的传感器ID获取传感器条目指向的TimeseriesIndex节点（该节点包含了目标path序列的TimeseriesIndex索引）内容（3）将读取到的TimeseriesIndex节点内容buffer里的每个TimeseriesIndex依次反序列化放进临时列表里，最后获取目标path序列的TimeseriesIndex索引并返回
    readFileMetadata();//若当前顺序阅读器的tsFileMetaData为null，则使用tsFileInput对象读取对应TsFile里的IndexOfTimeseriesIndex索引的所有内容读到bytebuffer缓存里，并从buffer里进行读取反序列化成该顺序读取器里的TsFileMetadata对象
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();//获取该TsFile的IndexOfTimeseriesIndex索引的第一个节点对象
    Pair<MetadataIndexEntry, Long> metadataIndexPair =//从第一个根节点对象里根据时间序列的设备ID查找对应的目标索引条目和该条目指向的子节点的结束位置（因此若是中间节点且不存在目标条目，则要递归到目标条目所在的叶子节点去查找）：1. 若找到了则返回<名为name的索引条目对象，该条目指向子节点的结束偏移位置> 2. 若没找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {  //若在IndexOfTimeseriesIndex的索引节点里没有找到该时间序列对应设备的索引条目，则
      if (ignoreNotExists) {//若ignoreNotExists为真，则返回null,否则抛异常
        return null;
      }
      throw new IOException("Device {" + path.getDevice() + "} is not in tsFileMetaData");
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);//从TsFile里的IndexOfTimeseriesIndex索引里读取该时间序列对应设备的索引条目指向的子节点的内容到二进制缓存
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;//该TsFile的IndexOfTimeseriesIndex索引的第一个节点对象
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {//若第一个节点对象类型不是LEAF_MEASUREMENT
      try {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);//将buffer缓存反序列化成节点对象，此时是该时间路径对应设备的索引条目指向的传感器节点（可能是中间或者叶子节点），该传感器节点里的所有传感器都是属于该设备的
      } catch (BufferOverflowException e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =//从传感器节点对象里根据时间序列对应的传感器名称查找对应的目标索引条目和该条目指向的子节点（此时应该是TimeseriesIndex节点了）的结束位置（此处必须用模糊查找，找到待查找的时间序列的TimeseriesIndex对应的索引条目所在的LEAF_MEASUREMENT节点。）。（因此若是中间节点且不存在目标条目，则要递归到目标条目所在的叶子节点去查找）：1. 若找到了则返回<名为name的索引条目对象，该条目指向子节点的结束偏移位置>> 2. 若没找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    if (metadataIndexPair == null) {//TOdo:bug?因为exactSearch是false，所以不可能为null
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right); //读取该时间序列对应在该TsFile里的TimeseriesIndex索引所在的节点的所有内容到二进制缓存，也就是说此时该buffer里有多条TimeseriesIndex索引
    while (buffer.hasRemaining()) { //当buffer缓存里还有没读完、可用的数据时
      try {
        timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));//该buffer里可能包含了多个TimeseriesIndex的内容。从buffer里反序列化一个TimeseriesIndex对象并返回。（若needChunkMetadata为true，则还要一一反序列化该TimeseriesIndex的所有ChunkIndex，若为false则无需反序列化，直接将buffer指针后移到最后一个ChunkIndex的结尾处）。把该TimeseriesIndex加入此顺序阅读器里的timeseriesMetadataList列表
      } catch (BufferOverflowException e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
    }
    // return null if path does not exist in the TsFile
    int searchResult =
        binarySearchInTimeseriesMetadataList(timeseriesMetadataList, path.getMeasurement());//使用二分搜索法查找传感器ID为key的时间序列索引TimeseriesIndex是在timeseriesMetadataList里的第几个，若没有则返回-1
    return searchResult >= 0 ? timeseriesMetadataList.get(searchResult) : null; //若找到，则从timeseriesMetadataList里返回该时间序列path对应的TimeseriesIndex对象
  }

  /**
   * Find the leaf node that contains this vector, return all the needed subSensor and time column
   *
   * @param path path with time column
   * @param subSensorList value columns that needed
   * @return TimeseriesMetadata for the time column and all the needed subSensor, the order of the
   *     element in this list should be the same as subSensorList
   */
  public List<TimeseriesMetadata> readTimeseriesMetadata(Path path, List<String> subSensorList)
      throws IOException {
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getLeafMetadataIndexPair(path);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    Map<String, TimeseriesMetadata> timeseriesMetadataMap = new HashMap<>();
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata;
      try {
        timeseriesMetadata = TimeseriesMetadata.deserializeFrom(buffer, true);
      } catch (BufferOverflowException e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
      timeseriesMetadataMap.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }

    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    for (String subSensor : subSensorList) {
      timeseriesMetadataList.add(timeseriesMetadataMap.get(subSensor));
    }
    return timeseriesMetadataList;
  }

  /* Find the leaf node that contains path, return all the sensors in that leaf node which are also in allSensors set */
  public List<TimeseriesMetadata> readTimeseriesMetadata(Path path, Set<String> allSensors)
      throws IOException {
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getLeafMetadataIndexPair(path);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();

    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata;
      try {
        timeseriesMetadata = TimeseriesMetadata.deserializeFrom(buffer, true);
      } catch (BufferOverflowException e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
      if (allSensors.contains(timeseriesMetadata.getMeasurementId())) {
        timeseriesMetadataList.add(timeseriesMetadata);
      }
    }
    return timeseriesMetadataList;
  }

  /* Get leaf MetadataIndexPair which contains path */
  private Pair<MetadataIndexEntry, Long> getLeafMetadataIndexPair(Path path) throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      return null;
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      } catch (BufferOverflowException e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    return metadataIndexPair;
  }

  public List<TimeseriesMetadata> readTimeseriesMetadata(String device, Set<String> measurements)//根据指定的设备ID和对应的传感器ID,获取该TsFile里对应的TimeseriesIndex对象
      throws IOException {
    readFileMetadata();//若当前顺序阅读器的tsFileMetaData为null，则使用tsFileInput对象读取对应TsFile里的IndexOfTimeseriesIndex索引的所有内容读到bytebuffer缓存里，并从buffer里进行读取反序列化成该顺序读取器里的TsFileMetadata对象
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();//获取IndexOfTimeseriesIndex索引的第一个索引节点对象
    Pair<MetadataIndexEntry, Long> metadataIndexPair =//从指定的MetadataIndexNode节点对象里根据name查找对应的目标索引条目和它所指向的孩子节点的结束偏移位置（因此若是中间节点且不存在目标条目，则要递归到目标条目所在的叶子节点去查找）：1. 若找到了，则把该条目对应条目对象和该条目指向的子节点的结束位置放入pair对象里，即<名为key的索引条目对象，该条目指向的子节点的结束偏移位置> 2. 若没有找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
        getMetadataAndEndOffset(deviceMetadataIndexNode, device, true, false);//Todo:此处必须是要精确查询？！因为最后设备叶子节点一定会存在具体的该设备的条目
    if (metadataIndexPair == null) {  //Todo:bug?由于精度查询为false，因此可以模糊查询，不应该出现null
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> resultTimeseriesMetadataList = new ArrayList<>();
    List<String> measurementList = new ArrayList<>(measurements);//初始化传感器列表
    Set<String> measurementsHadFound = new HashSet<>();
    for (int i = 0; i < measurementList.size(); i++) {//遍历每个传感器
      if (measurementsHadFound.contains(measurementList.get(i))) {
        continue;
      }
      ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);//将名为device变量的目标条目指向的子节点内容读取到buffer缓存，此处一定是属于该设备的传感器节点（可能中间或叶子）
      Pair<MetadataIndexEntry, Long> measurementMetadataIndexPair = metadataIndexPair;
      List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
      MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;//该文件的IndexOfTimeseriesIndex索引的第一个索引节点对象
      if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {//若不是叶子传感器节点，则
        try {
          metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);//将device变量的目标条目指向的子节点内容反序列化，即属于该设备的传感器节点
        } catch (BufferOverflowException e) {
          logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
          throw e;
        }
        measurementMetadataIndexPair =//获取属于该设备的该传感器的索引条目及其指向的子节点的结束位置。必须要用模糊查询，因为最后传感器叶子节点可能不包含具体的该传感器的条目，此时指向对应的TimeseriesIndex节点，它可能包含了多个时间序列的TimeseriesIndex
            getMetadataAndEndOffset(metadataIndexNode, measurementList.get(i), false, false);
      }
      if (measurementMetadataIndexPair == null) {
        return Collections.emptyList();
      }
      buffer =  //此时是包含该传感器的TimeseriesIndex节点的内容，它可能包含了多个时间序列的TimeseriesIndex
          readData(
              measurementMetadataIndexPair.left.getOffset(), measurementMetadataIndexPair.right);
      while (buffer.hasRemaining()) {   //下面开始一次从buffer里读取一个个TimeseriesIndex，把所有读出来加入到临时列表里
        try {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
        } catch (BufferOverflowException e) {
          logger.error(
              "Something error happened while deserializing TimeseriesMetadata of file {}", file);
          throw e;
        }
      }
      for (int j = i; j < measurementList.size(); j++) {//从中获得想要的传感器的TimeseriesIndex并返回即可
        String current = measurementList.get(j);
        if (!measurementsHadFound.contains(current)) {
          int searchResult = binarySearchInTimeseriesMetadataList(timeseriesMetadataList, current);
          if (searchResult >= 0) {
            resultTimeseriesMetadataList.add(timeseriesMetadataList.get(searchResult));
            measurementsHadFound.add(current);
          }
        }
        if (measurementsHadFound.size() == measurements.size()) {
          return resultTimeseriesMetadataList;
        }
      }
    }
    return resultTimeseriesMetadataList;
  }

  protected int binarySearchInTimeseriesMetadataList( //使用二分搜索法查找传感器ID为key的时间序列索引TimeseriesIndex是在timeseriesMetadataList里的第几个，若没有则返回-1
      List<TimeseriesMetadata> timeseriesMetadataList, String key) {
    int low = 0;
    int high = timeseriesMetadataList.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      TimeseriesMetadata midVal = timeseriesMetadataList.get(mid);
      int cmp = midVal.getMeasurementId().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -1; // key not found
  }

  public List<String> getAllDevices() throws IOException {//获取该TsFile文件里的所有设备ID，放入list里并返回。具体做法是使用该TsFile的IndexOfTimeseriesIndex的第一个根索引节点进行递归查找其下的所有LEAF_DEVICE子节点，从而获取该根节点下的所有设备ID
    if (tsFileMetaData == null) {
      readFileMetadata();//使用tsFileInput对象读取对应TsFile里的IndexOfTimeseriesIndex索引的所有内容读到bytebuffer缓存里，并从buffer里进行读取反序列化成该顺序读取器里的TsFileMetadata对象
    }
    return getAllDevices(tsFileMetaData.getMetadataIndex());//获取该索引节点包含的所有设备ID，返回设备ID列表（由于设备信息是在LEAF_DEVICE节点上的，具体做法是进行递归深度遍历每个设备节点，直至当前遍历的索引节点是LEAF_DEVICE节点，则读取该节点的条目内容获得一条条设备ID放入list里）
  }

  private List<String> getAllDevices(MetadataIndexNode metadataIndexNode) throws IOException {  //获取该索引节点包含的所有设备ID，返回设备ID列表（由于设备信息是在LEAF_DEVICE节点上的，具体做法是进行递归深度遍历每个设备节点，直至当前遍历的索引节点是LEAF_DEVICE节点，则读取该节点的条目内容获得一条条设备ID放入list里）
    List<String> deviceList = new ArrayList<>();
    int metadataIndexListSize = metadataIndexNode.getChildren().size(); //获取该索引节点对象的子节点的数量

    // if metadataIndexNode is LEAF_DEVICE, put all devices in node entry into the list
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {//若当前节点类型是LEAF_DEVICE，则将当前节点包含的所有设备ID加入deviceList里，并返回deviceList
      deviceList.addAll(
          metadataIndexNode.getChildren().stream()  //当前节点的一条条的条目内容的二进制流，代表了该索引节点有哪些子节点以及各自的偏移量是多少
              .map(MetadataIndexEntry::getName)
              .collect(Collectors.toList()));
      return deviceList;
    }

    for (int i = 0; i < metadataIndexListSize; i++) {//循环遍历当前索引节点的每个子节点
      long endOffset = metadataIndexNode.getEndOffset();//获取该索引节点的末尾偏移量位置
      if (i != metadataIndexListSize - 1) { //若此时遍历的不是最后一个子节点
        endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();//获取当前子节点的后一个子节点的开始偏移量
      }
      ByteBuffer buffer = readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);//从TsFile里读取当前子索引节点的内容到二进制buffer里，即读取的是当前子节点的开始位置到下一个子节点开始的位置
      MetadataIndexNode node = MetadataIndexNode.deserializeFrom(buffer);//将buffer的内容反序列化成索引节点MetadataIndexNode对象,即使用从buffer反序列化读取的节点条目（即子节点索引项）和结束偏移和节点类型创建一个索引节点对象
      if (node.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {//若当前子索引节点的类型是LEAF_DEVICE，则
        // if node in next level is LEAF_DEVICE, put all devices in node entry into the list
        deviceList.addAll(//将当前节点包含的所有设备ID加入deviceList里
            node.getChildren().stream()
                .map(MetadataIndexEntry::getName)
                .collect(Collectors.toList()));
      } else {
        // keep traversing
        deviceList.addAll(getAllDevices(node));//递归遍历获取该节点下的子节点的所包含的所有设备
      }
    }
    return deviceList;
  }

  /**
   * read all ChunkMetaDatas of given device
   *
   * @param device name
   * @return measurement -> ChunkMetadata list
   * @throws IOException io error
   */
  public Map<String, List<ChunkMetadata>> readChunkMetadataInDevice(String device)
      throws IOException {
    readFileMetadata();
    List<TimeseriesMetadata> timeseriesMetadataMap = getDeviceTimeseriesMetadata(device);
    if (timeseriesMetadataMap.isEmpty()) {
      return new HashMap<>();
    }
    Map<String, List<ChunkMetadata>> seriesMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap) {
      seriesMetadata.put(
          timeseriesMetadata.getMeasurementId(),
          timeseriesMetadata.getChunkMetadataList().stream()
              .map(chunkMetadata -> ((ChunkMetadata) chunkMetadata))
              .collect(Collectors.toList()));
    }
    return seriesMetadata;
  }

  /**
   * this function return all timeseries names in this file
   *
   * @return list of Paths
   * @throws IOException io error
   */
  public List<Path> getAllPaths() throws IOException {
    List<Path> paths = new ArrayList<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (String measurementId : timeseriesMetadataMap.keySet()) {
        paths.add(new Path(device, measurementId));
      }
    }
    return paths;
  }

  /**
   * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
   *
   * @param metadataIndex MetadataIndexEntry
   * @param buffer byte buffer
   * @param deviceId String
   * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
   * @param needChunkMetadata deserialize chunk metadata list or not
   */
  private void generateMetadataIndex(
      MetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      String deviceId,
      MetadataIndexNodeType type,
      Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap,
      boolean needChunkMetadata)
      throws IOException {
    try {
      if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata));
        }
        timeseriesMetadataMap
            .computeIfAbsent(deviceId, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
      } else {
        // deviceId should be determined by LEAF_DEVICE node
        if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
          deviceId = metadataIndex.getName();
        }
        MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          ByteBuffer nextBuffer =
              readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
          generateMetadataIndex(
              metadataIndexNode.getChildren().get(i),
              nextBuffer,
              deviceId,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap,
              needChunkMetadata);
        }
      }
    } catch (BufferOverflowException e) {
      logger.error("Something error happened while generating MetadataIndex of file {}", file);
      throw e;
    }
  }

  /* TimeseriesMetadata don't need deserialize chunk metadata list */
  public Map<String, List<TimeseriesMetadata>> getAllTimeseriesMetadata() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new HashMap<>();
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = tsFileMetaData.getMetadataIndex().getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
      generateMetadataIndex(
          metadataIndexEntry,
          buffer,
          null,
          metadataIndexNode.getNodeType(),
          timeseriesMetadataMap,
          false);
    }
    return timeseriesMetadataMap;
  }

  /* This method will only deserialize the TimeseriesMetadata, not including chunk metadata list */
  private List<TimeseriesMetadata> getDeviceTimeseriesMetadataWithoutChunkMetadata(String device)
      throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        false);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  /* This method will not only deserialize the TimeseriesMetadata, but also all the chunk metadata list meanwhile. */
  private List<TimeseriesMetadata> getDeviceTimeseriesMetadata(String device) throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        true);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  /**
   * Get target MetadataIndexEntry and its end offset
   *
   * @param metadataIndex given MetadataIndexNode 索引节点对象
   * @param name target device / measurement name
   * @param isDeviceLevel whether target MetadataIndexNode is device level    //判断第一个参数索引节点对象是否是设备类型的节点
   * @param exactSearch whether is in exact search mode, return null when there is no entry with  //是否是精确搜索模式
   *     name; or else return the nearest MetadataIndexEntry before it (for deeper search)
   * @return target MetadataIndexEntry, endOffset pair
   */
  protected Pair<MetadataIndexEntry, Long> getMetadataAndEndOffset(//从指定的MetadataIndexNode节点对象里根据name查找对应的目标索引条目和它所指向的孩子节点的结束偏移位置（因此若是中间节点且不存在目标条目，则要递归到目标条目所在的叶子节点去查找）：1. 若找到了，则把该条目对应条目对象和该条目指向的子节点的结束位置放入pair对象里，即<名为key的索引条目对象，该条目指向的子节点的结束偏移位置> 2. 若没有找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
      MetadataIndexNode metadataIndex, String name, boolean isDeviceLevel, boolean exactSearch)
      throws IOException {
    try {
      // When searching for a device node, return when it is not INTERNAL_DEVICE
      // When searching for a measurement node, return when it is not INTERNAL_MEASUREMENT
      if ((isDeviceLevel
              && !metadataIndex.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE))//若是设备类型的节点且节点类型是LEAF_DEVICE
          || (!isDeviceLevel
              && !metadataIndex.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT))) {  //若是传感器类型的节点且节点类型是LEAF_MEASUREMENT
        return metadataIndex.getChildIndexEntry(name, exactSearch);//该方法其实就是用来查找名为name的索引条目它所指向的孩子节点的开始偏移位置和结束偏移位置。具体做法是从当前索引节点查找名字为name的索引条目：1. 若找到了，则把该条目对应条目对象和该条目指向的子节点的结束位置放入pair对象里，即<名为key的索引条目对象，该条目指向的子节点的结束偏移位置> 2. 若没有找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
      } else {//若节点类型是INTERNAL_DEVICE或者INTERNAL_MEASUREMENT，则
        Pair<MetadataIndexEntry, Long> childIndexEntry =//该方法其实就是用来查找名为name的索引条目它所指向的孩子节点的开始偏移位置和结束偏移位置。具体做法是从当前索引节点查找名字为name的索引条目：1. 若找到了，则把该条目对应条目对象和该条目指向的子节点的结束位置放入pair对象里，即<名为key的索引条目对象，该条目指向的子节点的结束偏移位置> 2. 若没有找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
            metadataIndex.getChildIndexEntry(name, false);//此处其实是获得了叶子节点里的目标条目对应在中间节点的索引条目对象（称之为目标条目）和其子节点的结束位置
        ByteBuffer buffer = readData(childIndexEntry.left.getOffset(), childIndexEntry.right);//将目标条目指向的子节点的内容读取到二进制缓存里
        return getMetadataAndEndOffset( //使用该索引节点继续递归
            MetadataIndexNode.deserializeFrom(buffer), name, isDeviceLevel, exactSearch);//将目标条目指向的子节点的内容反序列化。将buffer的内容反序列化成索引节点MetadataIndexNode对象,即使用从buffer反序列化读取的节点条目（即子节点索引项）和结束偏移和节点类型创建一个索引节点对象
      }
    } catch (BufferOverflowException e) {
      logger.error("Something error happened while deserializing MetadataIndex of file {}", file);
      throw e;
    }
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupHeader() throws IOException {
    return ChunkGroupHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER.
   *
   * @param position the offset of the chunk group footer in the file
   * @param markerRead true if the offset does not contains the marker , otherwise false
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupHeader(long position, boolean markerRead)
      throws IOException {
    return ChunkGroupHeader.deserializeFrom(tsFileInput, position, markerRead);
  }

  public void readPlanIndex() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    minPlanIndex = buffer.getLong();
    buffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    maxPlanIndex = buffer.getLong();
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader(byte chunkType) throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), chunkType);
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize) throws IOException {// 根据该Chunk的ChunkHeader在TsFile文件中的偏移量和该ChunkHeader的大小，使用TsFileInput对象来读取本地文件并反序列化成一个ChunkHeader对象
    return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize);//使用TsFileInput对象从指定的ChunkHeader偏移量和ChunkHeader大小，来读取本地文件并反序列化成一个ChunkHeader对象
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  private ByteBuffer readChunk(long position, int dataSize) throws IOException {//从指定偏移量开始读取dataSize的大小到buffer缓存
    return readData(position, dataSize);
  }

  /**
   * read memory chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {  //根据传来的ChunkIndex，使用TsFileInput读取本地文件并反序列化成对应的Chunk对象
    int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());//根据传感器ID获取对应Chunk的ChunkHeader的字节大小
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize);// 根据该Chunk的ChunkHeader在TsFile文件中的偏移量和该ChunkHeader的大小，使用TsFileInput对象来读取本地文件并反序列化成一个ChunkHeader对象
    ByteBuffer buffer =
        readChunk(
            metaData.getOffsetOfChunkHeader() + header.getSerializedSize(), header.getDataSize());//读取该Chunk的ChunkData到buffer缓存，即从该Chunk的ChunkData偏移量开始读取ChunkData的大小到buffer缓存
    return new Chunk(header, buffer, metaData.getDeleteIntervalList(), metaData.getStatistics());
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type, boolean hasStatistic) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type, hasStatistic);
  }

  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void position(long offset) throws IOException {
    tsFileInput.position(offset);
  }

  public void skipPageData(PageHeader header) throws IOException {
    tsFileInput.position(tsFileInput.position() + header.getCompressedSize());
  }

  public ByteBuffer readCompressedPage(PageHeader header) throws IOException {
    return readData(-1, header.getCompressedSize());
  }

  public ByteBuffer readPage(PageHeader header, CompressionType type)
      throws IOException { // 根据pageHeader和压缩类型，把该page的二进制流数据进行解压后的二进制流数据存入ByteBuffer缓存并返回
    ByteBuffer buffer =
        readData(-1, header.getCompressedSize()); // 从当前读指针位置开始，读取该page压缩后字节数量的数据到ByteBuffer缓存里
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type); // 获取解压缩类对象
    ByteBuffer uncompressedBuffer =
        ByteBuffer.allocate(header.getUncompressedSize()); // 创建该page解压后字节数量大小的缓存
    if (type == CompressionType.UNCOMPRESSED) { // 若该page是未压缩的
      return buffer;
    } // FIXME if the buffer is not array-implemented.
    unCompressor.uncompress( // 对该page的数据进行解压缩并存入uncompressedBuffer缓存里
        buffer.array(), buffer.position(), buffer.remaining(), uncompressedBuffer.array(), 0);
    return uncompressedBuffer;
  }

  /**
   * read one byte from the input. <br>
   * this method is not thread safe
   */
  public byte readMarker() throws IOException { // 读取一个字节
    markerBuffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, markerBuffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    markerBuffer.flip();
    return markerBuffer.get();
  }

  @Override
  public void close() throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is closed.", file);
    }
    this.tsFileInput.close();
  }

  public String getFileName() {
    return this.file;
  }

  public long fileSize() throws IOException {
    return tsFileInput.size();
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position. <br>
   * if position = -1, the tsFileInput's position will be changed to the current position + real
   * data size that been read. Other wise, the tsFileInput's position is not changed.
   *
   * @param position the start position of data in the tsFileInput, or the current position if
   *     position = -1
   * @param size the size of data that want to read
   * @return data that been read.
   */
  protected ByteBuffer readData(long position, int size)
      throws IOException { //使用tsFileInput对象读取对应TsFile里从position位置开始，共计size个字节的数据到ByteBuffer缓存里.若position为-1，则从当前文件的指针位置开始读取
    ByteBuffer buffer = ByteBuffer.allocate(size); // 指定缓存大小，并创建缓存
    if (position < 0) {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != size) {//使用TsFileInput对象把对应TsFile的内容读到buffer缓存里直至装满或者文件末尾，返回读取的字节数
        throw new IOException("reach the end of the data");
      }
    } else {
      long actualReadSize = ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, size); //使用TsFileInput对象把对应TsFile的内容，从第offset偏移量的位置开始读取读到target二进制缓存里，读取的长度为len
      if (actualReadSize != size) {
        throw new IOException(
            String.format(
                "reach the end of the data. Size of data that want to read: %s,"
                    + "actual read size: %s, position: %s",
                size, actualReadSize, position));
      }
    }
    buffer.flip();
    return buffer;
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position.
   *
   * @param start the start position of data in the tsFileInput, or the current position if position
   *     = -1
   * @param end the end position of data that want to read
   * @return data that been read.
   */
  protected ByteBuffer readData(long start, long end) throws IOException {//从该TsFile里读取第start位置到第end位置的内容到Bytebuffer里，并返回该buffer
    return readData(start, (int) (end - start));
  }

  /** notice, the target bytebuffer are not flipped. */
  public int readRaw(long position, int length, ByteBuffer target) throws IOException {
    return ReadWriteIOUtils.readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   *
   * @param newSchema the schema on each time series in the file
   * @param chunkGroupMetadataList ChunkGroupMetadata List
   * @param fastFinish if true and the file is complete, then newSchema and chunkGroupMetadataList
   *     parameter will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   *     be truncated.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public long selfCheck(
      Map<Path, IMeasurementSchema> newSchema,
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      boolean fastFinish)
      throws IOException {
    File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
    long fileSize;
    if (!checkFile.exists()) {
      return TsFileCheckStatus.FILE_NOT_FOUND;
    } else {
      fileSize = checkFile.length();
    }
    ChunkMetadata currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;

    // ChunkMetadata of current ChunkGroup
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();

    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
    if (fileSize < headerLength) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic())
        || (TSFileConfig.VERSION_NUMBER != readVersionNumber())) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    tsFileInput.position(headerLength);
    if (fileSize == headerLength) {
      return headerLength;
    } else if (isComplete()) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }
    // not a complete file, we will recover it...
    long truncatedSize = headerLength;
    byte marker;
    String lastDeviceId = null;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | 0x80):
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | 0x40):
            fileOffsetOfChunk = this.position() - 1;
            // if there is something wrong with a chunk, we will drop the whole ChunkGroup
            // as different chunks may be created by the same insertions(sqls), and partial
            // insertion is not tolerable
            ChunkHeader chunkHeader = this.readChunkHeader(marker);
            measurementID = chunkHeader.getMeasurementID();
            IMeasurementSchema measurementSchema =
                new UnaryMeasurementSchema(
                    measurementID,
                    chunkHeader.getDataType(),
                    chunkHeader.getEncodingType(),
                    chunkHeader.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            dataType = chunkHeader.getDataType();
            Statistics<? extends Serializable> chunkStatistics =
                Statistics.getStatsByType(dataType);
            int dataSize = chunkHeader.getDataSize();
            if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.CHUNK_HEADER) {
              while (dataSize > 0) {
                // a new Page
                PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), true);
                chunkStatistics.mergeStatistics(pageHeader.getStatistics());
                this.skipPageData(pageHeader);
                dataSize -= pageHeader.getSerializedPageSize();
                chunkHeader.increasePageNums(1);
              }
            } else {
              // only one page without statistic, we need to iterate each point to generate
              // statistic
              PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), false);
              Decoder valueDecoder =
                  Decoder.getDecoderByType(
                      chunkHeader.getEncodingType(), chunkHeader.getDataType());
              ByteBuffer pageData = readPage(pageHeader, chunkHeader.getCompressionType());
              Decoder timeDecoder =
                  Decoder.getDecoderByType(
                      TSEncoding.valueOf(
                          TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                      TSDataType.INT64);
              PageReader reader =
                  new PageReader(
                      pageHeader,
                      pageData,
                      chunkHeader.getDataType(),
                      valueDecoder,
                      timeDecoder,
                      null);
              BatchData batchData = reader.getAllSatisfiedPageData();
              while (batchData.hasCurrent()) {
                switch (dataType) {
                  case INT32:
                    chunkStatistics.update(batchData.currentTime(), batchData.getInt());
                    break;
                  case INT64:
                    chunkStatistics.update(batchData.currentTime(), batchData.getLong());
                    break;
                  case FLOAT:
                    chunkStatistics.update(batchData.currentTime(), batchData.getFloat());
                    break;
                  case DOUBLE:
                    chunkStatistics.update(batchData.currentTime(), batchData.getDouble());
                    break;
                  case BOOLEAN:
                    chunkStatistics.update(batchData.currentTime(), batchData.getBoolean());
                    break;
                  case TEXT:
                    chunkStatistics.update(batchData.currentTime(), batchData.getBinary());
                    break;
                  default:
                    throw new IOException("Unexpected type " + dataType);
                }
                batchData.next();
              }
              chunkHeader.increasePageNums(1);
            }
            currentChunk =
                new ChunkMetadata(measurementID, dataType, fileOffsetOfChunk, chunkStatistics);
            chunkMetadataList.add(currentChunk);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // if there is something wrong with the ChunkGroup Header, we will drop this ChunkGroup
            // because we can not guarantee the correctness of the deviceId.
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId()), tsSchema);
                }
              }
              measurementSchemaList = new ArrayList<>();
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
            }
            // this is a chunk group
            chunkMetadataList = new ArrayList<>();
            ChunkGroupHeader chunkGroupHeader = this.readChunkGroupHeader();
            lastDeviceId = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId()), tsSchema);
                }
              }
              measurementSchemaList = new ArrayList<>();
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
              lastDeviceId = null;
            }
            readPlanIndex();
            truncatedSize = this.position();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
      // now we read the tail of the data section, so we are sure that the last
      // ChunkGroupFooter is complete.
      if (lastDeviceId != null) {
        // schema of last chunk group
        if (newSchema != null) {
          for (IMeasurementSchema tsSchema : measurementSchemaList) {
            newSchema.putIfAbsent(new Path(lastDeviceId, tsSchema.getMeasurementId()), tsSchema);
          }
        }
        // last chunk group Metadata
        chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
      }
      truncatedSize = this.position() - 1;
    } catch (Exception e) {
      logger.info(
          "TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
          file,
          this.position(),
          e.getMessage());
    }
    // Despite the completeness of the data section, we will discard current FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedSize;
  }

  /**
   * get ChunkMetaDatas of given path, and throw exception if path not exists
   *
   * @param path timeseries path
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> getChunkMetadataList(Path path, boolean ignoreNotExists)
      throws IOException {//根据给定的时间序列path，获取其TimeseriesIndex对象里的所有ChunkIndex，并按照每个ChunkIndex的开始时间戳从小到大进行排序并返回
    TimeseriesMetadata timeseriesMetaData = readTimeseriesMetadata(path, ignoreNotExists);//根据时间序列路径path，获取该时间序列在该TsFile里的TimeseriesIndex索引对象。具体做法是：（1）先获取该序列对应设备的索引条目指向的传感器节点内容（2）根据序列的传感器ID获取传感器条目指向的TimeseriesIndex节点（该节点包含了目标path序列的TimeseriesIndex索引）内容（3）将读取到的TimeseriesIndex节点内容buffer里的每个TimeseriesIndex依次反序列化放进临时列表里，最后获取目标path序列的TimeseriesIndex索引并返回
    if (timeseriesMetaData == null) { //若为空，说明该TsFile里不存在此时间序列path,返回空列表
      return Collections.emptyList();
    }
    List<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetaData);//从指定的TimeseriesIndex对象里获取对应的所有ChunkIndex，存入列表里并返回
    chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));//将该chunkMetadataList列表依次每个ChunkIndex的开始时间戳从小到大进行排序
    return chunkMetadataList;
  }

  public List<ChunkMetadata> getChunkMetadataList(Path path) throws IOException {//根据给定的时间序列path，获取其TimeseriesIndex对象里的所有ChunkIndex，并按照每个ChunkIndex的开始时间戳从小到大进行排序并返回
    return getChunkMetadataList(path, false);
  }

  /**
   * get ChunkMetaDatas in given TimeseriesMetaData
   *
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> readChunkMetaDataList(TimeseriesMetadata timeseriesMetaData)
      throws IOException {//从指定的TimeseriesIndex对象里获取对应的所有ChunkIndex，存入列表里并返回
    return timeseriesMetaData.getChunkMetadataList().stream()
        .map(chunkMetadata -> (ChunkMetadata) chunkMetadata)
        .collect(Collectors.toList());
  }

  /**
   * get all measurements in this file
   *
   * @return measurement -> datatype
   */
  public Map<String, TSDataType> getAllMeasurements() throws IOException {
    Map<String, TSDataType> result = new HashMap<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      }
    }
    return result;
  }

  public Map<String, List<String>> getDeviceMeasurementsMap() throws IOException {
    Map<String, List<String>> result = new HashMap<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result
            .computeIfAbsent(device, d -> new ArrayList<>())
            .add(timeseriesMetadata.getMeasurementId());
      }
    }
    return result;
  }

  /**
   * get device names which has valid chunks in [start, end)
   *
   * @param start start of the partition
   * @param end end of the partition
   * @return device names in range
   */
  public List<String> getDeviceNameInRange(long start, long end) throws IOException {
    List<String> res = new ArrayList<>();
    for (String device : getAllDevices()) {
      Map<String, List<ChunkMetadata>> seriesMetadataMap = readChunkMetadataInDevice(device);
      if (hasDataInPartition(seriesMetadataMap, start, end)) {
        res.add(device);
      }
    }
    return res;
  }

  /**
   * get metadata index node
   *
   * @param startOffset start read offset
   * @param endOffset end read offset
   * @return MetadataIndexNode
   */
  public MetadataIndexNode getMetadataIndexNode(long startOffset, long endOffset)
      throws IOException {
    return MetadataIndexNode.deserializeFrom(readData(startOffset, endOffset));
  }

  /**
   * Check if the device has at least one Chunk in this partition
   *
   * @param seriesMetadataMap chunkMetaDataList of each measurement
   * @param start the start position of the space partition
   * @param end the end position of the space partition
   */
  private boolean hasDataInPartition(
      Map<String, List<ChunkMetadata>> seriesMetadataMap, long start, long end) {
    for (List<ChunkMetadata> chunkMetadataList : seriesMetadataMap.values()) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        LocateStatus location =
            MetadataQuerierByFileImpl.checkLocateStatus(chunkMetadata, start, end);
        if (location == LocateStatus.in) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The location of a chunkGroupMetaData with respect to a space partition constraint.
   *
   * <p>in - the middle point of the chunkGroupMetaData is located in the current space partition.
   * before - the middle point of the chunkGroupMetaData is located before the current space
   * partition. after - the middle point of the chunkGroupMetaData is located after the current
   * space partition.
   */
  public enum LocateStatus {
    in,
    before,
    after
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  /**
   * @return An iterator of linked hashmaps ( measurement -> chunk metadata list ). When traversing
   *     the linked hashmap, you will get chunk metadata lists according to the lexicographic order
   *     of the measurements. The first measurement of the linked hashmap of each iteration is
   *     always larger than the last measurement of the linked hashmap of the previous iteration in
   *     lexicographic order.
   */
  public Iterator<Map<String, List<ChunkMetadata>>> getMeasurementChunkMetadataListMapIterator(
      String device) throws IOException {
    readFileMetadata();

    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);

    if (metadataIndexPair == null) {
      return new Iterator<Map<String, List<ChunkMetadata>>>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public LinkedHashMap<String, List<ChunkMetadata>> next() {
          throw new NoSuchElementException();
        }
      };
    }

    Queue<Pair<Long, Long>> queue = new LinkedList<>();
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    collectEachLeafMeasurementNodeOffsetRange(buffer, queue);

    return new Iterator<Map<String, List<ChunkMetadata>>>() {

      @Override
      public boolean hasNext() {
        return !queue.isEmpty();
      }

      @Override
      public LinkedHashMap<String, List<ChunkMetadata>> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Pair<Long, Long> startEndPair = queue.remove();
        LinkedHashMap<String, List<ChunkMetadata>> measurementChunkMetadataList =
            new LinkedHashMap<>();
        try {
          List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
          ByteBuffer nextBuffer = readData(startEndPair.left, startEndPair.right);
          while (nextBuffer.hasRemaining()) {
            timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(nextBuffer, true));
          }
          for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
            List<ChunkMetadata> list =
                measurementChunkMetadataList.computeIfAbsent(
                    timeseriesMetadata.getMeasurementId(), m -> new ArrayList<>());
            for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
              list.add((ChunkMetadata) chunkMetadata);
            }
          }
          return measurementChunkMetadataList;
        } catch (IOException e) {
          throw new TsFileRuntimeException(
              "Error occurred while reading a time series metadata block.");
        }
      }
    };
  }

  private void collectEachLeafMeasurementNodeOffsetRange(
      ByteBuffer buffer, Queue<Pair<Long, Long>> queue) throws IOException {
    try {
      final MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      final MetadataIndexNodeType metadataIndexNodeType = metadataIndexNode.getNodeType();
      final int metadataIndexListSize = metadataIndexNode.getChildren().size();
      for (int i = 0; i < metadataIndexListSize; ++i) {
        long startOffset = metadataIndexNode.getChildren().get(i).getOffset();
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        if (metadataIndexNodeType.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          queue.add(new Pair<>(startOffset, endOffset));
          continue;
        }
        collectEachLeafMeasurementNodeOffsetRange(readData(startOffset, endOffset), queue);
      }
    } catch (BufferOverflowException e) {
      logger.error(
          "Error occurred while collecting offset ranges of measurement nodes of file {}", file);
      throw e;
    }
  }
}
