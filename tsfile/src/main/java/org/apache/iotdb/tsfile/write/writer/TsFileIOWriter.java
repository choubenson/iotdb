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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * TsFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter { // TsFile写入类，在写入操作中，要写入一个TSFile就要对应一个TsFileIOWriter类对象

  protected static final byte[] MAGIC_STRING_BYTES;
  public static final byte VERSION_NUMBER_BYTE;
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  static {
    MAGIC_STRING_BYTES = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    VERSION_NUMBER_BYTE = TSFileConfig.VERSION_NUMBER;
  }

  protected TsFileOutput
      out; // TsFile写入类对象，它有相应的输出缓存流BufferedOutputStream（该缓存流有对应的缓存数组）。所有往该TsFile写入的内容都会被存入该TsFileOutput对象的输出流BufferedOutputStream的缓存数组里
  protected boolean canWrite = true;
  protected File file; // 该写入类要写入的TSFile文件

  // current flushed Chunk
  private ChunkMetadata currentChunkMetadata; // 当前要写入的ChunkGroup的Chunk的ChunkIndex对象,可能是多元序列的时间Chunk的ChunkIndex（数据类型为vector）或各数值Chunk的ChunkIndex
  // current flushed ChunkGroup
  protected List<ChunkMetadata> chunkMetadataList =
      new ArrayList<>(); // 该列表存放当前写操作的ChunkGroup里的所有Chunk的ChunkIndex对象
  // all flushed ChunkGroups
  protected List<ChunkGroupMetadata> chunkGroupMetadataList =
      new ArrayList<>(); // 该写入类要写入的TsFile的所有ChunkGroup元数据列表，每个ChunkGroupMetedata存放了该ChunkGroup里所有Chunk对应的ChunkIndex，可能包含多元序列的时间Chunk的ChunkIndex（数据类型为vector）或各数值Chunk的ChunkIndex

  private long markedPosition;
  private String currentChunkGroupDeviceId; // 当前ChunkGroup对应的设备ID

  // for upgrade tool
  Map<String, List<TimeseriesMetadata>>
      deviceTimeseriesMetadataMap; // (设备路径，该设备下所有传感器的TimeseriesMetadata对象列表)

  // the two longs marks the index range of operations in current MemTable
  // and are serialized after MetaMarker.OPERATION_INDEX_RANGE to recover file-level range
  private long minPlanIndex;
  private long maxPlanIndex;

  /** empty construct function. */
  protected TsFileIOWriter() {}

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this.out =
        FSFactoryProducer.getFileOutputFactory()
            .getTsFileOutput(file.getPath(), false); // 获取给定TsFile文件的写入流
    this.file = file;
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} writer is opened.", file.getName());
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.out = output;
    startFile();
  }

  /** for test only */
  public TsFileIOWriter(TsFileOutput output, boolean test) {
    this.out = output;
  }

  /**
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes)
      throws IOException { // 该参数是待写入Chunk的输出流pageBuffer(里面存放了该Chunk的所有page数据（pageHeader+pageData）)
    // 将指定Chunk里的所有page数据（pageHeader+pageData）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
    bytes.writeTo(out.wrapAsStream());
  }

  protected void startFile()
      throws
          IOException { // 往该TsFileIOWriter的文件写入对象TsFileOutput的缓存输出流BufferedOutputStream的数组里写Magic
    // String和version number
    out.write(MAGIC_STRING_BYTES); // 写入Magic String
    out.write(VERSION_NUMBER_BYTE); // 写入version number
  }

  public void startChunkGroup(String deviceId)
      throws
          IOException { // 开启一个新的设备ChunkGroup，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
    this.currentChunkGroupDeviceId = deviceId; // 设置当前的ChunkGroup的设备ID
    if (logger.isDebugEnabled()) {
      logger.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    }
    chunkMetadataList = new ArrayList<>(); // 初始化该ChunkGroup的Chunk元数据对象列表，为空列表
    ChunkGroupHeader chunkGroupHeader =
        new ChunkGroupHeader(currentChunkGroupDeviceId); // 根据设备ID创建当前ChunkGroup的ChunkGroupHeader
    chunkGroupHeader.serializeTo(
        out
            .wrapAsStream()); // 将TsFile写入类对象out封装成outputStream输出流，并向该输出流写入ChunkGroupHeader的内容（marker（为0）和设备ID），返回写入的字节数
  }

  /**
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  // 每往TSFileIOWriter的缓存里写完一个ChunkGroup的数据就会将该缓存里的内容flush到本地TsFile文件
  public void endChunkGroup()
      throws
          IOException { // 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
    if (currentChunkGroupDeviceId == null
        || chunkMetadataList
            .isEmpty()) { // 若当前ChunkGroup的所属设备ID为null或者某ChunkGroup的所有Chunk的元数据类对象列表为空  则返回
      return;
    }
    chunkGroupMetadataList.add( // 往该TSFile写入类对象的chunkGroupMetadataList缓存里加入该待被结束的ChunkGroup的元数据对象
        new ChunkGroupMetadata(
            currentChunkGroupDeviceId,
            chunkMetadataList)); // 根据当前ChunkGroup的设备ID和Chunk其元数据对象列表创建ChunkGroup元数据类对象，并加入该写入类要写入的TsFile的ChunkGroup元数据列表
    currentChunkGroupDeviceId = null;
    chunkMetadataList = null;
    out.flush(); // 将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地文件
  }

  /**
   * For TsFileReWriteTool / UpgradeTool. Use this method to determine if needs to start a
   * ChunkGroup.
   *
   * @return isWritingChunkGroup
   */
  public boolean isWritingChunkGroup() {
    return currentChunkGroupDeviceId != null;
  }

  /**
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param measurementId - measurementId of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @param mask - 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
   * @throws IOException if I/O error occurs
   */
  public void
      startFlushChunk( // 初始化该TsFileIOWriter的当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
          String measurementId,
          CompressionType compressionCodecName,
          TSDataType tsDataType,  //若是多元序列的TimeChunk，则此处的类型是Vector.若是多元序列的ValueChunk，则此处是对应子传感器的数据类型
          TSEncoding encodingType,
          Statistics<? extends Serializable> statistics,
          int dataSize,
          int numOfPages,
          int mask)
          throws IOException {

    //Todo:bug?若是VectorChunk呢？是否应该VectorChunkMetadata?
    currentChunkMetadata = // 创建当前Chunk的元数据类对象
        new ChunkMetadata(measurementId, tsDataType, out.getPosition(), statistics);
    currentChunkMetadata.setMask((byte) mask);

    ChunkHeader header = // 创建当前Chunk的ChunkHeader
        new ChunkHeader(
            measurementId,
            dataSize,
            tsDataType,
            compressionCodecName,
            encodingType,
            numOfPages,
            mask);
    header.serializeTo(
        out
            .wrapAsStream()); // 把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
  }

  /** Write a whole chunk in another file into this file. Providing fast merge for IoTDB. */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata =
        new ChunkMetadata(
            chunkHeader.getMeasurementID(),
            chunkHeader.getDataType(),
            out.getPosition(),
            chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "end flushing a chunk:{}, totalvalue:{}",
          chunkHeader.getMeasurementID(),
          chunkMetadata.getNumOfPoints());
    }
  }

  /** end chunk and write some log. */
  public void endCurrentChunk() { // 当结束当前Chunk的写操作后就会调用此方法，往当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的ChunkIndex对象，并把当前Chunk元数据对象清空
    chunkMetadataList.add(
        currentChunkMetadata); // 往当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的ChunkIndex对象,可能包含多元序列的TimeChunkIndex或者ValueChunkIndex
    currentChunkMetadata = null; // 把当前Chunk元数据对象清空
  }

  /**
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @throws IOException if I/O error occurs
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void endFile()
      throws
          IOException { // 在向该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写完数据区的内容并flush到本地后，再往该缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）
    long metaOffset = out.getPosition();

    // serialize the SEPARATOR of MetaData
    ReadWriteIOUtils.write(
        MetaMarker.SEPARATOR,
        out
            .wrapAsStream()); // 往该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写入索引区的marker(为2)

    // group ChunkMetadata by series
    // only contains ordinary path and time column of vector series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap =
        new TreeMap<>(); // 传感器Chunk元数据对象列表，存放每个时间序列对应的其ChunkIndex列表，即（时间序列路径对象，ChunkIndex列表）。若是多元序列，则存放其TimeChunk的ChunkIndex

    // time column -> ChunkMetadataList TreeMap of value columns in vector
    Map<Path, Map<Path, List<IChunkMetadata>>> vectorToPathsMap = new HashMap<>();//存放每个多元序列路径对应的每个子分量传感器对应的所有ValueChunk的ChunkIndex列表

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {  //遍历当前TsFile的每个ChunkGroup的ChunkGroupMetedata
      List<ChunkMetadata> chunkMetadatas = chunkGroupMetadata.getChunkMetadataList(); //获取该ChunkGroup里的所有Chunk的ChunkIndex列表
      int idx = 0;
      while (idx < chunkMetadatas.size()) { //遍历该ChunkGroup里的每个ChunkIndex，该ChunkIndex也可能是一个多元序列的TimeChunkIndex或ValueChunkIndex
        IChunkMetadata chunkMetadata = chunkMetadatas.get(idx);
        if (chunkMetadata.getMask() == 0) {//若是一元Chunk,则
          Path series = new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());//该一元序列的全路径
          chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);//往chunkMetadataListMap里加入该序列对应的此ChunkIndex
          idx++;
        } else if (chunkMetadata.isTimeColumn()) {//若是多元序列的TimeChunk,则
          // time column of a vector series
          Path series = new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());//多元序列的路徑，到传感器
          chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);  //往chunkMetadataListMap里加入该序列对应的此VectorChunkIndex
          idx++;
          Map<Path, List<IChunkMetadata>> chunkMetadataListMapInVector =//获取该多元序列的每个子分量传感器对应的所有ChunkIndex列表
              vectorToPathsMap.computeIfAbsent(series, key -> new TreeMap<>());

          // value columns of a vector series
          while (idx < chunkMetadatas.size() && chunkMetadatas.get(idx).isValueColumn()) {//继续遍历该多元序列后续的ValueChunkIndex
            chunkMetadata = chunkMetadatas.get(idx);//获取ValueChunkIndex
            Path vectorSeries =
                new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());//此多元序列的该子传感器的全路径
            chunkMetadataListMapInVector  //往chunkMetadataListMapInVector的该多元序列的该子分量里添加此ValueChunkIndex
                .computeIfAbsent(vectorSeries, k -> new ArrayList<>())
                .add(chunkMetadata);
            idx++;
          }
        }
      }
    }

    //索引树的根节点
    MetadataIndexNode metadataIndex = flushMetadataIndex(chunkMetadataListMap, vectorToPathsMap);//（1）遍历每个时间序列路径及其对应的ChunkIndex列表，把该一元或多元时间序列的所有ChunkIndex给flush到临时缓存流，并用该临时缓存流创建对应该序列的TimeseriesIndex,并将其加入deviceTimeseriesMetadataMap对应的该设备的序列索引（2）根据每个设备对应的所有TimeseriesIndex(每个TimeseriesIndex又存放了该序列的所有ChunkIndex)创建一颗树，并把出来TsFileMetadata的其他索引数据按序序列化到out缓存里
    TsFileMetadata tsFileMetaData = new TsFileMetadata();
    tsFileMetaData.setMetadataIndex(metadataIndex);
    tsFileMetaData.setMetaOffset(metaOffset);

    long footerIndex = out.getPosition();
    if (logger.isDebugEnabled()) {
      logger.debug("start to flush the footer,file pos:{}", footerIndex);
    }

    // write TsFileMetaData
    int size = tsFileMetaData.serializeTo(out.wrapAsStream());//把IndexOfTimeseriesIndex里的根节点和metaOffset序列化到outputStream缓存
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPosition());
    }

    // write bloom filter
    size += tsFileMetaData.serializeBloomFilter(out.wrapAsStream(), chunkMetadataListMap.keySet());//序列化bloomFilter
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the bloom filter file pos:{}", out.getPosition());
    }

    // write TsFileMetaData size  //序列化TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream()); // write the size of the file metadata.

    // write magic string   //序列化MAGIC_STRING
    out.write(MAGIC_STRING_BYTES);

    // close file
    out
        .close(); // 关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入缓存里的数据（即整个TsFile内容，包括数据区和索引区等）
    if (resourceLogger.isDebugEnabled() && file != null) {
      resourceLogger.debug("{} writer is closed.", file.getName());
    }
    canWrite = false;
  }

  /**
   * Flush TsFileMetadata, including ChunkMetadataList and TimeseriesMetaData
   *
   * @param chunkMetadataListMap chunkMetadata that Path.mask == 0
   * @param vectorToPathsMap Map Path to chunkMataList, Key is Path(timeColumn) and Value is it's
   *     sub chunkMetadataListMap
   * @return MetadataIndexEntry list in TsFileMetadata
   */
  private MetadataIndexNode flushMetadataIndex(//（1）遍历每个时间序列路径及其对应的ChunkIndex列表，把该一元或多元时间序列的所有ChunkIndex给flush到临时缓存流，并用该临时缓存流创建对应该序列的TimeseriesIndex,并将其加入deviceTimeseriesMetadataMap对应的该设备的序列索引（2）根据每个设备对应的所有TimeseriesIndex(每个TimeseriesIndex又存放了该序列的所有ChunkIndex)创建一颗树，并把出来TsFileMetadata的其他索引数据按序序列化到out缓存里
      Map<Path, List<IChunkMetadata>> chunkMetadataListMap, //存放了每个时间序列对应的ChunkIndex列表。若是多元序列则只存放其TimeChunkIndex
      Map<Path, Map<Path, List<IChunkMetadata>>> vectorToPathsMap)  //存放每个多元序列路径对应的所有子传感器对应的所有ValueChunkIndex
      throws IOException {

    // convert ChunkMetadataList to this field
    deviceTimeseriesMetadataMap = new LinkedHashMap<>();//存放了(设备路径，该设备下所有传感器的TimeseriesMetadata对象列表)
    // create device -> TimeseriesMetaDataList Map
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {//遍历每个时间序列路径及其对应的ChunkIndex列表。若是多元，则会先传"device1.vector1"及其对应的TimeChunkIndex,然后
      // for ordinary path
      flushOneChunkMetadata(entry.getKey(), entry.getValue(), vectorToPathsMap);//把该一元或多元时间序列的所有ChunkIndex给flush到临时缓存流，并用该临时缓存流创建对应该序列的TimeseriesIndex,并将其加入deviceTimeseriesMetadataMap对应的该设备的序列索引
    }

    // construct TsFileMetadata and return
    return MetadataIndexConstructor.constructMetadataIndex(deviceTimeseriesMetadataMap, out);//根据每个设备对应的所有TimeseriesIndex(每个TimeseriesIndex又存放了该序列的所有ChunkIndex)创建一颗树，并把出来TsFileMetadata的其他索引数据按序序列化到out缓存里
  }

  /**
   * Flush one chunkMetadata
   *
   * @param path Path of chunk
   * @param chunkMetadataList List of chunkMetadata about path(previous param)
   * @param vectorToPathsMap Key is Path(timeColumn) and Value is it's sub chunkMetadataListMap
   */
  private void flushOneChunkMetadata(   //把该一元或多元时间序列的所有ChunkIndex给flush到临时缓存流，并用该临时缓存流创建对应该序列的TimeseriesIndex,并将其加入deviceTimeseriesMetadataMap对应的该设备的序列索引
      Path path,  //时间序列路径,可能是多元序列路径
      List<IChunkMetadata> chunkMetadataList, //该序列对应的所有ChunkIndex，若是多元序列则只存放其TimeChunk的ChunkIndex
      Map<Path, Map<Path, List<IChunkMetadata>>> vectorToPathsMap) //存放每个多元序列路径对应的所有子传感器对应的所有ValueChunkIndex
      throws IOException {
    // create TimeseriesMetaData
    PublicBAOS publicBAOS = new PublicBAOS(); //输出缓存流
    TSDataType dataType = chunkMetadataList.get(chunkMetadataList.size() - 1).getDataType();//获取该序列最后一个Chunk的数据类型。若是多元序列，则第一个TimeChunk的数据类型是Vector，后面几个ValueChunk则对应各自的数据类型。若是一元序列，则所有Chunk的数据类型统一
    Statistics seriesStatistics = Statistics.getStatsByType(dataType);  //获取该数据类型对应需要的统计量，为该TimeseriesIndex需要的统计量

    int chunkMetadataListLength = 0;//当前时间序列的所有ChunkIndex序列化的字节大小。若是多元，则先跳过序列化TimeChunkIndex，先序列化后续的ValueChunk
    boolean serializeStatistic = (chunkMetadataList.size() > 1);//判断是否需要序列化统计量，当该序列的Chunk数量大于1的时候，ChunkIndex才需要统计量
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {//遍历该序列的每个ChunkIndex，序列化当前时间序列的所有ChunkIndex
      if (!chunkMetadata.getDataType().equals(dataType)) { //Todo:bug??获取该ChunkIndex数据类型不是dataType，则说明该序列是多元序列，且此时是其第一个Chunk，为TimeChunk，要跳过，因为多元序列的第一个Chunk数据类型是Vector
        continue;
      }
      chunkMetadataListLength += chunkMetadata.serializeTo(publicBAOS, serializeStatistic);//把当前ChunkIndex的offsetOfChunkHeader变量以及统计量（serializeStatistic为true时）序列化到publicBAOS输出缓存流里
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());//把ChunkIndex的统计量对象里的参数合并到seriesStatistics统计量对象
    }

    //初始化当前时间序列的TimeseriesIndex，把其加入deviceTimeseriesMetadataMap里对应设备里，供后续要序列化TimeseriesIndex使用
    TimeseriesMetadata timeseriesMetadata =
        new TimeseriesMetadata(
            (byte)
                ((serializeStatistic ? (byte) 1 : (byte) 0) | chunkMetadataList.get(0).getMask()),  //该ChunkIndex的类型
            chunkMetadataListLength,//当前时间序列的所有ChunkIndex序列化的字节大小
            path.getMeasurement(),//当前序列的传感器ID
            dataType, //数据类型
            seriesStatistics, //该TimeseriesIndex需要的统计量
            publicBAOS);  //输出缓存流
    deviceTimeseriesMetadataMap //往deviceTimeseriesMetadataMap的该序列的设备里加入此序列TimeseriesIndex
        .computeIfAbsent(path.getDevice(), k -> new ArrayList<>())
        .add(timeseriesMetadata);

    // for VECTOR
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {//再次遍历该序列的每个ChunkIndex
      // chunkMetadata is time column of a vector series
      if (chunkMetadata.isTimeColumn()) { //若是时间ChunkIndex,则
        Map<Path, List<IChunkMetadata>> vectorMap = vectorToPathsMap.get(path);//获取该多元序列路径对应的所有子传感器对应的所有ValueChunkIndex

        for (Map.Entry<Path, List<IChunkMetadata>> entry : vectorMap.entrySet()) {//遍历该多元序列路径对应的所有子传感器对应的所有ValueChunkIndex
          flushOneChunkMetadata(entry.getKey(), entry.getValue(), vectorToPathsMap);
        }
      }
    }
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException { // 获取当前TsFileIOWriter对象的输出缓存流out的长度，或者说缓存流out的缓存数组的指针位置
    return out.getPosition();
  }

  // device -> ChunkMetadataList
  public Map<String, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<String, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      deviceChunkMetadataMap
          .computeIfAbsent(chunkGroupMetadata.getDevice(), k -> new ArrayList<>())
          .addAll(chunkGroupMetadata.getChunkMetadataList());
    }
    return deviceChunkMetadataMap;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public void mark() throws IOException {
    markedPosition = getPos();
  }

  public void reset() throws IOException {
    out.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    out.close();
  }

  void writeSeparatorMaskForTest() throws IOException {
    out.write(new byte[] {MetaMarker.SEPARATOR});
  }

  void writeChunkGroupMarkerForTest() throws IOException {
    out.write(new byte[] {MetaMarker.CHUNK_GROUP_HEADER});
  }

  public File getFile() { // 获取对应的TsFile文件对象
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  /** Remove such ChunkMetadata that its startTime is not in chunkStartTimes */
  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    Iterator<ChunkGroupMetadata> chunkGroupMetaDataIterator = chunkGroupMetadataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetadata chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDevice();
      int chunkNum = chunkGroupMetaData.getChunkMetadataList().size();
      Iterator<ChunkMetadata> chunkMetaDataIterator =
          chunkGroupMetaData.getChunkMetadataList().iterator();
      while (chunkMetaDataIterator.hasNext()) {
        IChunkMetadata chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);

        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid =
            startTimeIdx < pathChunkStartTimes.size()
                && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetaDataIterator.remove();
          chunkNum--;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
      }
    }
  }
  // 每往TSFileIOWriter的缓存里写完一个PlanIndex,就会将该缓存里的内容flush到本地TsFile文件
  public void writePlanIndices()
      throws
          IOException { // 往该TsFile对应的TsFileIOWriter类对象的TsFileOutput对象的输出流BufferedOutputStream的缓存数组里写入planIndex，并将该TsFileIOWriter类对象的TsFileOutput对象的输出流BufferedOutputStream的缓存数组内容flush到本地TsFile文件
    ReadWriteIOUtils.write(MetaMarker.OPERATION_INDEX_RANGE, out.wrapAsStream());
    ReadWriteIOUtils.write(minPlanIndex, out.wrapAsStream());
    ReadWriteIOUtils.write(maxPlanIndex, out.wrapAsStream());
    out.flush();
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return out;
  }

  /**
   * this function is only for Upgrade Tool.
   *
   * @return DeviceTimeseriesMetadataMap
   */
  public Map<String, List<TimeseriesMetadata>> getDeviceTimeseriesMetadataMap() {
    return deviceTimeseriesMetadataMap;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }
}
