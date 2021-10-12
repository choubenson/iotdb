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
package org.apache.iotdb.tsfile.write.page;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * This writer is used to write time-value into a page. It consists of a time encoder, a value
 * encoder and respective OutputStream.
 */
public
class PageWriter { // page写入类对象，每个Chunk都会有一个PageWriter类对象。该类有时间解码器和对应的时间写入流， 以及数据值解码器和对应的数据值写入流

  private static final Logger logger = LoggerFactory.getLogger(PageWriter.class);

  private ICompressor compressor;

  // time
  private Encoder timeEncoder; // 时间解码器
  private PublicBAOS timeOut; // 时间的写入流（输出流），它有byte型缓存数组，存放了该pageWriter要写入的时间戳
  // value
  private Encoder valueEncoder; // 数据值解码器
  private PublicBAOS valueOut; // 数据值的写入流（输出流），它有缓存数组，存放了该pageWriter要写入的数据值

  /**
   * statistic of current page. It will be reset after calling {@code
   * writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<? extends Serializable> statistics; // 当前page的statistics统计量信息

  public PageWriter() {
    this(null, null);
  }

  public PageWriter(IMeasurementSchema measurementSchema) {
    this(measurementSchema.getTimeEncoder(), measurementSchema.getValueEncoder());
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
    this.compressor = ICompressor.getCompressor(measurementSchema.getCompressor());
  }

  private PageWriter(Encoder timeEncoder, Encoder valueEncoder) {
    this.timeOut = new PublicBAOS();
    this.valueOut = new PublicBAOS();
    this.timeEncoder = timeEncoder;
    this.valueEncoder = valueEncoder;
  }

  /** write a time value pair into encoder */
  public void write(long time, boolean value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, short value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(
      long time,
      int value) { // 写入int型的数据（将经编码后的time和value分别写入到该PageWriter对象的timeOut输出流和valueOut输出流的buffer缓存里）
    timeEncoder.encode(time, timeOut); // 对time进行编码后，写入timeOut写入流的buffer缓存里
    valueEncoder.encode(value, valueOut); // 对value进行编码后，写入valueOut写入流的buffer缓存里
    statistics.update(time, value); // 每向该page插入一次数据就要更新该page相应的statistics统计量数据
  }

  /** write a time value pair into encoder */
  public void write(long time, long value) { // 写入long型的数据
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, float value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, double value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write a time value pair into encoder */
  public void write(long time, Binary value) {
    timeEncoder.encode(time, timeOut);
    valueEncoder.encode(value, valueOut);
    statistics.update(time, value);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, int[] values, int batchSize) {// 根据给定的数组，写入int型的数据（将经编码后的time和value分别写入到该PageWriter对象的timeOut输出流和valueOut输出流的buffer缓存里）
    for (int i = 0; i < batchSize; i++) {//依次遍历每个数据点
      timeEncoder.encode(timestamps[i], timeOut);// 对time进行编码后，写入timeOut写入流的buffer缓存里
      valueEncoder.encode(values[i], valueOut);// 对value进行编码后，写入valueOut写入流的buffer缓存里
    }
    statistics.update(timestamps, values, batchSize);// 每向该page插入一次数据就要更新该page相应的statistics统计量数据
  }

  /** write time series into encoder */
  public void write(long[] timestamps, long[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, float[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** flush all data remained in encoders. */
  private void prepareEndWriteOnePage()
      throws
          IOException { // 对那些还残留在该pageWriter的时间编码器和数据值编码器中的数据，把他们写入该pageWriter对应的输出流timeOut和valueOut的缓存中（而输出写入流timeOut和valueOut缓存里的数据何时flush到输出写入流指定的本地文件里取决于OutputStream的机制）
    timeEncoder.flush(timeOut);
    valueEncoder.flush(valueOut);
  }

  /**
   * getUncompressedBytes return data what it has been written in form of <code>
   * size of time list, time list, value list</code>
   *
   * @return a new readable ByteBuffer whose position is 0.
   */
  public ByteBuffer getUncompressedBytes()
      throws
          IOException { // 新建一个ByteBuffer缓存，先后往里写入该PageWriter要写入的数据点数量、所有数据点的时间戳(即该PageWriter的输出流timeOut的缓存内容)和所有数据点的值(即该PageWriter的输出流valueOut的缓存内容),并返回此ByteBuffer
    prepareEndWriteOnePage(); // 对那些还残留在该pageWriter的时间编码器和数据值编码器中的数据，把他们写入该pageWriter对应的输出流timeOut和valueOut的缓存中
    ByteBuffer buffer =
        ByteBuffer.allocate(
            timeOut.size()
                + valueOut.size()
                + 4); // 创建大小为“timeOut输出流里缓存的字节数+valueOut输出流里缓存的字节数+4”的缓存
    ReadWriteForEncodingUtils.writeUnsignedVarInt(
        timeOut.size(), buffer); // 把timeOut输出流里缓存的字节数大小的二进制格式写入buffer缓存中，返回写入的字节数量
    buffer.put(timeOut.getBuf(), 0, timeOut.size()); // 把timeout输出流的缓存数据放进buffer里
    buffer.put(valueOut.getBuf(), 0, valueOut.size()); // 把valueOut输出流的缓存数据放进buffer里
    buffer.flip();
    return buffer;
  }

  /** write the page header and data into the PageWriter's output stream. */
  public int writePageHeaderAndDataIntoBuff(
      PublicBAOS pageBuffer,
      boolean
          first) // 把该pageWriter对象暂存的数据（pageHeader和pageData，pageData需考虑是否经过压缩compress）依次写入该Chunk的ChunkWriterImpl的输出流pageBuffer的缓冲数组里（即先写入该Page的PageHeader，再写入PageData，其中pageData又依次存放了时间分量和数值分量），返回的内容是：(1)若要写入的数据所属page是Chunk的第一个page，则返回写入的pageHeader去掉statistics的字节数（2）若不是第一个page，则返回0
      throws IOException {
    if (statistics.getCount() == 0) { // 若该page的数据点个数为0，返回0
      return 0;
    }

    ByteBuffer pageData =
        getUncompressedBytes(); // 该PageWriter要写入的未压缩的数据缓存。即先后往里写入该PageWriter要写入的数据点数量、所有数据点的时间戳(即该PageWriter的输出流timeOut的缓存内容)和所有数据点的值(即该PageWriter的输出流valueOut的缓存内容)
    int uncompressedSize = pageData.remaining(); // 获取该pageData缓存里存放的数据大小，该大小就是该page还未压缩的数据大小
    int compressedSize; // 该page经压缩后的大小
    byte[] compressedBytes = null; // 存放经压缩后的数据内容

    if (compressor
        .getType()
        .equals(CompressionType.UNCOMPRESSED)) { // 若该page是压缩方式是不压缩 ，则压缩后的数据大小等于未压缩的数据大小
      compressedSize = uncompressedSize;
    } else { // 否则，计算压缩后的数据大小
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      // data is never a directByteBuffer now, so we can use data.array()
      compressedSize =
          compressor.compress( // 将pageData里未压缩的数据进行压缩并存入compressedBytes这个数组里，获得该page经压缩后的大小
              pageData.array(), pageData.position(), uncompressedSize, compressedBytes);
    }

    // write the page header to IOWriter//下面是往Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入pageHeader
    int sizeWithoutStatistic = 0;
    if (first) { // 如果该page是所属Chunk的第一个page，则（此处不用往Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入statistics）
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(
              uncompressedSize,
              pageBuffer); // 往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入uncompressedSize数据
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(
              compressedSize,
              pageBuffer); // 往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入compressedSize数据
    } else {
      ReadWriteForEncodingUtils.writeUnsignedVarInt(
          uncompressedSize,
          pageBuffer); // 往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入uncompressedSize数据
      ReadWriteForEncodingUtils.writeUnsignedVarInt(
          compressedSize,
          pageBuffer); // 往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入compressedSize数据
      statistics.serialize(
          pageBuffer); // 往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入statistics统计量数据
    }

    // write page content to temp PBAOS//下面是往Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入pageData
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    if (compressor
        .getType()
        .equals(
            CompressionType
                .UNCOMPRESSED)) { // 若数据未压缩，则往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入未压缩的数据pageData
      try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
        channel.write(pageData);
      }
    } else { // 若数据经过压缩了，则往该Chunk的ChunkWriterImpl对象的pageBuffer输出流缓存写入经压缩的数据compressedBytes
      pageBuffer.write(compressedBytes, 0, compressedSize);
    }
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    return sizeWithoutStatistic;
  }

  /**
   * calculate max possible memory size it occupies, including time outputStream and value
   * outputStream, because size outputStream is never used until flushing.
   *
   * @return allocated size in time, value and outputStream
   */
  public long estimateMaxMemSize() { // 计算该PageWriter可能占用的最大内存（timeOut缓存+valueOut缓存+timeEncoder和valueEncoder的最大大小）
    return timeOut.size()
        + valueOut.size()
        + timeEncoder.getMaxByteSize()
        + valueEncoder.getMaxByteSize();
  }

  /** reset this page */
  public void reset(
      IMeasurementSchema
          measurementSchema) { // 重置该pageWriter（清空输出流timeOut和valueOut缓存，并重置该page的statistics信息）。每往一个新的page写入数据前就要重置以下pageWriter，即写入一个新的page需要自己的新的pageWriter
    timeOut.reset();
    valueOut.reset();
    statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  public void setTimeEncoder(Encoder encoder) {
    this.timeEncoder = encoder;
  }

  public void setValueEncoder(Encoder encoder) {
    this.valueEncoder = encoder;
  }

  public void initStatistics(TSDataType dataType) {
    statistics = Statistics.getStatsByType(dataType);
  }

  public long getPointNumber() { // 获取当前page的数据点数量
    return statistics.getCount();
  }

  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }
}
