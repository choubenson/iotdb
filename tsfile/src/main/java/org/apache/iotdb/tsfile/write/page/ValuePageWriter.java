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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * This writer is used to write value into a page. It consists of a value encoder and respective
 * OutputStream.
 */
public class ValuePageWriter {
  private static final Logger logger = LoggerFactory.getLogger(ValuePageWriter.class);

  private final ICompressor compressor;

  // value
  private Encoder valueEncoder;
  private final PublicBAOS valueOut;

  /**
   * statistic of current page. It will be reset after calling {@code
   * writePageHeaderAndDataIntoBuff()}
   */
  private Statistics<? extends Serializable> statistics;

  private byte bitmap;

  private int size;

  private final PublicBAOS bitmapOut;

  private static final int MASK = 1 << 7;

  public ValuePageWriter(Encoder valueEncoder, ICompressor compressor, TSDataType dataType) {
    this.valueOut = new PublicBAOS();
    this.bitmap = 0;
    this.size = 0;
    this.bitmapOut = new PublicBAOS();
    this.valueEncoder = valueEncoder;
    this.statistics = Statistics.getStatsByType(dataType);
    this.compressor = compressor;
  }

  /** write a time value pair into encoder */
  public void write(long time, boolean value, boolean isNull) {
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);
      statistics.update(time, value);
    }
  }

  /** write a time value pair into encoder */
  public void write(long time, short value, boolean isNull) {
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);
      statistics.update(time, value);
    }
  }

  /** write a time value pair into encoder */
  public void write(long time, int value, boolean isNull) { //第三个参数是判断该多元传感器的该分量是不是为null
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);// 对value进行编码后，写入valueOut写入流的buffer缓存里
      statistics.update(time, value);// 每向该page插入一次数据就要更新该page相应的statistics统计量数据
    }
  }

  /** write a time value pair into encoder */
  public void write(long time, long value, boolean isNull) {
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);
      statistics.update(time, value);
    }
  }

  /** write a time value pair into encoder */
  public void write(long time, float value, boolean isNull) {
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);
      statistics.update(time, value);
    }
  }

  /** write a time value pair into encoder */
  public void write(long time, double value, boolean isNull) {
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);
      statistics.update(time, value);
    }
  }

  /** write a time value pair into encoder */
  public void write(long time, Binary value, boolean isNull) {
    setBit(isNull);
    if (!isNull) {
      valueEncoder.encode(value, valueOut);
      statistics.update(time, value);
    }
  }

  private void setBit(boolean isNull) {
    if (!isNull) {
      bitmap |= (MASK >>> (size % 8));
    }
    size++;
    if (size % 8 == 0) {
      bitmapOut.write(bitmap);
      bitmap = 0;
    }
  }

  /** write time series into encoder */
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, int[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, long[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, float[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, double[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** write time series into encoder */
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      valueEncoder.encode(values[i], valueOut);
    }
    statistics.update(timestamps, values, batchSize);
  }

  /** flush all data remained in encoders. */
  private void prepareEndWriteOnePage() throws IOException {
    valueEncoder.flush(valueOut);
    if (size % 8 != 0) {//把bitmap数组中还未写到bitmapOut缓存流的数据写入
      bitmapOut.write(bitmap);
    }
  }

  /**
   * getUncompressedBytes return data what it has been written in form of <code>
   * size of time list, time list, value list</code>
   *
   * @return a new readable ByteBuffer whose position is 0.
   */
  public ByteBuffer getUncompressedBytes() throws IOException {
    prepareEndWriteOnePage(); // 对那些还残留在该ValuePageWriter的数值编码器中的数据，把他们写入该ValuePageWriter对应的输出流valueOut的缓存中（而输出写入流valueOut缓存里的数据何时flush到输出写入流指定的本地文件里取决于OutputStream的机制）
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + bitmapOut.size() + valueOut.size());
    buffer.putInt(size);
    buffer.put(bitmapOut.getBuf(), 0, bitmapOut.size());
    buffer.put(valueOut.getBuf(), 0, valueOut.size());
    buffer.flip();
    return buffer;
  }

  /** write the page header and data into the PageWriter's output stream. */
  public int writePageHeaderAndDataIntoBuff(PublicBAOS pageBuffer, boolean first)
      throws
          IOException { // 把该pageWriter对象暂存的数据（pageHeader和该子分量的内容bitmapOut和valueOut，需考虑是否经过压缩compress）依次写入该Chunk的TimeChunkWriter的输出流pageBuffer的缓冲数组里（即先写入该Page的PageHeader，再写入该子分量的内容bitmapOut和valueOut），返回的内容是：(1)若要写入的数据所属page是Chunk的第一个page，则返回写入的pageHeader去掉statistics的字节数（2）若不是第一个page，则返回0
    if (size == 0) {
      return 0;
    }

    ByteBuffer pageData = getUncompressedBytes();// 新建一个ByteBuffer，并先后往里写入该ValuePageWriter的bitmapOut和valueOut缓存流里的内容，并返回此ByteBuffer
    int uncompressedSize = pageData.remaining();
    int compressedSize;
    byte[] compressedBytes = null;

    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      compressedSize = uncompressedSize;
    } else {
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      // data is never a directByteBuffer now, so we can use data.array()
      compressedSize =//将未经压缩的该多元传感器的bitmapOut和valueOut内容进行压缩到compressedBytes数组里
          compressor.compress(
              pageData.array(), pageData.position(), uncompressedSize, compressedBytes);
    }

    // write the page header to IOWriter
    int sizeWithoutStatistic = 0;
    if (first) { // 如果该page是所属Chunk的第一个page，则（此处不用往Chunk的TimeChunkWriter对象的pageBuffer输出流缓存写入statistics）
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      sizeWithoutStatistic +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
    } else {
      ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
      statistics.serialize(pageBuffer); // 往该Chunk的TimeChunkWriter对象的pageBuffer输出流缓存写入statistics统计量数据
    }

    // write page content to temp PBAOS
    logger.trace("start to flush a page data into buffer, buffer position {} ", pageBuffer.size());
    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
        channel.write(pageData);
      }
    } else {
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
  public long estimateMaxMemSize() {
    return Integer.BYTES + bitmapOut.size() + 1 + valueOut.size() + valueEncoder.getMaxByteSize();
  }

  /** reset this page */
  public void reset(TSDataType dataType) {
    bitmapOut.reset();
    size = 0;
    bitmap = 0;
    valueOut.reset();
    statistics = Statistics.getStatsByType(dataType);
  }

  public void setValueEncoder(Encoder encoder) {
    this.valueEncoder = encoder;
  }

  public void initStatistics(TSDataType dataType) {
    statistics = Statistics.getStatsByType(dataType);
  }

  public long getPointNumber() {
    return statistics.getCount();
  }

  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public int getSize() {
    return size;
  }
}
