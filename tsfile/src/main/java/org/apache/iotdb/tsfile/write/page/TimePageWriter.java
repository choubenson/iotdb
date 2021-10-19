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
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * This writer is used to write time into a page. It consists of a time encoder and respective
 * OutputStream.
 */
public class TimePageWriter {

  private static final Logger logger = LoggerFactory.getLogger(TimePageWriter.class);

  private final ICompressor compressor;

  // time
  private Encoder timeEncoder;
  private final PublicBAOS timeOut; //时间流缓存

  /**
   * statistic of current page. It will be reset after calling {@code
   * writePageHeaderAndDataIntoBuff()}
   */
  private TimeStatistics statistics;

  public TimePageWriter(Encoder timeEncoder, ICompressor compressor) {
    this.timeOut = new PublicBAOS();
    this.timeEncoder = timeEncoder;
    this.statistics = new TimeStatistics();
    this.compressor = compressor;
  }

  /** write a time into encoder */
  public void write(long time) {
    timeEncoder.encode(time, timeOut);// 对time进行编码后，写入timeOut写入流的buffer缓存里
    statistics.update(time);// 每向该page插入一次数据就要更新该page相应的statistics统计量数据
  }

  /** write time series into encoder */
  public void write(long[] timestamps, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      timeEncoder.encode(timestamps[i], timeOut);
    }
    statistics.update(timestamps, batchSize);
  }

  /** flush all data remained in encoders. */
  private void prepareEndWriteOnePage()
      throws
          IOException { // 对那些还残留在该TimePageWriter的时间编码器中的数据，把他们写入该pageWriter对应的输出流timeOut的缓存中（而输出写入流timeOut和valueOut缓存里的数据何时flush到输出写入流指定的本地文件里取决于OutputStream的机制）
    timeEncoder.flush(timeOut);
  }

  /**
   * getUncompressedBytes return data what it has been written in form of <code>
   * size of time list, time list, value list</code>
   *
   * @return a new readable ByteBuffer whose position is 0.
   */
  public ByteBuffer getUncompressedBytes()
      throws IOException { // 新建一个ByteBuffer，并往里写入该TimePageWriter的timeOut缓存流里的内容，并返回此ByteBuffer
    prepareEndWriteOnePage(); // 对那些还残留在该TimePageWriter的时间编码器中的数据，把他们写入该pageWriter对应的输出流timeOut的缓存中（而输出写入流timeOut缓存里的数据何时flush到输出写入流指定的本地文件里取决于OutputStream的机制）
    ByteBuffer buffer = ByteBuffer.allocate(timeOut.size()); // 创建大小为"timeOut输出流里缓存的字节数"的缓存
    buffer.put(timeOut.getBuf(), 0, timeOut.size()); // 往该缓存里放入timeOut缓存流里的内容
    buffer.flip();
    return buffer;
  }

  /** write the page header and data into the PageWriter's output stream. */
  public int writePageHeaderAndDataIntoBuff(PublicBAOS pageBuffer, boolean first)
      throws
          IOException { // 把该pageWriter对象的数据（pageHeader和时间戳列，时间戳列需考虑是否经过压缩compress）依次写入该Chunk的TimeChunkWriter的输出流pageBuffer的缓冲数组里（即先写入该Page的PageHeader，再写入时间戳列，即该多元传感器的时间分量），返回的内容是：(1)若要写入的数据所属page是Chunk的第一个page，则返回写入的pageHeader去掉statistics的字节数（2）若不是第一个page，则返回0
    if (statistics.getCount() == 0) { // 若该page的数据点个数为0，返回0
      return 0;
    }

    ByteBuffer pageData =
        getUncompressedBytes(); // 新建一个ByteBuffer，并往里写入该TimePageWriter的timeOut缓存流里的内容，并返回此ByteBuffer
    int uncompressedSize = pageData.remaining(); // 获取该pageData缓存里存放的数据大小，该大小就是该page还未压缩的数据大小
    int compressedSize; // 该page经压缩后的大小
    byte[] compressedBytes = null; // 存放经压缩后的数据内容，即经压缩的time时间戳timeOut内容

    if (compressor
        .getType()
        .equals(CompressionType.UNCOMPRESSED)) { // 若该page是压缩方式是不压缩 ，则压缩后的数据大小等于未压缩的数据大小
      compressedSize = uncompressedSize;
    } else {
      compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      // data is never a directByteBuffer now, so we can use data.array()
      compressedSize =        //将未经压缩的该多元传感器的timeOut内容进行压缩到compressedBytes数组里
          compressor.compress(
              pageData.array(), pageData.position(), uncompressedSize, compressedBytes);
    }

    // write the page header to IOWriter
    int sizeWithoutStatistic = 0;
    if (first) { // 如果该page是所属Chunk的第一个page，则（此处不用往Chunk的TimeChunkWriter对象的pageBuffer输出流缓存写入statistics）
      sizeWithoutStatistic += // 往该Chunk的TimeChunkWriter对象的pageBuffer输出流缓存写入变量uncompressedSize数据
          ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      sizeWithoutStatistic += // 往该Chunk的TimeChunkWriter对象的pageBuffer输出流缓存写入变量compressedSize数据
          ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
    } else {
      ReadWriteForEncodingUtils.writeUnsignedVarInt(uncompressedSize, pageBuffer);
      ReadWriteForEncodingUtils.writeUnsignedVarInt(compressedSize, pageBuffer);
      statistics.serialize(
          pageBuffer); // 往该Chunk的TimeChunkWriter对象的pageBuffer输出流缓存写入statistics统计量数据
    }

    // write page content to temp PBAOS
    logger.trace(
        "start to flush a time page data into buffer, buffer position {} ", pageBuffer.size());
    if (compressor
        .getType()
        .equals(
            CompressionType
                .UNCOMPRESSED)) { // 若数据未压缩，则直接把pageData(其实只是时间分量)写入TimeChunkWriter的pageBuffer缓存里
      try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
        channel.write(pageData);
      }
    } else { // 若数据经压缩，则把压缩的timeOut内容写入pageBuffer里
      pageBuffer.write(compressedBytes, 0, compressedSize);
    }
    logger.trace(
        "finish flushing a time page data into buffer, buffer position {} ", pageBuffer.size());
    return sizeWithoutStatistic;
  }

  /**
   * calculate max possible memory size it occupies, including time outputStream and value
   * outputStream, because size outputStream is never used until flushing.
   *
   * @return allocated size in time, value and outputStream
   */
  public long estimateMaxMemSize() {//获取TimePageWrtier占用的可能最大内存大小
    return timeOut.size() + timeEncoder.getMaxByteSize();
  }

  /** reset this page */
  public void reset() {
    timeOut.reset();
    statistics = new TimeStatistics();
  }

  public void setTimeEncoder(Encoder encoder) {
    this.timeEncoder = encoder;
  }

  public void initStatistics() {
    statistics = new TimeStatistics();
  }

  public long getPointNumber() {
    return statistics.getCount();
  }

  public TimeStatistics getStatistics() {
    return statistics;
  }
}
