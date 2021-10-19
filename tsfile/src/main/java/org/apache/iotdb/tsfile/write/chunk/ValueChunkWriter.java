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
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class ValueChunkWriter { //一个多元传感器里每个子分量（不含时间）会有自己的ValueChunkWriter和对应的ValuePageWriter,因为数据类型等属性都不一样

  private static final Logger logger = LoggerFactory.getLogger(ValueChunkWriter.class);

  private final String measurementId; //

  private final TSEncoding encodingType;

  private final TSDataType dataType;

  private final CompressionType compressionType;

  /** all pages of this chunk. */
  private final PublicBAOS pageBuffer;//存放了该ValueChunk的所有ValuePage的内容（即pageHeader和该子分量的内容bitmapOut和valueOut）

  private int numOfPages;

  /** write data into current page */
  private ValuePageWriter pageWriter;

  /** statistic of this chunk. */
  private Statistics<? extends Serializable> statistics;

  /** first page info */
  private int sizeWithoutStatistic;

  private Statistics<?> firstPageStatistics;

  public ValueChunkWriter(
      String measurementId,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encodingType,
      Encoder valueEncoder) {
    this.measurementId = measurementId;
    this.encodingType = encodingType;
    this.dataType = dataType;
    this.compressionType = compressionType;
    this.pageBuffer = new PublicBAOS();

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(dataType);

    this.pageWriter =
        new ValuePageWriter(valueEncoder, ICompressor.getCompressor(compressionType), dataType);
  }

  public void write(long time, long value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, int value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, boolean value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, float value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, double value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long time, Binary value, boolean isNull) {
    pageWriter.write(time, value, isNull);
  }

  public void write(long[] timestamps, int[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, long[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, boolean[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, float[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, double[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void write(long[] timestamps, Binary[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
  }

  public void writePageToPageBuffer() {// 往对应多元传感器Chunk的该子分量的ValueChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和该子分量的数值内容（即对应ValuePageWriter里的bitmapOut和valueOut缓存），最后重置该TimePageWriter
    try {
      if (numOfPages == 0) { // record the firstPageStatistics
        this.firstPageStatistics = pageWriter.getStatistics();
        this.sizeWithoutStatistic = pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, true);// 把该pageWriter对象暂存的数据（pageHeader和该子分量的内容bitmapOut和valueOut，需考虑是否经过压缩compress）依次写入该Chunk的TimeChunkWriter的输出流pageBuffer的缓冲数组里（即先写入该Page的PageHeader，再写入该子分量的内容bitmapOut和valueOut），返回的内容是：(1)若要写入的数据所属page是Chunk的第一个page，则返回写入的pageHeader去掉statistics的字节数（2）若不是第一个page，则返回0
      } else if (numOfPages == 1) { // put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray();
        pageBuffer.reset();
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);
        firstPageStatistics.serialize(pageBuffer);
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
        firstPageStatistics = null;
      } else {
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);
      }

      // update statistics of this chunk
      numOfPages++;
      this.statistics.mergeStatistics(pageWriter.getStatistics());
    } catch (IOException e) {
      logger.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      pageWriter.reset(dataType);
    }
  }

  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {//1. 封口当前子传感器的ValuePage的内容（即ValuePageWriter对象里输出流bitmapOut和valueOut的缓存数据）到此ValueChunkWriter的输出流pageBuffer缓存里 2. 把当前ValueChunk的ChunkHeader和ValueChunkWriter里pageBuffer缓存的内容（该chunk所有page的pageHeader+bitmapOut和valueOut）写到该TsFileIOWriter对象的out缓存里 ,并初始化当前Chunk的ChunkIndex对象并放到TsFileIOWriter里
    sealCurrentPage();// 往对应多元传感器Chunk的该子分量的ValueChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和该子分量的数值内容（即对应ValuePageWriter里的bitmapOut和valueOut缓存），最后重置该TimePageWriter
    writeAllPagesOfChunkToTsFile(tsfileWriter);//1. 初始化当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的out缓存里 2. 把当前ValueChunkWriter里pageBuffer缓存的内容（所有page的pageHeader+bitmapOut和valueOut）写到该TsFileIOWriter对象的out缓存里 3. 往TsFileIOWriter的当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的ChunkIndex对象，并把当前Chunk元数据对象清空

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
    firstPageStatistics = null;
    this.statistics = Statistics.getStatsByType(dataType);
  }

  public long estimateMaxSeriesMemSize() {
    return pageBuffer.size()
        + pageWriter.estimateMaxMemSize()
        + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        + pageWriter.getStatistics().getSerializedSize();
  }

  public long getCurrentChunkSize() {
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementId, pageBuffer.size())
        + (long) pageBuffer.size();
  }

  public void sealCurrentPage() {
    // if the page contains no points, we still need to serialize it
    if (pageWriter != null && pageWriter.getSize() != 0) {
      writePageToPageBuffer();// 往对应多元传感器Chunk的该子分量的ValueChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和该子分量的数值内容（即对应ValuePageWriter里的bitmapOut和valueOut缓存），最后重置该TimePageWriter
    }
  }

  public void clearPageWriter() {
    pageWriter = null;
  }

  public int getNumOfPages() {
    return numOfPages;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @throws IOException exception in IO
   *///1. 初始化当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的out缓存里 2. 把当前ValueChunkWriter里pageBuffer缓存的内容（所有page的pageHeader+bitmapOut和valueOut）写到该TsFileIOWriter对象的out缓存里 3. 往TsFileIOWriter的当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的ChunkIndex对象，并把当前Chunk元数据对象清空
  public void writeAllPagesOfChunkToTsFile(TsFileIOWriter writer) throws IOException {
    if (statistics.getCount() == 0) {
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(// 初始化该TsFileIOWriter的当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
        measurementId,
        compressionType,
        dataType,
        encodingType,
        statistics,
        pageBuffer.size(),
        numOfPages,
        TsFileConstant.VALUE_COLUMN_MASK);

    long dataOffset = writer.getPos();

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);//把当前ValueChunkWriter里pageBuffer缓存的内容写到该TsFileIOWriter对象的out缓存里

    int dataSize = (int) (writer.getPos() - dataOffset);
    if (dataSize != pageBuffer.size()) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: "
              + dataSize
              + " !="
              + " "
              + pageBuffer.size());
    }

    writer.endCurrentChunk();// 结束当前Chunk的写操作后就会调用此方法，往TsFileIOWriter的当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的ChunkIndex对象，并把当前Chunk元数据对象清空
  }

  /** only used for test */
  public PublicBAOS getPageBuffer() {
    return pageBuffer;
  }
}
