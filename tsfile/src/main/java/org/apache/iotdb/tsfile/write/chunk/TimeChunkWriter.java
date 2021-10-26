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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TimeChunkWriter { // 用作多元序列的TimeChunkWriter

  private static final Logger logger = LoggerFactory.getLogger(TimeChunkWriter.class);

  private final String measurementId; // 多元序列的传感器ID，eg:vector1

  private final TSEncoding encodingType;

  private final CompressionType compressionType;

  /** all pages of this chunk. */
  private final PublicBAOS
      pageBuffer; // 该Chunk的输出流pageBuffer，该chunk的每个page数据（pageHeader+时间戳列）会按顺序依次放入该输出流pageBuffer的缓存数组里

  private int numOfPages; //该Chunk的page数量

  /** write data into current page */
  private TimePageWriter pageWriter;

  /** page size threshold. */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  /** value count in current page. */
  private int valueCountInOnePageForNextCheck;

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /** statistic of this chunk. */
  private TimeStatistics statistics;

  /** first page info */
  private int sizeWithoutStatistic;

  private Statistics<?> firstPageStatistics;

  public TimeChunkWriter(
      String measurementId,
      CompressionType compressionType,
      TSEncoding encodingType,
      Encoder timeEncoder) {
    this.measurementId = measurementId;
    this.encodingType = encodingType;
    this.compressionType = compressionType;
    this.pageBuffer = new PublicBAOS();

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = new TimeStatistics();

    this.pageWriter = new TimePageWriter(timeEncoder, ICompressor.getCompressor(compressionType));
  }

  public void write(long time) {//往该多元传感器的TimeChunkWriter的pageWriter写入时间戳到其timeOut缓存里
    pageWriter.write(time);
  }

  public void write(long[] timestamps, int batchSize) {
    pageWriter.write(timestamps, batchSize);
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  public boolean checkPageSizeAndMayOpenANewPage() {  //根据时间分量占用的大小判断是否需要开启一个新的page
    if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {//若该page的数据点数量到达临界值，则返回真
      logger.debug("current line count reaches the upper bound, write page {}", measurementId);
      return true;
    } else if (pageWriter.getPointNumber()
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();//获取TimePageWrtier占用的可能最大内存大小，即该page的时间分量占用的大小
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        logger.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            measurementId,
            pageSizeThreshold,
            currentPageSize,
            pageWriter.getPointNumber());
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
        return true;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck =
            (int) (((float) pageSizeThreshold / currentPageSize) * pageWriter.getPointNumber());
      }
    }
    return false;
  }

  // 当TimePageWriter里暂存的数据满一页page时就会调用此方法。
  public void
      writePageToPageBuffer() { // 往对应多元Chunk的TimeChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和时间戳列（即TimePageWriter对象里输出流timeOut的缓存数据），最后重置该TimePageWriter
    try {// 此处的方法是：(1)
      // 若写入的是page是该chunk的第一个page，则先往该chunk的ChunkWriterImpl的输出流pageBuffer缓存里依次写入该page的pageHeader(去掉statistics)和pageData（2）若若写入的是page是该chunk的第二个page，则重置pageBuffer，然后依次写入第一个page的pageHeader(包含statistics)和时间戳列，然后再写入该第二个page的pageHeader(包含statistics)和时间戳列（3）其余的page，就往所属chunk的ChunkWriterImpl的输出流pageBuffer缓存里依次写入该page的pageHeader(包含statistics)和时间戳列。 这样做的目的是若该Chunk只有一个page，则此page里无需再存储自己的statistics,因为chunk里会存储。
      if (numOfPages == 0) { // record the firstPageStatistics
        this.firstPageStatistics = pageWriter.getStatistics();
        this.sizeWithoutStatistic =
            pageWriter.writePageHeaderAndDataIntoBuff(
                pageBuffer,
                true); // 把该pageWriter对象的数据（pageHeader和时间戳列，时间戳列需考虑是否经过压缩compress）依次写入该多元Chunk的TimeChunkWriter的输出流pageBuffer的缓冲数组里（即先写入该Page的PageHeader，再写入时间戳列，即该多元传感器的时间分量），返回的内容是：(1)若要写入的数据所属page是Chunk的第一个page，则返回写入的pageHeader去掉statistics的字节数（2）若不是第一个page，则返回0
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
      pageWriter.reset();
    }
  }

  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException { //1. 封口当前TimePage的内容（即TimePageWriter对象里输出流timeOut的缓存数据）到TimeChunkWriter的输出流pageBuffer缓存里 2. 把当前Chunk的ChunkHeader和TimeChunkWriter里pageBuffer缓存的内容（该chunk所有page的pageHeader+时间戳列）写到该TsFileIOWriter对象的out缓存里 ,并初始化当前Chunk的ChunkIndex对象并放到TsFileIOWriter里
    sealCurrentPage();//封口TimeChunkWriter的当前PageWriter的内容，即往对应多元Chunk的TimeChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和时间戳列（即TimePageWriter对象里输出流timeOut的缓存数据），最后重置该TimePageWriter
    writeAllPagesOfChunkToTsFile(tsfileWriter);//1. 初始化当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的out缓存里 2. 把当前TimeChunkWriter里pageBuffer缓存的内容（该chunk所有page的pageHeader+时间戳列）写到该TsFileIOWriter对象的out缓存里 3. 往TsFileIOWriter的当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的ChunkIndex对象，并把当前Chunk元数据对象清空

    // reinit this chunk writer
    pageBuffer.reset();
    numOfPages = 0;
    firstPageStatistics = null;
    this.statistics = new TimeStatistics();
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

  public void sealCurrentPage() {//封口TimeChunkWriter的当前PageWriter的内容，即往对应多元Chunk的TimeChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和时间戳列（即TimePageWriter对象里输出流timeOut的缓存数据），最后重置该TimePageWriter
    if (pageWriter != null && pageWriter.getPointNumber() > 0) {
      writePageToPageBuffer();// 往对应多元Chunk的TimeChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和时间戳列（即TimePageWriter对象里输出流timeOut的缓存数据），最后重置该TimePageWriter
    }
  }

  public void clearPageWriter() {
    pageWriter = null;
  }

  public int getNumOfPages() {
    return numOfPages;
  }

  public TSDataType getDataType() {
    return TSDataType.VECTOR;
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @throws IOException exception in IO
   */
  public void writeAllPagesOfChunkToTsFile(TsFileIOWriter writer) throws IOException {
    if (statistics.getCount() == 0) {//1. 初始化当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的out缓存里 2. 把当前TimeChunkWriter里pageBuffer缓存的内容（所有page的pageHeader+时间戳列）写到该TsFileIOWriter对象的out缓存里 3. 往TsFileIOWriter的当前写操作的ChunkGroup对应的所有ChunkIndex类对象列表里加入当前写完的currentChunkMetadata对象，并把当前Chunk元数据对象清空
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(// 初始化该TsFileIOWriter的当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
        measurementId,
        compressionType,
        TSDataType.VECTOR,
        encodingType,
        statistics,
        pageBuffer.size(),
        numOfPages,
        TsFileConstant.TIME_COLUMN_MASK);

    long dataOffset = writer.getPos();

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);//把当前TimeChunkWriter里pageBuffer缓存的内容写到该TsFileIOWriter对象的out缓存里

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
