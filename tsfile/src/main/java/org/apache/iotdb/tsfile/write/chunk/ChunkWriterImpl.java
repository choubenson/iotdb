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
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class ChunkWriterImpl implements IChunkWriter {  //Chunk写入接口的具体实现类 //每个时间分区TsFile对应着一个ChunkWriterImpl类对象

  private static final Logger logger = LoggerFactory.getLogger(ChunkWriterImpl.class);

  private IMeasurementSchema measurementSchema; //该Chunk传感器的配置类对象

  private ICompressor compressor;   //压缩器类对象

  /** all pages of this chunk. */
  private PublicBAOS pageBuffer;  //该Chunk的输出流pageBuffer，该chunk的每个page数据（pageHeader+pageData）会按顺序依次放入该输出流pageBuffer的缓存数组里

  private int numOfPages;   //该Chunk的page数量

  /** write data into current page */
  private PageWriter pageWriter;    //使用PageWriter类对象可以把数据写入当前Chunk的page里

  /** page size threshold. */
  private final long pageSizeThreshold;   //page大小临界点

  private final int maxNumberOfPointsInPage;  //page的最大数据点数量

  /** value count in current page. */
  private int valueCountInOnePageForNextCheck;

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /** statistic of this chunk. */
  private Statistics<?> statistics;

  /** SDT parameters */
  private boolean isSdtEncoding;  //该Chunk是否是SDT的编码方式
  // When the ChunkWriter WILL write the last data point in the chunk, set it to true to tell SDT
  // saves the point.
  private boolean isLastPoint;    //是否是当前Chunk的最后一个写入的数据点
  // do not re-execute SDT compression when merging chunks
  private boolean isMerging;
  private SDTEncoder sdtEncoder;    //SDT编码器

  private static final String LOSS = "loss";
  private static final String SDT = "sdt";
  private static final String SDT_COMP_DEV = "compdev";
  private static final String SDT_COMP_MIN_TIME = "compmintime";
  private static final String SDT_COMP_MAX_TIME = "compmaxtime";

  /** first page info */
  private int sizeWithoutStatistic;   //存放pageHeader里除去statistics后的字节大小（即UncompressedSize+compressedSize两个属性的字节大小）

  private Statistics<?> firstPageStatistics;  //该Chunk的第一个page的Statistics

  /** @param schema schema of this measurement */
  public ChunkWriterImpl(IMeasurementSchema schema) {
    this.measurementSchema = schema;
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    this.pageBuffer = new PublicBAOS();

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new PageWriter(measurementSchema);

    this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());

    // check if the measurement schema uses SDT
    checkSdtEncoding();
  }

  public ChunkWriterImpl(IMeasurementSchema schema, boolean isMerging) {
    this(schema);
    this.isMerging = isMerging;
  }

  private void checkSdtEncoding() {
    if (measurementSchema.getProps() != null && !isMerging) {
      if (measurementSchema.getProps().getOrDefault(LOSS, "").equals(SDT)) {
        isSdtEncoding = true;
        sdtEncoder = new SDTEncoder();
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_DEV)) {
        sdtEncoder.setCompDeviation(
            Double.parseDouble(measurementSchema.getProps().get(SDT_COMP_DEV)));
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_MIN_TIME)) {
        sdtEncoder.setCompMinTime(
            Long.parseLong(measurementSchema.getProps().get(SDT_COMP_MIN_TIME)));
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey(SDT_COMP_MAX_TIME)) {
        sdtEncoder.setCompMaxTime(
            Long.parseLong(measurementSchema.getProps().get(SDT_COMP_MAX_TIME)));
      }
    }
  }

  @Override
  public void write(long time, long value, boolean isNull) {
    // store last point for sdtEncoding, it still needs to go through encoding process
    // in case it exceeds compdev and needs to store second last point
    if (!isSdtEncoding || sdtEncoder.encodeLong(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getLongValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, int value, boolean isNull) {  //将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    if (!isSdtEncoding || sdtEncoder.encodeInt(time, value)) {    //如果该Chunk不是SDT编码的 或者
      pageWriter.write(   //将给定的数据点交由该Chunk的pageWriter进行写入page(具体操作是将编码后的time和value写入pageWriter对象里的输出流timeOut和valueOut的缓存buffer中)
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getIntValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {//若当前Chunk是SDT编码并且写入的是最后一个数据点了，则
      pageWriter.write(time, value);//将给定的数据点交由该Chunk的pageWriter进行写入page(具体操作是将未编码的time和value写入pageWriter对象里的输出流timeOut和valueOut的缓存buffer中)
    }
    checkPageSizeAndMayOpenANewPage(); //检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
  }

  @Override
  public void write(long time, boolean value, boolean isNull) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, float value, boolean isNull) {
    if (!isSdtEncoding || sdtEncoder.encodeFloat(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getFloatValue() : value);
    }
    // store last point for sdt encoding
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, double value, boolean isNull) {
    if (!isSdtEncoding || sdtEncoder.encodeDouble(time, value)) {
      pageWriter.write(
          isSdtEncoding ? sdtEncoder.getTime() : time,
          isSdtEncoding ? sdtEncoder.getDoubleValue() : value);
    }
    if (isSdtEncoding && isLastPoint) {
      pageWriter.write(time, value);
    }
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, Binary value, boolean isNull) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time) {
    throw new IllegalStateException("write time method is not implemented in common chunk writer");
  }

  @Override
  public void write(long[] timestamps, int[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, long[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, float[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, double[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private void checkPageSizeAndMayOpenANewPage() {  //检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则要把当前Chunk的pageWriter里两个输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
    if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) { //若该Chunk的pageWriter对象的缓存存放的数据点数量到达系统规定一个page数据点的最大值，则writePageToPageBuffer
      logger.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      writePageToPageBuffer();//往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    } else if (pageWriter.getPointNumber()        //若当前page的数据点数量到达某一值，需要检查该page占用的内存量是否到达系统设定的临界值
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();//计算该PageWriter可能占用的最大内存
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold//若该PageWriter可能占用的最大内存超过系统设定的临界值，则需要writePageToPageBuffer
        // we will write the current page
        logger.debug(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            measurementSchema.getMeasurementId(),
            pageSizeThreshold,
            currentPageSize,
            pageWriter.getPointNumber());
        writePageToPageBuffer();//往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {  //若当前Chunk的pageWriter的数据点数量还很少(没有满一页or占用内存没满)，则还不需要writePageToPageBuffer
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck =
            (int) (((float) pageSizeThreshold / currentPageSize) * pageWriter.getPointNumber());
      }
    }
  }

  //往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
  private void writePageToPageBuffer() {  //当PageWriter里暂存的数据满一页page时就会调用此方法。即先把该page的pageHeader写到对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里，然后该pageWriter对象里输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里，最后重置该pageWriter
    try { //此处的方法是：(1) 若写入的是page是该chunk的第一个page，则先往该chunk的ChunkWriterImpl的输出流pageBuffer缓存里依次写入该page的pageHeader(去掉statistics)和pageData（2）若若写入的是page是该chunk的第二个page，则重置pageBuffer，然后依次写入第一个page的pageHeader(包含statistics)和pageData，然后再写入该第二个page的pageHeader(包含statistics)和pageData（3）其余的page，就往所属chunk的ChunkWriterImpl的输出流pageBuffer缓存里依次写入该page的pageHeader(包含statistics)和pageData。 这样做的目的是若该Chunk只有一个page，则此page里无需再存储自己的statistics,因为chunk里会存储。
      if (numOfPages == 0) { // record the firstPageStatistics    //如果在该写操作之前，该Chunk还没有任何page，则把该page的pageHeader<去掉statistics>和pageData写入pageBuffer输出流缓存中
        this.firstPageStatistics = pageWriter.getStatistics();  //通过该Chunk的pageWriter来记录当前Chunk的第一个page的statistics统计量
        this.sizeWithoutStatistic = pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, true);//使用该Chunk的pageWriter对象把其暂存的数据（pageHeader<去掉statistics>和pageData，pageData需考虑是否经过压缩compress）写入该Chunk的ChunkWriterImpl的输出流pageBuffer的缓冲数组里，返回的内容是：(1)若要写入的数据所属page是Chunk的第一个page，则返回写入的pageHeader去掉statistics的字节数（2）若不是第一个page，则返回0
      } else if (numOfPages == 1) { //如果已经存在一个page,则 put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray();    //获取pageBuffer输出流里的缓存数组
        pageBuffer.reset(); //将输出流pageBuffer重置，即只需要设置其存放的数据点数量是0，而其buf则后续覆盖写入就可以，无需再重置操作
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);  //往重置后的pageBuffer输出流的缓存里写入第一个page的pageHeader(去掉statistics)（即将数组b第0个位置开始，共计sizeWithoutStatistic个字节的数据写入到重置后的pageBuffer里）
        firstPageStatistics.serialize(pageBuffer); //将第一个page的统计量的相关属性序列化存入该输出流pageBuffer的缓存数组里
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);//再往pageBuffer写入第一个page的pageData  (即数组b的剩余数据）
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);//把该第二个page的pageHeader（包含statistics）和pageData写入该page所属Chunk的ChunkWriterImpl对象的输出流pageBuffer的缓冲数组里
        firstPageStatistics = null;
      } else {
        pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer, false);//把该page的pageHeader和pageData写入该pageWriter的输出流pageBuffer的缓冲数组里
      }

      // update statistics of this chunk
      numOfPages++; //将该Chunk的page数量+1
      this.statistics.mergeStatistics(pageWriter.getStatistics());  //每往该Chunk放入一个新的page数据，就要更新该Chunk的statistics信息
    } catch (IOException e) {
      logger.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      pageWriter.reset(measurementSchema);//重置该pageWriter（清空输出流timeOut和valueOut缓存，并重置该page的statistics信息）。每往一个新的page写入数据前就要重置以下pageWriter，即写入一个新的page需要自己的新的pageWriter
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {// 首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
    sealCurrentPage(); //关闭、封口当前pageWriter(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)
    writeAllPagesOfChunkToTsFile(tsfileWriter, statistics);//首先往TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,然后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）

    // reinit this chunk writer
    pageBuffer.reset(); //写完当前Chunk的所有page数据后，重置该Chunk的输出流pageBuffer
    numOfPages = 0; //设置当前Chunk的page数量为0
    firstPageStatistics = null; //重置
    this.statistics = Statistics.getStatsByType(measurementSchema.getType()); //重置
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageBuffer.size()
        + pageWriter.estimateMaxMemSize()
        + PageHeader.estimateMaxPageHeaderSizeWithoutStatistics()
        + pageWriter.getStatistics().getSerializedSize();
  }

  @Override
  public long getCurrentChunkSize() {
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId(), pageBuffer.size())
        + (long) pageBuffer.size();
  }

  @Override
  public void sealCurrentPage() {   //关闭、封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)
    if (pageWriter != null && pageWriter.getPointNumber() > 0) {  //若该Chunk的pageWriter不为空并且pageWriter的输出流数据点数量大于0
      writePageToPageBuffer();  //则把该Chunk的pageWriter对象里输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter
    }
  }

  @Override
  public void clearPageWriter() {
    pageWriter = null;
  }

  @Override
  public int getNumOfPages() {
    return numOfPages;
  }

  @Override
  public TSDataType getDataType() {
    return measurementSchema.getType();
  }

  /**
   * write the page header and data into the PageWriter's output stream. @NOTE: for upgrading
   * 0.11/v2 to 0.12/v3 TsFile
   */
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)    //往当前Chunk的ChunkWriterImpl对象的输出流pageBuffer里写入page数据（pageData+pageHeader，此处pageData是存放在二进制缓存ByteBuffer对象里）
      throws PageException {
    //此处的方法是：(1) 若写入的是page是该chunk的第一个page，则先往该chunk的ChunkWriterImpl的输出流pageBuffer缓存里依次写入该page的pageHeader(去掉statistics)和pageData（2）若若写入的是page是该chunk的第二个page，则重置pageBuffer，然后依次写入第一个page的pageHeader(包含statistics)和pageData，然后再写入该第二个page的pageHeader(包含statistics)和pageData（3）其余的page，就往所属chunk的ChunkWriterImpl的输出流pageBuffer缓存里依次写入该page的pageHeader(包含statistics)和pageData。 这样做的目的是若该Chunk只有一个page，则此page里无需再存储自己的statistics,因为chunk里会存储。
    // write the page header to pageBuffer
    try {
      logger.debug(
          "start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      // serialize pageHeader  see writePageToPageBuffer method //首先写pageHeader到输出流pageBuffer
      if (numOfPages == 0) { // record the firstPageStatistics //如果在该写操作之前，该Chunk还没有任何page，则把该page的pageHeader<去掉statistics>和pageData写入pageBuffer输出流缓存中
        this.firstPageStatistics = header.getStatistics();//获取第一个page的staistics
        this.sizeWithoutStatistic +=  //往该Chunk的ChunkWriterImpl对象的输出流pageBuffer的缓存里写入pageHeader的UncompressedSize属性
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);
        this.sizeWithoutStatistic +=    //往该Chunk的ChunkWriterImpl对象的输出流pageBuffer的缓存里写入pageHeader的compressedSize属性
            ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);
      } else if (numOfPages == 1) { //如果已经存在一个page,则 put the firstPageStatistics into pageBuffer
        byte[] b = pageBuffer.toByteArray(); //获取pageBuffer输出流里的缓存数组
        pageBuffer.reset();//重置输出流pageBuffer
        pageBuffer.write(b, 0, this.sizeWithoutStatistic);//往重置后的pageBuffer输出流的缓存里写入第一个page的pageHeader(去掉statistics)（即将数组b第0个位置开始，共计sizeWithoutStatistic个字节的数据写入到重置后的pageBuffer里）
        firstPageStatistics.serialize(pageBuffer);//将第一个page的统计量的相关属性序列化存入该输出流pageBuffer的缓存数组里
        pageBuffer.write(b, this.sizeWithoutStatistic, b.length - this.sizeWithoutStatistic);//再往pageBuffer写入第一个page的pageData  (即数组b的剩余数据）
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);  //往pageBuffer写入该第二个page的pageHeader里的UncompressedSize属性
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);//往pageBuffer写入该第二个page的pageHeader里的compressedSize属性
        header.getStatistics().serialize(pageBuffer);//往pageBuffer写入该第二个page的pageHeader里的statistics属性
        firstPageStatistics = null; //重置
      } else {  //若该page是第三个page以上
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getUncompressedSize(), pageBuffer);//往pageBuffer写入该第二个page的pageHeader里的UncompressedSize属性
        ReadWriteForEncodingUtils.writeUnsignedVarInt(header.getCompressedSize(), pageBuffer);//往pageBuffer写入该第二个page的pageHeader里的compressedSize属性
        header.getStatistics().serialize(pageBuffer);//往pageBuffer写入该第二个page的pageHeader里的statistics属性
      }
      logger.debug(
          "finish to flush a page header {} of {} into buffer, buffer position {} ",
          header,
          measurementSchema.getMeasurementId(),
          pageBuffer.size());

      statistics.mergeStatistics(header.getStatistics());//每往该Chunk的ChunkWriterImpl的输出流pageBuffer放入一个新的page数据，就要更新该Chunk的statistics信息

    } catch (IOException e) {
      throw new PageException("IO Exception in writeDataPageHeader,ignore this page", e);
    }
    numOfPages++; //该chunk的page数量+1
    // write page content to temp PBAOS//然后写pageData到输出流pageBuffer
    try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
      channel.write(data);    //往该Chunk的ChunkWriterImpl的输出流pageBuffer写入pageData
    } catch (IOException e) {
      throw new PageException(e);
    }
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer the specified IOWriter
   * @param statistics the chunk statistics
   * @throws IOException exception in IO
   */
  private void writeAllPagesOfChunkToTsFile(TsFileIOWriter writer, Statistics<?> statistics)
      throws IOException {    //首先往TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,然后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
    if (statistics.getCount() == 0) { //若该Chunk的数据点为0，则返回
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(  //初始化该TsFileIOWriter的当前要写入的Chunk元数据对象属性currentChunkMetadata，并把该Chunk的ChunkHeader内容写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
        measurementSchema.getMeasurementId(),
        compressor.getType(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        statistics,
        pageBuffer.size(),  //pageBuffer的字节数量
        numOfPages,   //待写入的page数量
        0);

    long dataOffset = writer.getPos(); //获取TsFileIOWriter的当前TsFileOutput对象的缓存输出流的缓存数组大小，即获取在TSFile文件中该Chunk的第一个page开始写入的位置

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);//将该Chunk里的所有page数据（pageHeader+pageData）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里

    int dataSize = (int) (writer.getPos() - dataOffset);  //计算TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的所有page数据的大小，用于后续判断该写操作（把该Chunk的所有page数据写入）是否出现错误
    if (dataSize != pageBuffer.size()) {  //若写入的该Chunk所有page数据大小不等于pageBuffer大小，则报错
      throw new IOException(
          "Bytes written is inconsistent with the size of data: "
              + dataSize
              + " !="
              + " "
              + pageBuffer.size());
    }

    writer.endCurrentChunk();//当结束当前Chunk的写操作后就会调用此方法，做一些善后工作
  }

  public void setIsMerging(boolean isMerging) {
    this.isMerging = isMerging;
  }

  public boolean isMerging() {
    return isMerging;
  }

  public void setLastPoint(boolean isLastPoint) {
    this.isLastPoint = isLastPoint;
  }
}
