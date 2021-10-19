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

package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.v2.file.header.PageHeaderV2;
import org.apache.iotdb.tsfile.v2.read.reader.page.PageReaderV2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ChunkReader implements IChunkReader {  //一个时间序列的Chunk读取器，一个时间序列可能会有好几个ChunkReader，因为一个TsFile的一个时间序列会存在同设备ID的多个ChunkGroup的Chunk

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkDataBuffer; //该Chunk的ChunkData的二进制流缓存
  private IUnCompressor unCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  protected Filter filter;  //过滤器，用于在读取数据的时候过滤相应的数据

  private List<IPageReader> pageReaderList = new LinkedList<>();//存放了该Chunk的满足过滤器的每个page的PageReader对象，每读完一个page，就把该pageReader从列表中移除

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  /**
   * constructor of ChunkReader.
   *
   * @param chunk input Chunk object
   * @param filter filter
   */
  public ChunkReader(Chunk chunk, Filter filter) throws IOException {//创建对应的ChunkReader并初始化其所有满足条件（存在未被删除且满足过滤器的数据的page）的PageReader
    this.filter = filter;
    this.chunkDataBuffer = chunk.getData();
    this.deleteIntervalList = chunk.getDeleteIntervalList();
    chunkHeader = chunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    if (chunk.isFromOldFile()) {
      initAllPageReadersV2();
    } else {
      initAllPageReaders(chunk.getChunkStatistic());//通过读取该Chunk的所有pageData的内容来初始化对应的所有满足条件（存在未被删除且满足过滤器的数据的page）的page的pageReader
    }
  }

  private void initAllPageReaders(Statistics chunkStatistic) throws IOException {//通过读取该Chunk的所有pageData的内容来初始化对应的所有满足条件（存在未被删除且满足过滤器的数据的page）的page的pageReader
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {//若chunkDataBuffer还有未读取的数据
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;  //下面从chunkDataBuffer里反序列化pageHeader
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {  //若该Chunk有大于1个数量的page,则
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      // if the current page satisfies
      if (pageSatisfied(pageHeader)) {//判断该page里是否存在未被删除且满足过滤器的数据，存在则返回true
        pageReaderList.add(constructPageReaderForNextPage(pageHeader));//通过读取对应page的经压缩后的pageData并进行解压后，创建属于该page的pageReader,并加入该ChunkReader的pageReaderList列表里
      } else {//若该page里的所有数据都不满足条件，则跳过读取该pageData的内容
        skipBytesInStreamByLength(pageHeader.getCompressedSize());//将chunkDataBuffer里读指针的位置后移length个长度，即跳过中间的内容
      }
    }
  }

  /** judge if has next page whose page header satisfies the filter. */
  @Override
  public boolean hasNextSatisfiedPage() { //判断该Chunk是否还存在满足过滤器的Page没有被读取
    return !pageReaderList.isEmpty();   //若存在满足过滤器的page则pageReaderList不为空
  }

  /**
   * get next data batch.
   *
   * @return next data batch
   * @throws IOException IOException
   */
  @Override
  public BatchData nextPageData() throws IOException {
    if (pageReaderList.isEmpty()) {
      throw new IOException("No more page");
    }
    return pageReaderList.remove(0).getAllSatisfiedPageData();// 读取Chunk下一个page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里并返回。每读完一个page，就把该pageReader从列表中移除
  }

  private void skipBytesInStreamByLength(int length) {  //将chunkDataBuffer里读指针的位置后移length个长度，即跳过中间的内容
    chunkDataBuffer.position(chunkDataBuffer.position() + length);
  }

  protected boolean pageSatisfied(PageHeader pageHeader) {//判断该page里是否存在未被删除且满足过滤器的数据，存在则返回true
    if (deleteIntervalList != null) {
      for (TimeRange range : deleteIntervalList) {
        if (range.contains(pageHeader.getStartTime(), pageHeader.getEndTime())) {//若该page的数据全被删了，则返回false
          return false;
        }
        if (range.overlaps(new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime()))) {//若当前page存在被删除的数据，即该page时间戳区间与删除区间有重叠，则设置当前page是已经被修改过的
          pageHeader.setModified(true);
        }
      }
    }
    return filter == null || filter.satisfy(pageHeader.getStatistics());
  }

  private PageReader constructPageReaderForNextPage(PageHeader pageHeader) throws IOException {//通过读取对应page的经压缩后的pageData并进行解压后，创建属于该page的pageReader
    int compressedPageBodyLength = pageHeader.getCompressedSize();//该page压缩后的大小
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {//当该page压缩后的大小 大于 该chunk的chunkDataBuffer里剩余未读的内容，则抛异常
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);//从chunkDataBuffer里读取内容填满到compressedPageBody字节数组里
    Decoder valueDecoder =  //根据编码类型和数据类型创建数值解码器
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];//该page解压缩后的大小
    try {
      unCompressor.uncompress(  //将压缩后的内容compressedPageBody进行解压缩后存放入uncompressedPageData数组里
          compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }

    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);//把解压缩后的内容放入二进制缓存pageData
    PageReader reader = //通过解压后的pageData数据内容创建属于该page的pageReader
        new PageReader(
            pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    reader.setDeleteIntervalList(deleteIntervalList);//设置删除时间
    return reader;
  }

  @Override
  public void close() {}

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return pageReaderList;
  }

  // For reading TsFile V2
  private void initAllPageReadersV2() throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader =
          PageHeaderV2.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      // if the current page satisfies
      if (pageSatisfied(pageHeader)) {
        pageReaderList.add(constructPageReaderForNextPageV2(pageHeader));
      } else {
        skipBytesInStreamByLength(pageHeader.getCompressedSize());
      }
    }
  }

  // For reading TsFile V2
  private PageReader constructPageReaderForNextPageV2(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    unCompressor.uncompress(
        compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);
    PageReader reader =
        new PageReaderV2(
            pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }
}
