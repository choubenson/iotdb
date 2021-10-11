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
package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class PageReader implements IPageReader { // page读取接口的具体实现类

  private PageHeader pageHeader;

  protected TSDataType dataType; // page数据类型

  /** decoder for value column */
  protected Decoder valueDecoder; // 数据值解码器

  /** decoder for time column */
  protected Decoder timeDecoder; // 时间解码器

  /** time column in memory */
  protected ByteBuffer timeBuffer; // 时间缓存，是二进制格式的，需要用timeDecoder时间解码器去读取该二进制缓存里的时间

  /** value column in memory */
  protected ByteBuffer valueBuffer; // 数据缓存，是二进制格式的，需要用valueDecoder数值解码器去读取该二进制缓存里的数值

  protected Filter filter;

  /** A list of deleted intervals. */
  private List<TimeRange>
      deleteIntervalList; // 该列表存放了属于该page的每个删除的时间范围类对象，它是按时间范围顺序存储的，如第一个删除的时间戳范围是（2到10），第二个是（15到20），第三个是（22到100）

  private int deleteCursor = 0;

  public PageReader(
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(
      ByteBuffer pageData) { // 把pageData这个ByteBuffer对象拆成两个ByteBuffer对象，即timeBuffer和valueBuffer
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending)
      throws IOException { // 读取该page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里并返回

    BatchData pageData =
        BatchDataFactory.createBatchData(dataType, ascending, false); // BatchData类对象

    while (timeDecoder.hasNext(
        timeBuffer)) { // 读取该page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里
      long timestamp = timeDecoder.readLong(timeBuffer); // 使用时间解码器从时间缓存timeBuffer里读取时间戳
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean =
              valueDecoder.readBoolean(valueBuffer); // 使用数值解码器从数值缓存valueBuffer里读取Boolean类型数据
          if (!isDeleted(timestamp)
              && (filter == null
                  || filter.satisfy(
                      timestamp, aBoolean))) { // 若该时间戳不在删除的时间范围里 并且 过滤器对象是空或者该时间戳的数据符合过滤器
            pageData.putBoolean(timestamp, aBoolean); // 把该时间戳和数值放入BatchData类对象里
          }
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
            pageData.putInt(timestamp, anInt);
          }
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
            pageData.putLong(timestamp, aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
            pageData.putFloat(timestamp, aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
            pageData.putDouble(timestamp, aDouble);
          }
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
            pageData.putBinary(timestamp, aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData.flip();
  }

  @Override
  public Statistics getStatistics() {
    return pageHeader.getStatistics();
  }

  @Override
  public void setFilter(Filter filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = new AndFilter(this.filter, filter);
    }
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  // 在使用该方法时，默认
  protected boolean isDeleted(long timestamp) { // 判断该时间戳是否在删除的时间范围里
    while (deleteIntervalList != null
        && deleteCursor < deleteIntervalList.size()) { // 遍历每个删除的时间范围对象
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) { // 若timestamp在该删除时间范围内，则返回true
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax()
          < timestamp) { // 若当前删除时间范围的最大值小于timeStamp，则说明后面还可能存在待删除的时间范围
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }
}
