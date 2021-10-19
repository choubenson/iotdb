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
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VectorChunkWriterImpl implements IChunkWriter {//多元传感器的ChunkWriter。一个多元传感器里每个子分量（不含时间）会有自己的ValueChunkWriter和对应的ValuePageWriter,因为数据类型等属性都不一样，但是只有一个TimeChunkWriter

  private final TimeChunkWriter timeChunkWriter;  //时间分量的timeChunkWriter
  private final List<ValueChunkWriter> valueChunkWriterList;  //数值分量的ValueChunkWriter列表
  private int valueIndex;

  /** @param schema schema of this measurement */
  public VectorChunkWriterImpl(IMeasurementSchema schema) {
    timeChunkWriter =
        new TimeChunkWriter(
            schema.getMeasurementId(),
            schema.getCompressor(),
            schema.getTimeTSEncoding(),
            schema.getTimeEncoder());

    List<String> valueMeasurementIdList = schema.getSubMeasurementsList();
    List<TSDataType> valueTSDataTypeList = schema.getSubMeasurementsTSDataTypeList();
    List<TSEncoding> valueTSEncodingList = schema.getSubMeasurementsTSEncodingList();
    List<Encoder> valueEncoderList = schema.getSubMeasurementsEncoderList();

    valueChunkWriterList = new ArrayList<>(valueMeasurementIdList.size());
    for (int i = 0; i < valueMeasurementIdList.size(); i++) {
      valueChunkWriterList.add(
          new ValueChunkWriter(
              schema.getMeasurementId()
                  + TsFileConstant.PATH_SEPARATOR
                  + valueMeasurementIdList.get(i),
              schema.getCompressor(),
              valueTSDataTypeList.get(i),
              valueTSEncodingList.get(i),
              valueEncoderList.get(i)));
    }

    this.valueIndex = 0;
  }

  @Override
  public void write(long time, int value, boolean isNull) {//使用该多元传感器的该子分量的ValueChunkWriter把该value写入其pageWriter的valueOut写入流缓存里。注意：此处只会写入value到缓存，time不会被写入
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, long value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, boolean value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, float value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, double value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time, Binary value, boolean isNull) {
    valueChunkWriterList.get(valueIndex++).write(time, value, isNull);
  }

  @Override
  public void write(long time) {//往该多元传感器的TimeChunkWriter的pageWriter写入时间戳到其timeOut缓存里，并判断是否需要开启一个新的page,若需要，则将该多元传感器的TimePage的内容（pageHeader和时间戳列，即TimePageWriter对象里输出流timeOut的缓存数据）和每个ValuePage的内容（pageHeader和该子分量的数值内容，即对应ValuePageWriter里的bitmapOut和valueOut缓存）写到对应TimeChunkWriter和各子分量ValueChunkWriter的pageBuffer缓存里
    valueIndex = 0;
    timeChunkWriter.write(time);//往该多元传感器的TimeChunkWriter的pageWriter写入时间戳到其timeOut缓存里
    if (checkPageSizeAndMayOpenANewPage()) {//根据时间分量占用的大小判断是否需要开启一个新的page,若需要，则
      writePageToPageBuffer();//将该多元传感器的TimePage的内容（pageHeader和时间戳列，即TimePageWriter对象里输出流timeOut的缓存数据）和每个ValuePage的内容（pageHeader和该子分量的数值内容，即对应ValuePageWriter里的bitmapOut和valueOut缓存）写到对应TimeChunkWriter和各子分量ValueChunkWriter的pageBuffer缓存里
    }
  }

  // TODO tsfile write interface
  @Override
  public void write(long[] timestamps, int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and put it
   * to pageBuffer
   */
  private boolean checkPageSizeAndMayOpenANewPage() {//根据时间分量占用的大小判断是否需要开启一个新的page
    return timeChunkWriter.checkPageSizeAndMayOpenANewPage();//根据时间分量占用的大小判断是否需要开启一个新的page
  }

  private void writePageToPageBuffer() {//将该多元传感器的TimePage的内容（pageHeader和时间戳列，即TimePageWriter对象里输出流timeOut的缓存数据）和每个ValuePage的内容（pageHeader和该子分量的数值内容，即对应ValuePageWriter里的bitmapOut和valueOut缓存）写到对应TimeChunkWriter和各子分量ValueChunkWriter的pageBuffer缓存里
    timeChunkWriter.writePageToPageBuffer();// 往对应多元传感器Chunk的TimeChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和时间戳列（即TimePageWriter对象里输出流timeOut的缓存数据），最后重置该TimePageWriter
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writePageToPageBuffer();// 往对应多元传感器Chunk的某子分量的ValueChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和该子分量的数值内容（即对应ValuePageWriter里的bitmapOut和valueOut缓存），最后重置该TimePageWriter
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {//先后一次对TimeChunkWriter和每个子传感器的ValueChunkWriter执行以下操作：1. 封口当前子传感器的TimePage/ValuePage的内容（即TimePageWriter/ValuePageWriter对象里输出流timeOut/bitmapOut和valueOut的缓存数据）到此TimeChunkWriter/ValueChunkWriter的输出流pageBuffer缓存里 2. 把当前TimeChunk/ValueChunk的ChunkHeader和TimeChunkWriter/ValueChunkWriter里pageBuffer缓存的内容（该chunk所有page的pageHeader+timeOut/bitmapOut和valueOut）写到该TsFileIOWriter对象的out缓存里 ,并初始化当前Chunk的ChunkIndex对象并放到TsFileIOWriter里
    timeChunkWriter.writeToFileWriter(tsfileWriter);//1. 封口当前TimePage的内容（即TimePageWriter对象里输出流timeOut的缓存数据）到TimeChunkWriter的输出流pageBuffer缓存里 2. 把当前Chunk的ChunkHeader和TimeChunkWriter里pageBuffer缓存的内容（该chunk所有page的pageHeader+时间戳列）写到该TsFileIOWriter对象的out缓存里 ,并初始化当前Chunk的ChunkIndex对象并放到TsFileIOWriter里
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.writeToFileWriter(tsfileWriter);//1. 封口当前子传感器的ValuePage的内容（即ValuePageWriter对象里输出流bitmapOut和valueOut的缓存数据）到此ValueChunkWriter的输出流pageBuffer缓存里 2. 把当前ValueChunk的ChunkHeader和ValueChunkWriter里pageBuffer缓存的内容（该chunk所有page的pageHeader+bitmapOut和valueOut）写到该TsFileIOWriter对象的out缓存里 ,并初始化当前Chunk的ChunkIndex对象并放到TsFileIOWriter里
    }
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    long estimateMaxSeriesMemSize = timeChunkWriter.estimateMaxSeriesMemSize();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      estimateMaxSeriesMemSize += valueChunkWriter.estimateMaxSeriesMemSize();
    }
    return estimateMaxSeriesMemSize;
  }

  @Override
  public long getCurrentChunkSize() {
    long currentChunkSize = timeChunkWriter.getCurrentChunkSize();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      currentChunkSize += valueChunkWriter.getCurrentChunkSize();
    }
    return currentChunkSize;
  }

  @Override
  public void sealCurrentPage() { //封口TimePageWriter的内容到TimeChunkWriter的pageBuffer和封口每个ValuePageWriter的内容（该子分量的数值内容（即对应ValuePageWriter里的bitmapOut和valueOut缓存）到各自的ValueChunkWriter的pageBuffer里
    timeChunkWriter.sealCurrentPage();//封口TimeChunkWriter的当前PageWriter的内容，即往对应多元Chunk的TimeChunkWriter的输出流pageBuffer缓存里写入该page的pageHeader和时间戳列（即TimePageWriter对象里输出流timeOut的缓存数据），最后重置该TimePageWriter
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.sealCurrentPage();
    }
  }

  @Override
  public void clearPageWriter() {
    timeChunkWriter.clearPageWriter();
    for (ValueChunkWriter valueChunkWriter : valueChunkWriterList) {
      valueChunkWriter.clearPageWriter();
    }
  }

  @Override
  public int getNumOfPages() {
    return timeChunkWriter.getNumOfPages();
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.VECTOR;
  }
}
