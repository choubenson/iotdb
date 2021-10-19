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

import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** a implementation of IChunkGroupWriter. */
public class ChunkGroupWriterImpl
    implements IChunkGroupWriter { // TsFile文件里，每个设备ID应该对应一个ChunkGroupWriter类对象。ChunkGroupWriter类里存放着每个传感器ID对应的ChunkWriter类对象

  private static final Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);

  private final String deviceId; // 该ChunkGroupWriter所属的设备ID

  /** Map(measurementID, ChunkWriterImpl). */
  private Map<String, IChunkWriter> chunkWriters = new HashMap<>(); // 存放了每个传感器ID对应的ChunkWriter类对象，可能包含多元传感器的VectorChunkWriterImpl

  public ChunkGroupWriterImpl(String deviceId) {
    this.deviceId = deviceId;
  }

  @Override // 此处schema可能是一个多元传感器配置类对象
  public void tryToAddSeriesWriter(
      IMeasurementSchema schema,
      int pageSizeThreshold) { // 判断该ChunkGroupWriter里是否存在此传感器ID对应的ChunkWriter，若不存在，则创建一个
    if (!chunkWriters.containsKey(
        schema.getMeasurementId())) { // 若chunkWriters里不包含该传感器ID的ChunkWriter，则
      IChunkWriter seriesWriter = null;
      // initialize depend on schema type
      if (schema instanceof VectorMeasurementSchema) { // 如果传感器配置类是多元传感器配置，则
        seriesWriter = new VectorChunkWriterImpl(schema); // 新建多元传感器的ChunkWriter
      } else if (schema instanceof UnaryMeasurementSchema) { // 如果是一元传感器配置，则
        seriesWriter = new ChunkWriterImpl(schema); // 新建一元传感器的IChunkWriter
      }
      this.chunkWriters.put(
          schema.getMeasurementId(),
          seriesWriter); // 往该ChunkGroupWriter的chunkWriter变量里添加此传感器ID和对应的ChunkWriter
    }
  }

  @Override
  public void write(long time, List<DataPoint> data)
      throws WriteProcessException, //Todo:支持vector
          IOException { // 将所有的数据点写入对应ChunkWriter的PageWriter的缓存里。具体做法是：遍历所有的data数据点（传感器ID，数值），根据每个数据点的传感器ID，将给定的数据点（time,value）交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    for (DataPoint point : data) { // 遍历同一时间戳上每个传感器的数据点（传感器ID，数值）
      String measurementId = point.getMeasurementId(); // 获取传感器ID
      if (!chunkWriters.containsKey(
          measurementId)) { // 若当前ChunkGroupWriter不存在此传感器ID的ChunkWriter，则报错
        throw new NoMeasurementException(
            "time " + time + ", measurement id " + measurementId + " not found!");
      }
      //Todo:此处貌似已经支持多元的写入了，因为chunkWriters里会存放多元的VectorChunkWriterImpl
      point.writeTo(
          time,
          chunkWriters.get(
              measurementId)); // 根据指定的ChunkWriter，将给定的数据点（time,value）交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    }
  }

  @Override
  public void write(Tablet tablet)  //该Tablet里可能有一元或者多元时间序列，若是一元序列则会占用该Tablet的一列，代表是一个一元传感器；若是多元序列，则其有几个子分量传感器就会占用该Tablet几列
      throws
          WriteProcessException { // 依次遍历tablet结构里的每个传感器，然后每次把该传感器上所有时间戳对应的数据交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    List<IMeasurementSchema> timeseries = tablet.getSchemas(); // 获取该Tablet的所有传感器配置类对象
    for (int i = 0; i < timeseries.size(); i++) { // 遍历每个传感器配置类对象
      String measurementId = timeseries.get(i).getMeasurementId(); // 获取传感器ID
      TSDataType dataType = timeseries.get(i).getType();
      if (!chunkWriters.containsKey(
          measurementId)) { // 若当前ChunkGroupWriter不存在此传感器ID的ChunkWriter，则报错
        throw new NoMeasurementException("measurement id" + measurementId + " not found!");
      }
      if (dataType.equals(TSDataType.VECTOR)) { // 如果数据类型是多元数据，则
        writeVectorDataType(tablet, measurementId, i);//每次遍历该多元序列的一行数据，先写所有的子传感器的value到对应各自的valuePageWriter缓存里，然后再写入该行的时间戳到对应的TimePageWriter里，并判断是否要开启新的page。具体做法是：（1）然后依次对该行的每个子分列传感器的值写入该多元传感器的VectorChunkWriterImpl里对应分量的ValueChunkWriter里的ValuePageWriter里的valueOut缓存里。（2）写完该行的所有子传感器数据后，再往该多元传感器的TimeChunkWriter的pageWriter写入时间戳到其timeOut缓存里，并判断是否需要开启一个新的page,若需要，则先后将该多元传感器的TimePage的内容（pageHeader和时间戳列，即TimePageWriter对象里输出流timeOut的缓存数据）和每个ValuePage的内容（pageHeader和该子分量的数值内容，即对应ValuePageWriter里的bitmapOut和valueOut缓存）写到对应TimeChunkWriter和各子分量ValueChunkWriter的pageBuffer缓存里。
      } else {
        writeByDataType(
            tablet,
            measurementId,
            dataType,
            i); // 将给定的Tablet里的数据点数组交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
      }
    }
  }

  /**
   * write if data type is VECTOR this method write next n column values (belong to one vector), and
   * return n to increase index
   *
   * @param tablet table
   * @param measurement vector measurement
   * @param index measurement start index
   */   //每次遍历该多元序列的一行数据，先写所有的子传感器的value到对应各自的valuePageWriter缓存里，然后再写入该行的时间戳到对应的TimePageWriter里，并判断是否要开启新的page。具体做法是：（1）然后依次对该行的每个子分列传感器的值写入该多元传感器的VectorChunkWriterImpl里对应分量的ValueChunkWriter里的ValuePageWriter里的valueOut缓存里。（2）写完该行的所有子传感器数据后，再往该多元传感器的TimeChunkWriter的pageWriter写入时间戳到其timeOut缓存里，并判断是否需要开启一个新的page,若需要，则将该多元传感器的TimePage的内容（pageHeader和时间戳列，即TimePageWriter对象里输出流timeOut的缓存数据）和每个ValuePage的内容（pageHeader和该子分量的数值内容，即对应ValuePageWriter里的bitmapOut和valueOut缓存）写到对应TimeChunkWriter和各子分量ValueChunkWriter的pageBuffer缓存里。
  private void writeVectorDataType(Tablet tablet, String measurement, int index) {//该Table里的第index列开始是该多元传感器的子分量传感器，列数的数量为子分量的数量。
    // reference: MemTableFlushTask.java
    int batchSize = tablet.rowSize;
    VectorMeasurementSchema vectorMeasurementSchema =
        (VectorMeasurementSchema) tablet.getSchemas().get(index);
    List<TSDataType> valueDataTypes = vectorMeasurementSchema.getSubMeasurementsTSDataTypeList();//获取该多元传感器里每个子分量的数据类型
    IChunkWriter vectorChunkWriter = chunkWriters.get(measurement);//获取该多元传感器的ChunkWriter,即VectorChunkWriterImpl对象
    for (int row = 0; row < batchSize; row++) {//遍历每行数据
      long time = tablet.timestamps[row];//获取该行时间戳
      for (int columnIndex = 0; columnIndex < valueDataTypes.size(); columnIndex++) {//遍历该多元传感器的该行的每列子分量，把对应的数值依次写入该多元传感器的VectorChunkWriterImpl里对应分量的ValueChunkWriter里的ValuePageWriter里的valueOut缓存里
        boolean isNull = false;
        // check isNull by bitMap in tablet
        if (tablet.bitMaps != null  //判断该行的该列子分量是否为null
            && tablet.bitMaps[columnIndex] != null
            && tablet.bitMaps[columnIndex].isMarked(row)) {
          isNull = true;
        }
        switch (valueDataTypes.get(columnIndex)) {
          case BOOLEAN:
            vectorChunkWriter.write(time, ((boolean[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT32:
            vectorChunkWriter.write(time, ((int[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT64:
            vectorChunkWriter.write(time, ((long[]) tablet.values[columnIndex])[row], isNull);//把该列子分量的该行数据传过去，第二个参数为是否为Null
            break;
          case FLOAT:
            vectorChunkWriter.write(time, ((float[]) tablet.values[columnIndex])[row], isNull);
            break;
          case DOUBLE:
            vectorChunkWriter.write(time, ((double[]) tablet.values[columnIndex])[row], isNull);
            break;
          case TEXT:
            vectorChunkWriter.write(time, ((Binary[]) tablet.values[columnIndex])[row], isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", valueDataTypes.get(columnIndex)));
        }
      }
      vectorChunkWriter.write(time);//往该多元传感器的TimeChunkWriter的pageWriter写入时间戳到其timeOut缓存里，并判断是否需要开启一个新的page,若需要，则将该多元传感器的TimePage的内容（pageHeader和时间戳列，即TimePageWriter对象里输出流timeOut的缓存数据）和每个ValuePage的内容（pageHeader和该子分量的数值内容，即对应ValuePageWriter里的bitmapOut和valueOut缓存）写到对应TimeChunkWriter和各子分量ValueChunkWriter的pageBuffer缓存里
    }
  }

  /**
   * write by data type dataType should not be VECTOR! VECTOR type should use writeVector
   *
   * @param tablet table contain all time and value
   * @param measurementId current measurement
   * @param dataType current data type
   * @param index which column values should be write
   */
  private void
      writeByDataType( // 将给定的Tablet里的数据点数组交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
      Tablet tablet, String measurementId, TSDataType dataType, int index) {
    int batchSize = tablet.rowSize; // 行数，即时间戳的数量
    switch (dataType) {
      case INT32:
        chunkWriters
            .get(measurementId)
            .write(
                tablet.timestamps,
                (int[]) tablet.values[index],
                batchSize); // 将给定的数据点数组交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
        break;
      case INT64:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (long[]) tablet.values[index], batchSize);
        break;
      case FLOAT:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (float[]) tablet.values[index], batchSize);
        break;
      case DOUBLE:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (double[]) tablet.values[index], batchSize);
        break;
      case BOOLEAN:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (boolean[]) tablet.values[index], batchSize);
        break;
      case TEXT:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (Binary[]) tablet.values[index], batchSize);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  @Override
  public long flushToFileWriter(TsFileIOWriter fileWriter)
      throws
          IOException { // 将该ChunkGroupWriter的所有ChunkWriter对应的Chunk数据（ChunkHeader+ChunkData）给flush到TsFileIOWriter类对象里的输出缓存流out里，并返回总共flush的字节数。具体做法是遍历该ChunkGroupWriter的所有ChunkWriter做如下操作：首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）.
    LOG.debug("start flush device id:{}", deviceId);
    // make sure all the pages have been compressed into buffers, so that we can get correct
    // groupWriter.getCurrentChunkGroupSize().
    sealAllChunks(); // 关闭、封口该ChunkGroupWriter里所有ChunkWriter对应的当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)
    long currentChunkGroupSize =
        getCurrentChunkGroupSize(); // 获取当前ChunkGroup的字节大小，即其所有Chunk的字节大小(ChunkHeader+ChunkData，ChunkData即缓存pageBuffer）的总和
    for (IChunkWriter seriesWriter : chunkWriters.values()) { // 遍历所有一元或多元传感器的ChunkWriter
      seriesWriter.writeToFileWriter(
          fileWriter); // 首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
    }
    return currentChunkGroupSize;
  }

  @Override
  public long updateMaxGroupMemSize() { // 获取该ChunkGroupWriter最大占用的内存大小，即其所有ChunkWriter占用的最大内存大小的总和
    long bufferSize = 0;
    for (IChunkWriter seriesWriter : chunkWriters.values()) {
      bufferSize += seriesWriter.estimateMaxSeriesMemSize();
    }
    return bufferSize;
  }

  @Override
  public long
      getCurrentChunkGroupSize() { // 获取当前ChunkGroup的字节大小，即其所有Chunk的字节大小(ChunkHeader+ChunkData，ChunkData即缓存pageBuffer）的总和
    long size = 0;
    for (IChunkWriter writer : chunkWriters.values()) {
      size +=
          writer.getCurrentChunkSize(); // 获取当前Chunk的字节大小，即ChunkHeader+ChunkData（缓存pageBuffer）的大小
    }
    return size;
  }

  /** seal all the chunks which may has un-sealed pages in force. */
  private void
      sealAllChunks() { // 关闭、封口该ChunkGroupWriter里所有ChunkWriter对应的当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)
    for (IChunkWriter writer : chunkWriters.values()) {
      writer
          .sealCurrentPage(); // 关闭、封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)
    }
  }

  @Override
  public int getSeriesNumber() {
    return chunkWriters.size();
  }
}
