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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkGroupWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TsFileWriter is the entrance for writing processing. It receives a record and send it to
 * responding chunk group write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user should call {@code
 * close()} method to flush the last data outside and close the normal outputStream and error
 * outputStream.
 */
public class TsFileWriter implements AutoCloseable { // 每个TsFile对应的Writer，此类里的write方法都只是写数据区的内容，因此在使用该对象写完数据后，用户必须手动调用close方法以便往tsFile里写入索引区。

  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger LOG = LoggerFactory.getLogger(TsFileWriter.class);
  /** schema of this TsFile. */
  protected final Schema schema;
  /** IO writer of this TsFile. */
  private final TsFileIOWriter fileWriter;

  private final int pageSize; // 该TsFile里每个page的大小
  private long recordCount = 0; // 记录该TsFile里共有几条时间戳的数据，即同一时间戳算是一条数据

  private Map<String, IChunkGroupWriter> groupWriters = new HashMap<>(); // （设备ID，IChunkGroupWriter）

  /** min value of threshold of data points num check. */
  private long recordCountForNextMemCheck = 100;

  private long chunkGroupSizeThreshold;

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   */
  public TsFileWriter(File file) throws IOException {
    this(new TsFileIOWriter(file), new Schema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param fileWriter the io writer of this TsFile
   */
  public TsFileWriter(TsFileIOWriter fileWriter) throws IOException {
    this(fileWriter, new Schema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   */
  public TsFileWriter(File file, Schema schema) throws IOException {
    this(new TsFileIOWriter(file), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param output the TsFileOutput of the file to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   * @throws IOException
   */
  public TsFileWriter(TsFileOutput output, Schema schema) throws IOException {
    this(new TsFileIOWriter(output), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  public TsFileWriter(File file, Schema schema, TSFileConfig conf) throws IOException {
    this(new TsFileIOWriter(file), schema, conf);
  }

  /**
   * init this TsFileWriter.
   *
   * @param fileWriter the io writer of this TsFile
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  protected TsFileWriter(TsFileIOWriter fileWriter, Schema schema, TSFileConfig conf)
      throws IOException {
    if (!fileWriter.canWrite()) {
      throw new IOException(
          "the given file Writer does not support writing any more. Maybe it is an complete TsFile");
    }
    this.fileWriter = fileWriter;

    if (fileWriter instanceof RestorableTsFileIOWriter) {
      this.schema = new Schema(((RestorableTsFileIOWriter) fileWriter).getKnownSchema());
    } else {
      this.schema = schema;
    }
    this.pageSize = conf.getPageSizeInByte();
    this.chunkGroupSizeThreshold = conf.getGroupSizeInByte();
    config.setTSFileStorageFs(conf.getTSFileStorageFs());
    if (this.pageSize >= chunkGroupSizeThreshold) {
      LOG.warn(
          "TsFile's page size {} is greater than chunk group size {}, please enlarge the chunk group"
              + " size or decrease page size. ",
          pageSize,
          chunkGroupSizeThreshold);
    }
  }

  public void registerSchemaTemplate(
      String templateName, Map<String, IMeasurementSchema> template) {
    schema.registerSchemaTemplate(templateName, template);
  }

  public void registerDevice(String deviceId, String templateName) {
    schema.registerDevice(deviceId, templateName);
  }

  public void registerTimeseries(Path path, IMeasurementSchema measurementSchema)
      throws WriteProcessException { // 往该TsFile里注册一个新的时间序列
    if (schema.containsTimeseries(path)) { // 判断当前TsFile文件里是否已经存在、注册过该时间序列路径
      throw new WriteProcessException("given timeseries has exists! " + path);
    }
    schema.registerTimeseries(path, measurementSchema); // 往该TsFile里注册一个新的时间序列
  }

  /**
   * Confirm whether the record is legal. If legal, add it into this RecordWriter.
   *
   * @param record - a record responding a line
   * @return - whether the record has been added into RecordWriter legally
   * @throws WriteProcessException exception
   */
  private boolean checkIsTimeSeriesExist(TSRecord record)
      throws
          WriteProcessException { // 判断当前的TsRecord数据是否合法（即该TsFile里存在对应的时间序列），若合法则：（1）判断当前TsFileWriter是否有对应设备ID的ChunkGroupWriter，若没有则新建一个放入groupWriters里。（2）判断对应设备ID的chunkGroupWriter里是否存在对应传感器ID的ChunkWriter，若没有则新建一个放入chunkWriters里
    IChunkGroupWriter groupWriter; // 存放该record对应的设备ID在该TsFile里对应的ChunkGroupWriterImpl对象
    if (!groupWriters.containsKey(record.deviceId)) { // 若groupWriters变量里不包含该设备，则
      groupWriter =
          new ChunkGroupWriterImpl(
              record.deviceId); // 新建一个属于该设备ID的ChunkGroupWriterImpl，并把它放入groupWriters变量里
      groupWriters.put(record.deviceId, groupWriter);
    } else { // 否则获取属于该设备ID的ChunkGroupWriterImpl对象
      groupWriter = groupWriters.get(record.deviceId);
    }

    // add all SeriesWriter of measurements in this TSRecord to this ChunkGroupWriter
    for (DataPoint dp : record.dataPointList) { // 遍历该TsRecord对象里在该时间戳上所有的传感器对应的数据
      String measurementId = dp.getMeasurementId(); // 该数据点的传感器ID
      Path path = new Path(record.deviceId, measurementId); // 时间序列路径，即将传感器ID和设备ID拼接成全路径
      if (schema.containsTimeseries(path)) { // 若当前TsFile的配置类对象schema里存在该时间序列，则
        groupWriter.tryToAddSeriesWriter(
            schema.getSeriesSchema(path),
            pageSize); // 判断该ChunkGroupWriter里是否存在此传感器ID对应的ChunkWriter，若不存在，则创建一个
      } else if (schema.getSchemaTemplates() != null
          && schema.getSchemaTemplates().size()
              == 1) { // 若当前TsFile的配置类对象schema里不存在该时间序列，且配置类对象schema的配置模板数量为1，则
        // use the default template without needing to register device
        Map<String, IMeasurementSchema> template =
            schema.getSchemaTemplates().entrySet().iterator().next().getValue();
        if (template.containsKey(path.getMeasurement())) { // 若此次遍历到的配置模板包含该MeasureId，则
          groupWriter.tryToAddSeriesWriter(
              template.get(path.getMeasurement()),
              pageSize); // 根据该配置模板获取对应传感器的传感器配置类对象，判断该ChunkGroupWriter里是否存在此传感器ID对应的ChunkWriter，若不存在，则创建一个
        }
      } else {
        throw new NoMeasurementException("input path is invalid: " + path);
      }
    }
    return true;
  }

  /**
   * Confirm whether the tablet is legal.
   *
   * @param tablet - a tablet data responding multiple columns
   * @return - whether the tablet's measurements have been added into RecordWriter legally
   * @throws WriteProcessException exception
   */
  private void checkIsTimeSeriesExist(Tablet tablet)
      throws
          WriteProcessException { // 判断当前的Tablet数据是否合法（即该TsFile里存在对应的时间序列），若合法则：（1）判断当前TsFileWriter是否有对应设备ID的ChunkGroupWriter，若没有则新建一个放入groupWriters里。（2）判断对应设备ID的chunkGroupWriter里是否存在对应传感器ID的ChunkWriter，若没有则新建一个放入chunkWriters里
    IChunkGroupWriter groupWriter;
    if (!groupWriters.containsKey(tablet.prefixPath)) { // 若groupWriters变量里不包含该设备，则
      groupWriter =
          new ChunkGroupWriterImpl(
              tablet.prefixPath); // 新建一个属于该设备ID的ChunkGroupWriterImpl，并把它放入groupWriters变量里
      groupWriters.put(tablet.prefixPath, groupWriter);
    } else {
      groupWriter = groupWriters.get(tablet.prefixPath);
    }
    String deviceId = tablet.prefixPath;

    // add all SeriesWriter of measurements in this Tablet to this ChunkGroupWriter
    for (IMeasurementSchema timeseries : tablet.getSchemas()) { // 遍历所有的传感器配置类对象
      String measurementId = timeseries.getMeasurementId(); // 获取传感器ID
      Path path = new Path(deviceId, measurementId); // 将设备ID和传感器ID拼接成完整的时间序列全路径
      if (schema.containsTimeseries(path)) { // 若当前TsFile的配置类对象schema里存在该时间序列，则
        groupWriter.tryToAddSeriesWriter(
            schema.getSeriesSchema(path),
            pageSize); // 判断该ChunkGroupWriter里是否存在此传感器ID对应的ChunkWriter，若不存在，则创建一个
      } else if (schema.getSchemaTemplates() != null && schema.getSchemaTemplates().size() == 1) {
        // use the default template without needing to register device
        Map<String, IMeasurementSchema> template =
            schema.getSchemaTemplates().entrySet().iterator().next().getValue();
        if (template.containsKey(path.getMeasurement())) {
          groupWriter.tryToAddSeriesWriter(template.get(path.getMeasurement()), pageSize);
        }
      } else {
        throw new NoMeasurementException("input measurement is invalid: " + measurementId);
      }
    }
  }

  /**
   * write a record in type of T.
   *
   * @param record - record responding a data line
   * @return true -size of tsfile or metadata reaches the threshold. false - otherwise
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  public boolean write(TSRecord record)
      throws IOException,
          WriteProcessException { // 写入TsRecord数据，粗略步骤为：（1）判断当前TsFile里是否存在给定TsRecord数据的时间序列（2）若第一步检查合法，则将TsRecord里的所有传感器的数据点写入对应ChunkWriter的PageWriter的缓存里。（3）最后要判断内存是否到达临界值而需要flush所有的ChunkGroups，若需要则将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。
    // make sure the ChunkGroupWriter for this TSRecord exist
    checkIsTimeSeriesExist(
        record); // 判断当前的TsRecord数据是否合法（即该TsFile里存在对应的时间序列），若合法则：（1）判断当前TsFileWriter是否有对应设备ID的ChunkGroupWriter，若没有则新建一个放入groupWriters里。（2）判断对应设备ID的chunkGroupWriter里是否存在对应传感器ID的ChunkWriter，若没有则新建一个放入chunkWriters里
    // get corresponding ChunkGroupWriter and write this TSRecord
    groupWriters
        .get(record.deviceId)
        .write(
            record.time,
            record
                .dataPointList); // 将所有的数据点写入对应ChunkWriter的PageWriter的缓存里。具体做法是：遍历所有的data数据点（传感器ID，数值），根据每个数据点的传感器ID，将给定的数据点（time,value）交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    ++recordCount; // 将该TsFile的时间戳数据点数量+1
    return checkMemorySizeAndMayFlushChunks(); // 判断内存是否到达临界值而需要flush所有的ChunkGroups，若需要则将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。具体的做法是：
    // （1）遍历当前TsFileWriter的所有ChunkGroupWriter，将该ChunkGroupWriter的所有ChunkWriter对应的Chunk数据（ChunkHeader+ChunkData）给flush到TsFileIOWriter类对象里的输出缓存流out里。具体做法是遍历该ChunkGroupWriter的所有ChunkWriter做如下操作：首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）.
    // （2）将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
    // （3）reset该TsFileWriter的相关属性，完成该次的flush操作
  }

  /**
   * write a tablet
   *
   * @param tablet - multiple time series of one device that share a time column
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  public boolean write(Tablet tablet) throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist
    checkIsTimeSeriesExist(
        tablet); // 判断当前的Tablet数据是否合法（即该TsFile里存在对应的时间序列），若合法则：（1）判断当前TsFileWriter是否有对应设备ID的ChunkGroupWriter，若没有则新建一个放入groupWriters里。（2）判断对应设备ID的chunkGroupWriter里是否存在对应传感器ID的ChunkWriter，若没有则新建一个放入chunkWriters里
    // get corresponding ChunkGroupWriter and write this Tablet
    groupWriters
        .get(tablet.prefixPath)
        .write(
            tablet); // 依次遍历tablet结构里的每个传感器，然后每次把该传感器上所有时间戳对应的数据交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    recordCount += tablet.rowSize; // 加上将该Tablet的时间戳行数
    return checkMemorySizeAndMayFlushChunks(); // 判断内存是否到达临界值而需要flush所有的ChunkGroups，若需要则将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。
  }

  /**
   * calculate total memory size occupied by all ChunkGroupWriter instances currently.
   *
   * @return total memory size used
   */
  private long
      calculateMemSizeForAllGroup() { // 获取该TsFileWriter最大占用的内存大小，即其所有ChunkGroupWriter占用的最大内存大小的总和
    long memTotalSize = 0;
    for (IChunkGroupWriter group : groupWriters.values()) {
      memTotalSize +=
          group.updateMaxGroupMemSize(); // 获取该ChunkGroupWriter最大占用的内存大小，即其所有ChunkWriter占用的最大内存大小的总和
    }
    return memTotalSize;
  }

  /**
   * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush them to given
   * OutputStream.
   *
   * @return true - size of tsfile or metadata reaches the threshold. false - otherwise
   * @throws IOException exception in IO
   */
  private boolean checkMemorySizeAndMayFlushChunks()
      throws
          IOException { // 判断内存是否到达临界值而需要flush所有的ChunkGroups，若需要则将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。
    if (recordCount >= recordCountForNextMemCheck) { // 如果数据条数量到达指定数量上限（100），则
      long memSize =
          calculateMemSizeForAllGroup(); // 获取该TsFileWriter最大占用的内存大小，即其所有ChunkGroupWriter占用的最大内存大小的总和
      assert memSize > 0;
      if (memSize > chunkGroupSizeThreshold) { // 如果该TsFileWriter最大内存占用大小超过了临界值，则
        LOG.debug("start to flush chunk groups, memory space occupy:{}", memSize);
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize; // 101*10/11
        return flushAllChunkGroups(); // 将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。具体的做法如下：
        // （1）遍历当前TsFileWriter的所有ChunkGroupWriter，将该ChunkGroupWriter的所有ChunkWriter对应的Chunk数据（ChunkHeader+ChunkData）给flush到TsFileIOWriter类对象里的输出缓存流out里。具体做法是遍历该ChunkGroupWriter的所有ChunkWriter做如下操作：首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）.
        // （2）将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
        // （3）reset该TsFileWriter的相关属性，完成该次的flush操作
      } else {
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
        return false;
      }
    }
    return false;
  }

  /**
   * flush the data in all series writers of all chunk group writers and their page writers to
   * outputStream.
   *
   * @return true - size of tsfile or metadata reaches the threshold. false - otherwise. But this
   *     function just return false, the Override of IoTDB may return true.
   * @throws IOException exception in IO
   */
  public boolean flushAllChunkGroups()
      throws
          IOException { // 将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。具体的做法如下：
    // （1）遍历当前TsFileWriter的所有ChunkGroupWriter，将该ChunkGroupWriter的所有ChunkWriter对应的Chunk数据（ChunkHeader+ChunkData）给flush到TsFileIOWriter类对象里的输出缓存流out里。具体做法是遍历该ChunkGroupWriter的所有ChunkWriter做如下操作：首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）.
    // （2）将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
    // （3）reset该TsFileWriter的相关属性，完成该次的flush操作
    if (recordCount > 0) {
      for (Map.Entry<String, IChunkGroupWriter> entry :
          groupWriters.entrySet()) { // 遍历所有的groupWriter
        String deviceId = entry.getKey();
        IChunkGroupWriter groupWriter = entry.getValue();
        fileWriter.startChunkGroup(
            deviceId); // 开启一个新的设备ChunkGroup，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
        long pos = fileWriter.getPos(); // 获取当前TsFileIOWriter对象的输出缓存流out的长度，或者说缓存流out的缓存数组的指针位置
        long dataSize =
            groupWriter.flushToFileWriter(
                fileWriter); // 将该ChunkGroupWriter的所有ChunkWriter对应的Chunk数据（ChunkHeader+ChunkData）给flush到TsFileIOWriter类对象里的输出缓存流out里，并返回总共flush的字节数。具体做法是遍历该ChunkGroupWriter的所有ChunkWriter做如下操作：首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput输出对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）.
        if (fileWriter.getPos() - pos != dataSize) { // 若fileWriter当前输出缓存流out的增加的长度不为flush的字节数，则报错
          throw new IOException(
              String.format(
                  "Flushed data size is inconsistent with computation! Estimated: %d, Actual: %d",
                  dataSize, fileWriter.getPos() - pos));
        }
        fileWriter
            .endChunkGroup(); // 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
      }
      reset(); // 清空当前TsFile的groupWriters和recordCount
    }
    return false;
  }

  private void reset() { // 清空当前TsFile的groupWriters和recordCount
    groupWriters.clear();
    recordCount = 0;
  }

  /**
   * calling this method to write the last data remaining in memory and close the normal and error
   * OutputStream.
   *
   * @throws IOException exception in IO
   */
  @Override
  public void close() throws IOException {
    LOG.info("start close file");
    flushAllChunkGroups();// 将该TsFileWriter的所有ChunkGroupWriter里的所有ChunkWriter的缓存数据按序缓存到TsFileIOWriter的输出流缓存out里，最后将out输出流给flush到本地对应的TsFile文件。
    fileWriter.endFile();// 在向该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写完数据区的内容并flush到本地后，再往该缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileIOWriter
   */
  public TsFileIOWriter getIOWriter() {
    return this.fileWriter;
  }
}
