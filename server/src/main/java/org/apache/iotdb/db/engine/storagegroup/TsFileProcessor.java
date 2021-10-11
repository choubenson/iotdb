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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.UpdateEndTimeCallBack;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.writelog.WALFlushListener;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@SuppressWarnings("java:S1135") // ignore todos
public class TsFileProcessor { // 每个TsFile对应着自己的一个TsFileProcessor

  /** logger fot this class */
  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);

  /** storgae group name of this tsfile */
  private final String storageGroupName;

  /** IoTDB config */
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** whether it's enable mem control */
  private final boolean enableMemControl = config.isEnableMemControl();

  /** storage group info for mem control */
  private StorageGroupInfo storageGroupInfo;
  /** tsfile processor info for mem control */
  private TsFileProcessorInfo tsFileProcessorInfo;

  /** sync this object in query() and asyncTryToFlush() */
  // 注意：它是个排队队列，因此flush的时候是对队列中第一个workMemtable进行flush，而加入的时候是往该队列的最后一个位置加入该待flush的workMemtable
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables =
      new ConcurrentLinkedDeque<>(); // 存放此TSFile文件的正在被flushing的workMemTable(如果数据量大，有可能几秒钟就flush一次workMemTable，因此要存入此队列中),他们在这个队列里进行排队等待被flush。注意：它存放的是该TsFile的待被flush的workMemtable

  /** modification to memtable mapping */
  // modsToMemtable里存放了一个个workMemtable对应各自的删除操作，说明该workMemtable在待flush时有删除操作，并且还没把此删除操作写入到本地mods文件里
  private List<Pair<Modification, IMemTable>> modsToMemtable =
      new ArrayList<>(); // 该列表用于记录在进行删除操作时，当前TsFile的flushingMemTables列表若不为空，则说明当前TsFile存在待被or正在被flush的workMemtable，因此此时若有  存在正在or准备被flushing的在flushingMemTables列表里的workMemTables，对于这些正在or准备被flush的workMemTable里的数据不能直接删除掉，就只能把对他们的操作（Modification修改类对象）放进该列表里。

  /** writer for restore tsfile and flushing */
  private RestorableTsFileIOWriter writer;

  /** tsfile resource for index this tsfile */
  private final TsFileResource tsFileResource;

  /** time range index to indicate this processor belongs to which time range */
  private long timeRangeId;
  /**
   * Whether the processor is in the queue of the FlushManager or being flushed by a flush thread.
   */
  private volatile boolean managedByFlushManager;

  /** a lock to mutual exclude query and query */
  private final ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();
  /**
   * It is set by the StorageGroupProcessor and checked by flush threads. (If shouldClose == true
   * and its flushingMemTables are all flushed, then the flush thread will close this file.)
   */
  private volatile boolean shouldClose;

  /** working memtable */
  private IMemTable
      workMemTable; // 每个TsFileProcessor有着属于自己的workMemTable，它存放了该TsFile的所有不同设备下的所有传感器Chunk的memTable（IWritableMemChunk类对象）

  /** last flush time to flush the working memtable */
  private long lastWorkMemtableFlushTime;

  /** this callback is called before the workMemtable is added into the flushingMemTables. */
  private final UpdateEndTimeCallBack updateLatestFlushTimeCallback;

  /** Wal log node */
  private WriteLogNode logNode;

  /** whether it's a sequence file or not */
  private final boolean sequence;

  /** total memtable size for mem control */
  private long totalMemTableSize;

  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE =
      "{}: {} get flushQueryLock write lock released";

  /** close file listener */
  private List<CloseFileListener> closeFileListeners = new ArrayList<>();

  /** flush file listener */
  private List<FlushListener> flushListeners = new ArrayList<>();

  @SuppressWarnings("squid:S107")
  TsFileProcessor(
      String storageGroupName,
      File tsfile,
      StorageGroupInfo storageGroupInfo,
      CloseFileListener closeTsFileCallback,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback,
      boolean sequence)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = new TsFileResource(tsfile, this);
    this.storageGroupInfo = storageGroupInfo;
    this.writer = new RestorableTsFileIOWriter(tsfile);
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("create a new tsfile processor {}", tsfile.getAbsolutePath());
    flushListeners.add(new WALFlushListener(this));
    closeFileListeners.add(closeTsFileCallback);
  }

  @SuppressWarnings("java:S107") // ignore number of arguments
  public TsFileProcessor(
      String storageGroupName,
      StorageGroupInfo storageGroupInfo,
      TsFileResource tsFileResource,
      CloseFileListener closeUnsealedTsFileProcessor,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback,
      boolean sequence,
      RestorableTsFileIOWriter writer) {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = tsFileResource;
    this.storageGroupInfo = storageGroupInfo;
    this.writer = writer;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("reopen a tsfile processor {}", tsFileResource.getTsFile());
    flushListeners.add(new WALFlushListener(this));
    closeFileListeners.add(closeUnsealedTsFileProcessor);
  }

  /**
   * insert data in an InsertRowPlan into the workingMemtable. //每个TsFile对应一个`workMemTable
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan)
      throws
          WriteProcessException { // 该方法做的事：（1）若当前TsFileProcessor的workMemTable是空，则创建一个（2）统计当前写入计划新增的内存占用，增加至TspInfo和SgInfo中（3）若系统配置是允许写前日志，则记录写前日志（4）写入workMemTable，即遍历该插入计划中每个待插入传感器的数值，往该传感器对应的memtable里的TVList写入待插入的数值,首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值（5）在插入操作后，要更新该TsFile对应TsFileResource对象里该设备数据的最大、最小时间戳

    if (workMemTable == null) { // 如果memtable是空
      if (enableMemControl) { // 如果系统开启了内存控制，就无需对memtable的创建进行控制，可以再需要的时候创建一个然后在MemTableManager管理类里把memtable数量+1
        workMemTable = new PrimitiveMemTable(enableMemControl); // 新建一个PrimitiveMemTable的memtable
        MemTableManager.getInstance()
            .addMemtableNumber(); // 在memtable管理类MemTableManager里把memtable数量+1
      } else { // 若系统没有开启内存控制，就需要通过下面的方法查看是否还有内存可以创建memtable，若已存在的memtable已经大于系统设置的数量，则不行创建，该方法侧面实现了内存控制
        workMemTable = MemTableManager.getInstance().getAvailableMemTable(storageGroupName);
      }
    }

    long[] memIncrements = null; // 存储此次insertRowPlan增加的内存占用
    if (enableMemControl) { // 若开启了内存控制，则
      memIncrements =
          checkMemCostAndAddToTspInfo(insertRowPlan); // 统计当前写入计划新增的内存占用，增加至TspInfo和SgInfo中
    }

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) { // 若系统配置是允许写前日志，则记录写前日志
      try {
        getLogNode().write(insertRowPlan); // 往对应的WAL日志节点写入该插入计划
      } catch (Exception e) {
        if (enableMemControl && memIncrements != null) {
          rollbackMemoryInfo(memIncrements);
        }
        throw new WriteProcessException(
            String.format(
                "%s: %s write WAL failed",
                storageGroupName, tsFileResource.getTsFile().getAbsolutePath()),
            e);
      }
    }

    workMemTable.insert(
        insertRowPlan); // 写入workMemTable，即遍历该插入计划中每个待插入传感器的数值，往该传感器对应的memtable里的TVList写入待插入的数值,首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值

    // 下面是插入workMemTable后要执行的操作
    // update start time of this memtable
    tsFileResource.updateStartTime( // 每插入一条数据，就要更新该TsFile对应的TsFileResource里该设备对应的数据最小时间戳
        insertRowPlan.getPrefixPath().getFullPath(), insertRowPlan.getTime());
    // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) { // 对于顺序TsFile，我们只要在该文件被封口前更新对应TsFileResource里该设备对应的数据最大时间戳；而对于乱序TsFile,则需要在每次插入后更新对应TsFileResource里该设备对应的数据最大时间戳
      tsFileResource.updateEndTime(
          insertRowPlan.getPrefixPath().getFullPath(), insertRowPlan.getTime());
    }
    tsFileResource.updatePlanIndexes(insertRowPlan.getIndex());
  }

  /**
   * insert batch data of insertTabletPlan into the workingMemtable. The rows to be inserted are in
   * the range [start, end). Null value in each column values will be replaced by the subsequent
   * non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   *
   * @param insertTabletPlan insert a tablet of a device
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   */
  public void insertTablet(
      InsertTabletPlan insertTabletPlan, int start, int end, TSStatus[] results)
      throws WriteProcessException {

    if (workMemTable == null) {
      if (enableMemControl) {
        workMemTable = new PrimitiveMemTable(enableMemControl);
        MemTableManager.getInstance().addMemtableNumber();
      } else {
        workMemTable = MemTableManager.getInstance().getAvailableMemTable(storageGroupName);
      }
    }

    long[] memIncrements = null;
    try {
      if (enableMemControl) {
        memIncrements = checkMemCostAndAddToTspInfo(insertTabletPlan, start, end);
      }
    } catch (WriteProcessException e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, e.getMessage());
      }
      throw new WriteProcessException(e);
    }

    try {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        insertTabletPlan.setStart(start);
        insertTabletPlan.setEnd(end);
        getLogNode().write(insertTabletPlan);
      }
    } catch (Exception e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      if (enableMemControl && memIncrements != null) {
        rollbackMemoryInfo(memIncrements);
      }
      throw new WriteProcessException(e);
    }

    try {
      workMemTable.insertTablet(insertTabletPlan, start, end);
    } catch (WriteProcessException e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      throw new WriteProcessException(e);
    }

    for (int i = start; i < end; i++) {
      results[i] = RpcUtils.SUCCESS_STATUS;
    }
    tsFileResource.updateStartTime(
        insertTabletPlan.getPrefixPath().getFullPath(), insertTabletPlan.getTimes()[start]);

    // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(
          insertTabletPlan.getPrefixPath().getFullPath(), insertTabletPlan.getTimes()[end - 1]);
    }
    tsFileResource.updatePlanIndexes(insertTabletPlan.getIndex());
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private long[] checkMemCostAndAddToTspInfo(
      InsertRowPlan insertRowPlan) // 统计当前写入计划新增的内存占用，分别有如下三种类型的内存占用(memTableIncrement,
      // chunkMetadataIncrement新Chunk增加的chunkIndex,
      // textDataIncrement插入的字符串数值占用内存)，增加至TspInfo和SgInfo中
      throws WriteProcessException {
    // memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L; // 存储此次插入行为导致ChunkIndex增加的内存大小
    String deviceId = insertRowPlan.getPrefixPath().getFullPath();
    int columnIndex = 0;
    for (int i = 0;
        i < insertRowPlan.getMeasurementMNodes().length;
        i++) { // i要小于该insertRowPlan的传感器节点数量
      // skip failed Measurements
      if (insertRowPlan.getDataTypes()[columnIndex] == null
          || insertRowPlan.getMeasurements()[i] == null) {
        columnIndex++;
        continue;
      }
      if (workMemTable.checkIfChunkDoesNotExist(
          deviceId,
          insertRowPlan
              .getMeasurements()[
              i])) { // 检查该设备下的该传感器是否不存在IWritableMemChunk信息，若不存在IWritableMemChunk信息，则说明TsFile文件里不存在该设备的该传感器Chunk，因此要新建一个
        // ChunkMetadataIncrement
        IMeasurementSchema schema =
            insertRowPlan.getMeasurementMNodes()[i].getSchema(); // 获取该传感器节点的schema配置信息
        if (insertRowPlan.isAligned()) {
          chunkMetadataIncrement +=
              schema.getSubMeasurementsTSDataTypeList().size()
                  * ChunkMetadata.calculateRamSize(
                      schema.getSubMeasurementsList().get(0),
                      schema.getSubMeasurementsTSDataTypeList().get(0));
          memTableIncrement +=
              TVList.vectorTvListArrayMemSize(schema.getSubMeasurementsTSDataTypeList());
        } else {
          chunkMetadataIncrement += // 因为TsFile文件中新建Chunk就要新建对应的ChunkIndex,于是要计算此次插入行为导致该传感器Chunk对应的ChunkIndex增加的内存大小
              ChunkMetadata
                  .calculateRamSize( // 计算该新传感器Chunk对应的新ChunkIndex占用的内存大小（原始固定大小+传感器ID名称字符串大小+数据类型对象大小）
                      insertRowPlan.getMeasurements()[i],
                      insertRowPlan.getDataTypes()[columnIndex]);
          memTableIncrement +=
              TVList.tvListArrayMemSize(
                  insertRowPlan
                      .getDataTypes()[
                      columnIndex]); // memtable新增的内存大小+该插入计划的新的Chunk的IWritableMemChunk的TVList占用的内存大小
        }
      } else { // 若该设备的该传感器存在IWritableMemChunk信息，即Chunk存在
        // here currentChunkPointNum >= 1
        int currentChunkPointNum = // 此设备的此传感器的Chunk的IWritableMemChunk的TVList里存放数据点数量
            // //这里的workMemtable其实就是指的是每个传感器Chunk的WritableMemChunk，它有TVList对象用来存放数据点
            workMemTable.getCurrentChunkPointNum(deviceId, insertRowPlan.getMeasurements()[i]);
        memTableIncrement +=
            (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE)
                    == 0 // 当传感器的Chunk的IWritableMemChunk的TVList里的数据点存满数组（到达系统规定的32个数据点）后，就要增加此TVList的数组占用的内存，否则不增加。
                ? TVList.tvListArrayMemSize(insertRowPlan.getDataTypes()[columnIndex])
                : 0;
      }
      // TEXT data mem size
      if (insertRowPlan.getDataTypes()[columnIndex] == TSDataType.TEXT) {
        textDataIncrement +=
            MemUtils.getBinarySize(
                (Binary)
                    insertRowPlan.getValues()[columnIndex]); // 当插入的数据是Text字符串类型时，要额外计算该字符串占用的内存大小
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement); // 更新相应的内存使用情况
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  private long[] checkMemCostAndAddToTspInfo(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    if (start >= end) {
      return new long[] {0, 0, 0};
    }
    long[] memIncrements = new long[3]; // memTable, text, chunk metadata

    String deviceId = insertTabletPlan.getPrefixPath().getFullPath();

    int columnIndex = 0;
    for (int i = 0; i < insertTabletPlan.getMeasurementMNodes().length; i++) {
      // for aligned timeseries
      if (insertTabletPlan.isAligned()) {
        VectorMeasurementSchema vectorSchema =
            (VectorMeasurementSchema) insertTabletPlan.getMeasurementMNodes()[i].getSchema();
        Object[] columns = new Object[vectorSchema.getSubMeasurementsList().size()];
        for (int j = 0; j < vectorSchema.getSubMeasurementsList().size(); j++) {
          columns[j] = insertTabletPlan.getColumns()[columnIndex++];
        }
        updateVectorMemCost(vectorSchema, deviceId, start, end, memIncrements, columns);
        break;
      }
      // for non aligned
      else {
        // skip failed Measurements
        TSDataType dataType = insertTabletPlan.getDataTypes()[columnIndex];
        String measurement = insertTabletPlan.getMeasurements()[i];
        Object column = insertTabletPlan.getColumns()[columnIndex];
        columnIndex++;
        if (dataType == null || column == null || measurement == null) {
          continue;
        }
        updateMemCost(dataType, measurement, deviceId, start, end, memIncrements, column);
      }
    }
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return memIncrements;
  }

  private void updateMemCost(
      TSDataType dataType,
      String measurement,
      String deviceId,
      int start,
      int end,
      long[] memIncrements,
      Object column) {
    // memIncrements = [memTable, text, chunk metadata] respectively

    if (workMemTable.checkIfChunkDoesNotExist(deviceId, measurement)) {
      // ChunkMetadataIncrement
      memIncrements[2] += ChunkMetadata.calculateRamSize(measurement, dataType);
      memIncrements[0] +=
          ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
              * TVList.tvListArrayMemSize(dataType);
    } else {
      int currentChunkPointNum = workMemTable.getCurrentChunkPointNum(deviceId, measurement);
      if (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE == 0) {
        memIncrements[0] +=
            ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
                * TVList.tvListArrayMemSize(dataType);
      } else {
        int acquireArray =
            (end - start - 1 + (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE))
                / PrimitiveArrayManager.ARRAY_SIZE;
        memIncrements[0] +=
            acquireArray == 0 ? 0 : acquireArray * TVList.tvListArrayMemSize(dataType);
      }
    }
    // TEXT data size
    if (dataType == TSDataType.TEXT) {
      Binary[] binColumn = (Binary[]) column;
      memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
    }
  }

  private void updateVectorMemCost(
      VectorMeasurementSchema vectorSchema,
      String deviceId,
      int start,
      int end,
      long[] memIncrements,
      Object[] columns) {
    // memIncrements = [memTable, text, chunk metadata] respectively

    List<String> measurementIds = vectorSchema.getSubMeasurementsList();
    List<TSDataType> dataTypes = vectorSchema.getSubMeasurementsTSDataTypeList();
    if (workMemTable.checkIfChunkDoesNotExist(deviceId, vectorSchema.getMeasurementId())) {
      // ChunkMetadataIncrement
      memIncrements[2] +=
          dataTypes.size()
              * ChunkMetadata.calculateRamSize(measurementIds.get(0), dataTypes.get(0));
      memIncrements[0] +=
          ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
              * TVList.vectorTvListArrayMemSize(dataTypes);
    } else {
      int currentChunkPointNum =
          workMemTable.getCurrentChunkPointNum(deviceId, vectorSchema.getMeasurementId());
      if (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE == 0) {
        memIncrements[0] +=
            ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
                * TVList.vectorTvListArrayMemSize(dataTypes);
      } else {
        int acquireArray =
            (end - start - 1 + (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE))
                / PrimitiveArrayManager.ARRAY_SIZE;
        memIncrements[0] +=
            acquireArray == 0 ? 0 : acquireArray * TVList.vectorTvListArrayMemSize(dataTypes);
      }
    }
    // TEXT data size
    for (int i = 0; i < dataTypes.size(); i++) {
      if (dataTypes.get(i) == TSDataType.TEXT) {
        Binary[] binColumn = (Binary[]) columns[i];
        memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
      }
    }
  }

  private void updateMemoryInfo( // 更新内存使用信息
      long memTableIncrement, long chunkMetadataIncrement, long textDataIncrement)
      throws WriteProcessException {
    memTableIncrement += textDataIncrement;
    storageGroupInfo.addStorageGroupMemCost(memTableIncrement); // 往存储组storageGroupInfo增加相应的新增内存
    tsFileProcessorInfo.addTSPMemCost(
        chunkMetadataIncrement); // 往tsFileProcessorInfo增加鲜奶供应的ChunkIndex新增内存
    if (storageGroupInfo.needToReportToSystem()) { // 判断StorageGroupInfo是否要向系统SystemInfo上报
      try { // 下面进行上报系统
        if (!SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo, this)) {
          StorageEngine.blockInsertionIfReject(this); // 判断系统是否阻塞写入
        }
      } catch (WriteProcessRejectException e) { // 若所有的StorageGroup的内存占用和 >=系统分配的写入操作可使用内存，则抛异常
        // 恢复此次insert操作执行前的内存，即把此次操作新增的内存复原归零，当作此次操作没有执行
        storageGroupInfo.releaseStorageGroupMemCost(memTableIncrement);
        tsFileProcessorInfo.releaseTSPMemCost(chunkMetadataIncrement);
        SystemInfo.getInstance()
            .resetStorageGroupStatus(storageGroupInfo); // 向系统SystemInfo重新汇报此StorageGroupInfo的内存使用情况
        throw e; // 抛异常，不执行后续操作
      }
    }
    workMemTable.addTVListRamCost(memTableIncrement); // 往workMemTable的TVList占用内存增加新的内存使用
    workMemTable.addTextDataSize(textDataIncrement); // 往workMemTable增加数据点的内存使用情况
  }

  private void rollbackMemoryInfo(long[] memIncrements) {
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];

    memTableIncrement += textDataIncrement;
    storageGroupInfo.releaseStorageGroupMemCost(memTableIncrement);
    tsFileProcessorInfo.releaseTSPMemCost(chunkMetadataIncrement);
    SystemInfo.getInstance().resetStorageGroupStatus(storageGroupInfo);
    workMemTable.releaseTVListRamCost(memTableIncrement);
    workMemTable.releaseTextDataSize(textDataIncrement);
  }

  /**
   * Delete data which belongs to the timeseries `deviceId.measurementId` and the timestamp of which
   * <= 'timestamp' in the deletion. <br>
   *
   * <p>Delete data in both working MemTable and flushing MemTables.
   */
  public void deleteDataInMemory(
      Deletion deletion,
      Set<PartialPath>
          devicePaths) { // 根据指定的设备路径和删除操作信息，删除那些还在内存中的数据（包含存在各个传感器里未被flush的memtable的数据和正在被进行flush的memtable的数据）。注意：devicePaths确定了设备路径对象，而此时Deletion对象里的path时间序列路径的存储组是确定的，但设备路径还不确定，可能包含通配符*，因此可能有多个设备。eg:root.ln.*.*.*
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable != null) { // (1)删除还存在各个传感器里未被flush的memtable的数据：
        // 若该TsFileProcessor的workMemTable不为空，则说明该TsFile存在还未被写入的数据，他们暂存在对应传感器的memtable的TVList内存中，需要从memtable的TVList中删除对应的数据
        for (PartialPath device : devicePaths) { // 循环遍历设备路径对象
          workMemTable
              .delete( // 根据通配路径originalPath下的指定设备，找到所有待删除的明确时间序列路径，依次遍历该设备下的待删除传感器进行删除内存中数据：(1)
                  // 若此传感器的内存memtable里的所有数据都要被删除掉，则直接从该workMemTable的memTableMap中把此传感器和对应的memtable直接删除掉（2）若此传感器内不是所有数据都要被删，则把该传感器的memtable对象里的TVList里对应时间范围里的数据删掉即可，该传感器的memtable仍然保存在对应TSFileProcessor的workMemTable的memTableMap中
                  deletion.getPath(), device, deletion.getStartTime(), deletion.getEndTime());
        }
      }
      // flushing memTables are immutable, only record this deletion in these memTables for query
      if (!flushingMemTables
          .isEmpty()) { // (2) 若此TsFile文件对应的flushingMemTables队列不为空，说明存正在被or等待被flushing的workMemTable
        modsToMemtable.add(
            new Pair<>(
                deletion,
                flushingMemTables
                    .getLast())); // 在进行删除操作时，队列里存在正在被or等待被flushing的workMemTables，对于这些待被flush的workMemTable里的数据不能直接删除掉，就只能把该删除操作（Deletion类对象）和flushingMemTables队列中最后一个workMemtable放进该列表里。
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  public boolean shouldFlush() { // 判断TsFileProcessor的workMemTable是否要flush
    if (workMemTable == null) {
      return false;
    }
    if (workMemTable.shouldFlush()) { // 若该workMemTable的shouldFlush属性为true，则要flush
      logger.info(
          "The memtable size {} of tsfile {} reaches the mem control threshold",
          workMemTable.memSize(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    if (!enableMemControl
        && workMemTable.memSize()
            >= getMemtableSizeThresholdBasedOnSeriesNum()) { // 当系统没开启内存控制且workMemTable的内存占用量大于系统设置
      logger.info(
          "The memtable size {} of tsfile {} reaches the threshold",
          workMemTable.memSize(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    if (workMemTable
        .reachTotalPointNumThreshold()) { // 当该workMemTable所有数据点的数量(即该TsFile里每个传感器的memTable的数据点数量的总和)到达系统设置
      logger.info(
          "The avg series points num {} of tsfile {} reaches the threshold",
          workMemTable.getTotalPointsNum() / workMemTable.getSeriesNumber(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    return false;
  }

  private long getMemtableSizeThresholdBasedOnSeriesNum() {
    return config.getMemtableSizeThreshold();
  }

  public boolean shouldClose() { // 根据该TsFileProcessor的TsFile大小是否大于系统指定的大小，来判断是否需要关闭该文件
    long fileSize = tsFileResource.getTsFileSize(); // 获取该Resource对应TsFile的文件大小
    long fileSizeThreshold = sequence ? config.getSeqTsFileSize() : config.getUnSeqTsFileSize();

    if (fileSize >= fileSizeThreshold) {
      logger.info(
          "{} fileSize {} >= fileSizeThreshold {}",
          tsFileResource.getTsFilePath(),
          fileSize,
          fileSizeThreshold);
    }
    return fileSize >= fileSizeThreshold; // 当该TsFileProcessor的TsFile大小大于系统指定的大小，则返回需要关闭该文件，否则不关闭
  }

  void syncClose() {
    logger.info(
        "Sync close file: {}, will firstly async close it",
        tsFileResource.getTsFile().getAbsolutePath());
    if (shouldClose) {
      return;
    }
    synchronized (flushingMemTables) {
      try {
        asyncClose();
        logger.info("Start to wait until file {} is closed", tsFileResource);
        long startTime = System.currentTimeMillis();
        while (!flushingMemTables.isEmpty()) {
          flushingMemTables.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000 && !flushingMemTables.isEmpty()) {
            logger.warn(
                "{} has spent {}s for waiting flushing one memtable; {} left (first: {}). FlushingManager info: {}",
                this.tsFileResource.getTsFile().getAbsolutePath(),
                (System.currentTimeMillis() - startTime) / 1000,
                flushingMemTables.size(),
                flushingMemTables.getFirst(),
                FlushManager.getInstance());
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "{}: {} wait close interrupted",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
        Thread.currentThread().interrupt();
      }
    }
    logger.info("File {} is closed synchronously", tsFileResource.getTsFile().getAbsolutePath());
  }

  /** async close one tsfile, register and close it by another thread */
  void
      asyncClose() { // 异步关闭该TsFileProcessor对应的TsFile文件，在关闭前需要判断该TsFileProcessor的workMemTable是否需要Flush，若要则进行Flush，然后我们要另起一个线程去关闭该TSFile文件。
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {

      if (logger.isInfoEnabled()) {
        if (workMemTable != null) { // 如果该TsFileProcessor的workMemTable不为空
          logger.info(
              "{}: flush a working memtable in async close tsfile {}, memtable size: {}, tsfile "
                  + "size: {}, plan index: [{}, {}]",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              workMemTable.memSize(),
              tsFileResource.getTsFileSize(),
              workMemTable.getMinPlanIndex(),
              workMemTable.getMaxPlanIndex());
        } else {
          logger.info(
              "{}: flush a NotifyFlushMemTable in async close tsfile {}, tsfile size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              tsFileResource.getTsFileSize());
        }
      }

      if (shouldClose) { // If shouldClose == true and its workMemTables are all flushed, then the
        // flush thread will close this file.
        return;
      }
      // when a flush thread serves this TsFileProcessor (because the processor is submitted by
      // registerTsFileProcessor()), the thread will seal the corresponding TsFile and
      // execute other cleanup works if "shouldClose == true and flushingMemTables is empty".

      // To ensure there must be a flush thread serving this processor after the field `shouldClose`
      // is set true, we need to generate a NotifyFlushMemTable as a signal task and submit it to
      // the FlushManager.

      // we have to add the memtable into flushingList first and then set the shouldClose tag.
      // see https://issues.apache.org/jira/browse/IOTDB-510
      IMemTable tmpMemTable =
          workMemTable == null || workMemTable.memSize() == 0
              ? new NotifyFlushMemTable()
              : workMemTable;

      try {
        // When invoke closing TsFile after insert data to memTable, we shouldn't flush until invoke
        // flushing memTable in System module.
        addAMemtableIntoFlushingList(tmpMemTable);
        logger.info("Memtable {} has been added to flushing list", tmpMemTable);
        shouldClose = true;
      } catch (Exception e) {
        logger.error(
            "{}: {} async close failed, because",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup Tips: I am
   * trying to solve this issue by checking whether the table exist before wait()
   */
  public void syncFlush() throws IOException {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled() && tmpMemTable.isSignalMemTable()) {
        logger.debug(
            "{}: {} add a signal memtable into flushing memtable list when sync flush",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
      addAMemtableIntoFlushingList(tmpMemTable);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }

    synchronized (tmpMemTable) {
      try {
        long startWait = System.currentTimeMillis();
        while (flushingMemTables.contains(tmpMemTable)) {
          tmpMemTable.wait(1000);

          if ((System.currentTimeMillis() - startWait) > 60_000) {
            logger.warn(
                "has waited for synced flushing a memtable in {} for 60 seconds.",
                this.tsFileResource.getTsFile().getAbsolutePath());
            startWait = System.currentTimeMillis();
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "{}: {} wait flush finished meets error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** put the working memtable into flushing list and set the working memtable to null */
  public void asyncFlush() { // 把该TSFile的workMemTable进行异步Flush，把该TSFile的workMemTable放入flushing
    // list中，并把workMemTable清空为null
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable == null) {
        return;
      }
      logger.info(
          "Async flush a memtable to tsfile: {}", tsFileResource.getTsFile().getAbsolutePath());
      addAMemtableIntoFlushingList(workMemTable); // 把该TSFile的workMemTable放入flushing list中
    } catch (Exception e) {
      logger.error(
          "{}: {} add a memtable into flushing list failed",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * this method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private void addAMemtableIntoFlushingList(IMemTable tobeFlushed)
      throws IOException { // 该方法把传过来的workMemTable放入flushing队列中，并把其清空
    if (!tobeFlushed.isSignalMemTable()
        && (!updateLatestFlushTimeCallback.call(this) || tobeFlushed.memSize() == 0)) {
      logger.warn(
          "This normal memtable is empty, skip it in flush. {}: {} Memetable info: {}",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          tobeFlushed.getMemTableMap());
      return;
    }

    for (FlushListener flushListener : flushListeners) {
      flushListener.onFlushStart(tobeFlushed);
    }

    if (enableMemControl) { // 若开启了内存空值，则把此待被flush的workMemtable占用的数据内存大小加入系统相关设置里
      SystemInfo.getInstance().addFlushingMemTableCost(tobeFlushed.getTVListsRamCost());
    }
    flushingMemTables.addLast(tobeFlushed); // 往flushingMemTables队列的最后加入此待被flush的workMemtable
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} Memtable (signal = {}) is added into the flushing Memtable, queue size = {}",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          tobeFlushed.isSignalMemTable(),
          flushingMemTables.size());
    }

    if (!tobeFlushed.isSignalMemTable()) {
      totalMemTableSize += tobeFlushed.memSize();
    }
    workMemTable = null;
    lastWorkMemtableFlushTime = System.currentTimeMillis();
    FlushManager.getInstance().registerTsFileProcessor(this);
  }

  /** put back the memtable to MemTablePool and make metadata in writer visible */
  private void releaseFlushedMemTable(IMemTable memTable) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      writer.makeMetadataVisible();
      if (!flushingMemTables.remove(memTable)) {
        logger.warn(
            "{}: {} put the memtable (signal={}) out of flushingMemtables but it is not in the queue.",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable());
      } else if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} memtable (signal={}) is removed from the queue. {} left.",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable(),
            flushingMemTables.size());
      }
      memTable.release();
      MemTableManager.getInstance().decreaseMemtableNumber();
      if (enableMemControl) {
        // reset the mem cost in StorageGroupProcessorInfo
        storageGroupInfo.releaseStorageGroupMemCost(memTable.getTVListsRamCost());
        if (logger.isDebugEnabled()) {
          logger.debug(
              "[mem control] {}: {} flush finished, try to reset system memcost, "
                  + "flushing memtable list size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              flushingMemTables.size());
        }
        // report to System
        SystemInfo.getInstance().resetStorageGroupStatus(storageGroupInfo);
        SystemInfo.getInstance().resetFlushingMemTableCost(memTable.getTVListsRamCost());
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} flush finished, remove a memtable from flushing list, "
                + "flushing memtable list size: {}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            flushingMemTables.size());
      }
    } catch (Exception e) {
      logger.error("{}: {}", storageGroupName, tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void
      flushOneMemTable() { // 把flushingMemTables队列中第一个workMemtable的数据flush到物理盘上，然后把该workMemtable从flushingMemTables队列中挪走
    IMemTable memTableToFlush = flushingMemTables.getFirst();

    // signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      try {
        writer.mark();
        MemTableFlushTask flushTask =
            new MemTableFlushTask(memTableToFlush, writer, storageGroupName);
        flushTask.syncFlushMemTable();
      } catch (Exception e) {
        if (writer == null) {
          logger.info(
              "{}: {} is closed during flush, abandon flush task",
              storageGroupName,
              tsFileResource.getTsFile().getName());
          synchronized (flushingMemTables) {
            flushingMemTables.notifyAll();
          }
        } else {
          logger.error(
              "{}: {} meet error when flushing a memtable, change system mode to read-only",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              e);
          IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
          try {
            logger.error(
                "{}: {} IOTask meets error, truncate the corrupted data",
                storageGroupName,
                tsFileResource.getTsFile().getName(),
                e);
            writer.reset();
          } catch (IOException e1) {
            logger.error(
                "{}: {} Truncate corrupted data meets error",
                storageGroupName,
                tsFileResource.getTsFile().getName(),
                e1);
          }
          Thread.currentThread().interrupt();
        }
      }
    }

    for (FlushListener flushListener : flushListeners) {
      flushListener.onFlushEnd(memTableToFlush);
    }

    try {
      Iterator<Pair<Modification, IMemTable>> iterator = modsToMemtable.iterator();
      while (iterator.hasNext()) { // 循环遍历先前对在内存中正在or待被flush的workMemtable存在对应的删除操作，
        Pair<Modification, IMemTable> entry =
            iterator.next(); // 获取的是数据结构为：（删除操作对象，此删除操作当下该TsFile的最新的待被flush的workMemtable）
        if (entry.right.equals(
            memTableToFlush)) { // 若当前遍历的modsToMemtable里的该workMemtable正好就是待被flush的workMemtable，则把对应的删除操作记入对应的mods文件里
          entry.left.setFileOffset(
              tsFileResource.getTsFileSize()); // 此时记录的offset位置是flush此workMemtable后TsFile文件的长度
          this.tsFileResource.getModFile().write(entry.left); // 把对应的删除操作记入对应的mods文件里。这样一来，该删除操作就会
          tsFileResource.getModFile().close();
          iterator
              .remove(); // flush完此workMemtable并把其在待flush时存在的删除操作写入到mods文件后，就把该项从modsToMemtable列表中移除
          logger.info(
              "[Deletion] Deletion with path: {}, time:{}-{} written when flush memtable",
              entry.left.getPath(),
              ((Deletion) (entry.left)).getStartTime(),
              ((Deletion) (entry.left)).getEndTime());
        }
      }
    } catch (IOException e) {
      logger.error(
          "Meet error when writing into ModificationFile file of {} ",
          tsFileResource.getTsFile().getName(),
          e);
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} try get lock to release a memtable (signal={})",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          memTableToFlush.isSignalMemTable());
    }
    // for sync flush
    synchronized (memTableToFlush) {
      releaseFlushedMemTable(memTableToFlush);
      memTableToFlush.notifyAll();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} released a memtable (signal={}), flushingMemtables size ={}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTableToFlush.isSignalMemTable(),
            flushingMemTables.size());
      }
    }

    if (shouldClose && flushingMemTables.isEmpty() && writer != null) {
      try {
        writer.mark();
        updateCompressionRatio(memTableToFlush);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: {} flushingMemtables is empty and will close the file",
              storageGroupName,
              tsFileResource.getTsFile().getName());
        }
        endFile();
        if (logger.isDebugEnabled()) {
          logger.debug("{} flushingMemtables is clear", storageGroupName);
        }
      } catch (Exception e) {
        logger.error(
            "{} meet error when flush FileMetadata to {}, change system mode to read-only",
            storageGroupName,
            tsFileResource.getTsFile().getAbsolutePath(),
            e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error(
              "{}: {} truncate corrupted data meets error",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              e1);
        }
        logger.error(
            "{}: {} marking or ending file meet error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      }
      // for sync close
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} try to get flushingMemtables lock.",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
      synchronized (flushingMemTables) {
        flushingMemTables.notifyAll();
      }
    }
  }

  private void updateCompressionRatio(IMemTable memTableToFlush) {
    try {
      double compressionRatio = ((double) totalMemTableSize) / writer.getPos();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "The compression ratio of tsfile {} is {}, totalMemTableSize: {}, the file size: {}",
            writer.getFile().getAbsolutePath(),
            compressionRatio,
            totalMemTableSize,
            writer.getPos());
      }
      if (compressionRatio == 0 && !memTableToFlush.isSignalMemTable()) {
        logger.error(
            "{} The compression ratio of tsfile {} is 0, totalMemTableSize: {}, the file size: {}",
            storageGroupName,
            writer.getFile().getAbsolutePath(),
            totalMemTableSize,
            writer.getPos());
      }
      CompressionRatio.getInstance().updateRatio(compressionRatio);
    } catch (IOException e) {
      logger.error(
          "{}: {} update compression ratio failed",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    }
  }

  /** end file and write some meta */
  private void endFile() throws IOException, TsFileProcessorException { // 关闭该TsFile
    logger.info("Start to end file {}", tsFileResource);
    long closeStartTime = System.currentTimeMillis();
    tsFileResource.serialize(); // 将TsFileResource对象序列化到本地.resource文件里
    writer.endFile();
    logger.info("Ended file {}", tsFileResource);

    // remove this processor from Closing list in StorageGroupProcessor,
    // mark the TsFileResource closed, no need writer anymore
    for (CloseFileListener closeFileListener : closeFileListeners) {
      closeFileListener.onClosed(this); // 调用关闭后的回调函数，该函数是Storage Group
      // Processor的closeUnsealedTsFileProcessorCallBack函数，里面会调用TsFileResource的close方法
    }

    if (enableMemControl) {
      tsFileProcessorInfo.clear();
      storageGroupInfo.closeTsFileProcessorAndReportToSystem(this);
    }
    if (logger.isInfoEnabled()) {
      long closeEndTime = System.currentTimeMillis();
      logger.info(
          "Storage group {} close the file {}, TsFile size is {}, "
              + "time consumption of flushing metadata is {}ms",
          storageGroupName,
          tsFileResource.getTsFile().getAbsoluteFile(),
          writer.getFile().length(),
          closeEndTime - closeStartTime);
    }

    writer = null;
  }

  public boolean isManagedByFlushManager() {
    return managedByFlushManager;
  }

  public void setManagedByFlushManager(boolean managedByFlushManager) {
    this.managedByFlushManager = managedByFlushManager;
  }

  /**
   * get WAL log node
   *
   * @return WAL log node
   */
  public WriteLogNode getLogNode() { // 获取WAL Log的节点
    if (logNode == null) {
      logNode =
          MultiFileLogNodeManager.getInstance()
              .getNode(
                  storageGroupName + "-" + tsFileResource.getTsFile().getName(),
                  storageGroupInfo.getWalSupplier());
    }
    return logNode;
  }

  /** close this tsfile */
  public void close() throws TsFileProcessorException {
    try {
      // when closing resource file, its corresponding mod file is also closed.
      tsFileResource.close();
      MultiFileLogNodeManager.getInstance()
          .deleteNode(
              storageGroupName + "-" + tsFileResource.getTsFile().getName(),
              storageGroupInfo.getWalConsumer());
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public int getFlushingMemTableSize() {
    return flushingMemTables.size();
  }

  RestorableTsFileIOWriter getWriter() {
    return writer;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  /** get modifications from a memtable */
  private List<Modification> getModificationsForMemtable(
      IMemTable
          memTable) { // 参数为在flushingMemtables队列中的某个待flush的workMemtable，获取此待flush的workMemtable所有的还未被处理的删除操作
    List<Modification> modifications = new ArrayList<>();
    boolean foundMemtable = false; // 该属性表明是否从modsToMemtable中找到给定的memTable
    for (Pair<Modification, IMemTable> entry :
        modsToMemtable) { // 遍历该列表，该列表存放了每个删除操作对应的一个workMemTable，这些删除操作都是对当下处在flushingMemTables列表中正在or准备被flushing的workMemtable,即删除操作的当下该TsFile存在待被flush的workMemTable
      if (foundMemtable || entry.right.equals(memTable)) { // Todo:什么意思？
        // //若modsToMemtable里存在该workMemtable，就说明该workMemtable在待flush时有删除操作，并且还没把此删除操作写入到本地mods文件里
        modifications.add(entry.left); // 往删除列表里加入此删除操作
        foundMemtable = true; //
      }
    }
    return modifications;
  }

  /**
   * construct a deletion list from a memtable
   *
   * @param memTable memtable
   * @param deviceId device id
   * @param measurement measurement name
   * @param timeLowerBound time water mark ,该参数表示允许的最小时间戳，小于该时间戳的数据就是超过了TTL的存活时间，就应该被删除
   */
  private List<TimeRange>
      constructDeletionList( // 对指定的此TsFile中待flush的memtable和设备、传感器ID获取那些还未被写入到mods文件里的删除记录，将它们对应的删除时间范围进行排序合并后返回
      IMemTable memTable, String deviceId, String measurement, long timeLowerBound)
          throws MetadataException {
    List<TimeRange> deletionList = new ArrayList<>(); // 待删除的时间范围列表
    deletionList.add(
        new TimeRange(
            Long.MIN_VALUE, timeLowerBound)); // 首先把小于等于TTL存活期的时间范围加入待删除列表里，因为那些数据超过TTL存活期，应该被删除
    for (Modification modification :
        getModificationsForMemtable(
            memTable)) { // 参数为在flushingMemtables队列中的某个待flush的workMemtable，遍历此待flush的workMemtable所有的还未被处理的删除操作
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion
                .getPath()
                .matchFullPath(
                    new PartialPath(deviceId, measurement)) // 若此删除操作要删除的时间序列路径与给定的设备和传感器匹配，则
            && deletion.getEndTime() > timeLowerBound) {
          long lowerBound =
              Math.max(
                  deletion.getStartTime(),
                  timeLowerBound); // 从删除操作的起使时间和数据TTL允许的最小时间戳中选择较大的作当前删除操作的起使时间
          deletionList.add(new TimeRange(lowerBound, deletion.getEndTime())); // 把当前删除操作加入列表里
        }
      }
    }
    return TimeRange.sortAndMerge(deletionList); // 将当前删除时间列表进行排序和合并
  }

  /**
   * get the chunk(s) in the memtable (one from work memtable and the other ones in flushing
   * memtables and then compact them into one TimeValuePairSorter). Then get the related
   * ChunkMetadata of data on disk.
   *
   * @param deviceId device id
   * @param measurementId measurements id
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void
      query( // 此方法根据给定的设备和传感器等参数，创建一个只读的TsFileResource对象，并把它放入tsfileResourcesForQuery列表里，用作查询
          String deviceId, // 此次查询操作里待查询的设备路径ID
          String measurementId, // 此次查询操作里待查询的传感器路径ID
          IMeasurementSchema schema, // 传感器配置类对象
          QueryContext context, // 此次查询的环境类对象
          List<TsFileResource>
              tsfileResourcesForQuery) // 此次查询需要用到的TsFileResource列表，其实该列表只有一个元素，存放的就是此TsFileResource对应的要给
          throws IOException, MetadataException {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} get flushQueryLock and hotCompactionMergeLock read lock",
          storageGroupName,
          tsFileResource.getTsFile().getName());
    }
    flushQueryLock.readLock().lock(); // 加读锁
    try {
      List<ReadOnlyMemChunk> readOnlyMemChunks = new ArrayList<>(); // 内存中的workMemtable快照列表，它是用作只读的
      for (IMemTable flushingMemTable :
          flushingMemTables) { // 循环遍历该TsFileResource里flushingMemTables列表中的每个待被flush的workMemtable，并为它们每个创建各自的只读ReadOnlyMemChunk类对象，它们相当于workMemtable的快照
        if (flushingMemTable.isSignalMemTable()) { // 若该workMemtable是PrimitiveMemTable类对象，则为false
          continue;
        }
        List<TimeRange> deletionList =
            constructDeletionList( // 对指定的此TsFile中待flush的memtable和设备、传感器ID获取那些还未被写入到mods文件里的删除记录，将它们对应的删除时间范围进行排序合并后返回
                flushingMemTable, deviceId, measurementId, context.getQueryTimeLowerBound());
        ReadOnlyMemChunk memChunk =
            flushingMemTable
                .query( // 根据给定的设备路径和传感器名，以及传感器配置类对象等属性获取该Chunk里排好序的TVList数据，并创建返回只读的内存Chunk类对象
                    deviceId,
                    measurementId,
                    schema,
                    context.getQueryTimeLowerBound(),
                    deletionList);
        if (memChunk != null) {
          readOnlyMemChunks.add(memChunk); // 往readOnlyMemChunks列表里加入非null的memChunk只读workMemtable对象
        }
      }
      if (workMemTable != null) {
        ReadOnlyMemChunk memChunk =
            workMemTable.query(
                deviceId, measurementId, schema, context.getQueryTimeLowerBound(), null);
        if (memChunk != null) {
          readOnlyMemChunks.add(memChunk);
        }
      }

      ModificationFile modificationFile = tsFileResource.getModFile(); // 获取此TsFile的mods文件类对象
      List<Modification> modifications =
          context.getPathModifications( // 从该TsFile的mods文件类对象里获取对该指定时间序列路径的所有删除操作，存入列表里
              modificationFile,
              new PartialPath(deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId));

      List<IChunkMetadata> chunkMetadataList = new ArrayList<>();
      if (schema instanceof VectorMeasurementSchema) { // 如果是vector
        List<ChunkMetadata> timeChunkMetadataList =
            writer.getVisibleMetadataList(deviceId, measurementId, schema.getType());
        List<List<ChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
        List<String> valueMeasurementIdList = schema.getSubMeasurementsList();
        List<TSDataType> valueDataTypeList = schema.getSubMeasurementsTSDataTypeList();
        for (int i = 0; i < valueMeasurementIdList.size(); i++) {
          valueChunkMetadataList.add(
              writer.getVisibleMetadataList(
                  deviceId, valueMeasurementIdList.get(i), valueDataTypeList.get(i)));
        }

        for (int i = 0; i < timeChunkMetadataList.size(); i++) {
          List<IChunkMetadata> valueChunkMetadata = new ArrayList<>();
          for (List<ChunkMetadata> chunkMetadata : valueChunkMetadataList) {
            valueChunkMetadata.add(chunkMetadata.get(i));
          }
          chunkMetadataList.add(
              new VectorChunkMetadata(timeChunkMetadataList.get(i), valueChunkMetadata));
        }
      } else { // 若不是vector
        chunkMetadataList =
            new ArrayList<>(
                writer.getVisibleMetadataList(deviceId, measurementId, schema.getType()));
      }

      QueryUtils.modifyChunkMetaData(chunkMetadataList, modifications);
      chunkMetadataList.removeIf(context::chunkNotSatisfy);

      // get in memory data
      if (!readOnlyMemChunks.isEmpty() || !chunkMetadataList.isEmpty()) {
        tsfileResourcesForQuery.add(
            new TsFileResource(readOnlyMemChunks, chunkMetadataList, tsFileResource));
      }
    } catch (QueryProcessException e) {
      logger.error(
          "{}: {} get ReadOnlyMemChunk has error",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    } finally {
      flushQueryLock.readLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} release flushQueryLock",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
  }

  public long getTimeRangeId() {
    return timeRangeId;
  }

  public void setTimeRangeId(long timeRangeId) {
    this.timeRangeId = timeRangeId;
  }

  /** release resource of a memtable */
  public void putMemTableBackAndClose() throws TsFileProcessorException {
    if (workMemTable != null) {
      workMemTable.release();
      workMemTable = null;
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public TsFileProcessorInfo getTsFileProcessorInfo() {
    return tsFileProcessorInfo;
  }

  public void setTsFileProcessorInfo(TsFileProcessorInfo tsFileProcessorInfo) {
    this.tsFileProcessorInfo = tsFileProcessorInfo;
  }

  public long getWorkMemTableRamCost() {
    return workMemTable != null ? workMemTable.getTVListsRamCost() : 0;
  }

  /** Return Long.MAX_VALUE if workMemTable is null */
  public long getWorkMemTableCreatedTime() {
    return workMemTable != null ? workMemTable.getCreatedTime() : Long.MAX_VALUE;
  }

  public long getLastWorkMemtableFlushTime() {
    return lastWorkMemtableFlushTime;
  }

  public boolean isSequence() {
    return sequence;
  }

  public void setWorkMemTableShouldFlush() {
    workMemTable.setShouldFlush();
  }

  public void addFlushListener(FlushListener listener) {
    flushListeners.add(listener);
  }

  public void addCloseFileListener(CloseFileListener listener) {
    closeFileListeners.add(listener);
  }

  public void addFlushListeners(Collection<FlushListener> listeners) {
    flushListeners.addAll(listeners);
  }

  public void addCloseFileListeners(Collection<CloseFileListener> listeners) {
    closeFileListeners.addAll(listeners);
  }

  public void submitAFlushTask() {
    this.storageGroupInfo.getStorageGroupProcessor().submitAFlushTaskWhenShouldFlush(this);
  }

  public boolean alreadyMarkedClosing() {
    return shouldClose;
  }
}
