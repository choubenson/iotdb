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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractMemTable
    implements IMemTable { // TsFileProcessor的workMemTable里存放了该TsFile的不同设备下的所有传感器Chunk的memTable（IWritableMemChunk类对象）

  private final Map<String, Map<String, IWritableMemChunk>> memTableMap; // deviceId-> measureId->
  // IWritableMemChunk，该属性存放了每个设备下的每个传感器对应的memtable(IWritableMemChunk类对象)
  /**
   * The initial value is true because we want calculate the text data size when recover memTable!!
   */
  protected boolean disableMemControl = true;

  private boolean shouldFlush = false;
  private final int avgSeriesPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
  /** memory size of data points, including TEXT values */
  private long memSize = 0; // 该memtable里数据点占用的大小
  /**
   * memory usage of all TVLists memory usage regardless of whether these TVLists are full,
   * including TEXT values
   */
  private long tvListRamCost = 0;

  private int seriesNumber = 0;

  private long totalPointsNum = 0; // 该workMemTable所有数据点的数量，即该TsFile里每个传感器的memTable的数据点数量的总和

  private long totalPointsNumThreshold = 0;

  private long maxPlanIndex = Long.MIN_VALUE;

  private long minPlanIndex = Long.MAX_VALUE;

  private long createdTime = System.currentTimeMillis();

  public AbstractMemTable() {
    this.memTableMap = new HashMap<>();
  }

  public AbstractMemTable(Map<String, Map<String, IWritableMemChunk>> memTableMap) {
    this.memTableMap = memTableMap;
  }

  @Override
  public Map<String, Map<String, IWritableMemChunk>> getMemTableMap() {
    return memTableMap;
  }

  /**
   * check whether the given seriesPath is within this memtable.
   *
   * @return true if seriesPath is within this memtable
   */
  private boolean checkPath(
      String deviceId, String measurement) { // 判断此TsFile的workmemTable里的memTableMap是否包含此设备对应的此传感器
    return memTableMap.containsKey(deviceId) && memTableMap.get(deviceId).containsKey(measurement);
  }

  /**
   * create this memtable if it's not exist
   *
   * @param deviceId device id
   * @param schema measurement schema
   * @return this memtable
   */
  private IWritableMemChunk createIfNotExistAndGet(String deviceId, IMeasurementSchema schema) {
    Map<String, IWritableMemChunk> memSeries =
        memTableMap.computeIfAbsent(
            deviceId, k -> new HashMap<>()); // 获取该设备下的每个传感器对应的IWritableMemChunk信息对象，若不存在该设备的则创建一个

    return memSeries
        .computeIfAbsent( // 返回该设备下该传感器的IWritableMemChunk信息（即memtable），若不存在此传感器的memtable则创建一个
            schema.getMeasurementId(),
            k -> {
              seriesNumber++;
              totalPointsNumThreshold +=
                  ((long) avgSeriesPointNumThreshold * schema.getSubMeasurementsCount());
              return genMemSeries(schema);
            });
  }

  protected abstract IWritableMemChunk genMemSeries(IMeasurementSchema schema);

  @Override
  public void insert(
      InsertRowPlan
          insertRowPlan) { // 遍历该插入计划中每个待插入传感器的数值，往该传感器对应的memtable里的TVList写入待插入的数值,首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值
    updatePlanIndexes(
        insertRowPlan.getIndex()); // 更新该TsFileProcessor的workMemTable的minPlanIndex和maxPlanIndex
    Object[] values = insertRowPlan.getValues();

    IMeasurementMNode[] measurementMNodes = insertRowPlan.getMeasurementMNodes();
    int columnIndex = 0; // 用作临时的传感器索引，用于遍历该插入计划的指定设备下的一个个传感器
    if (insertRowPlan.isAligned()) { // 如果是对齐的
      IMeasurementMNode measurementMNode = measurementMNodes[0];
      if (measurementMNode != null) {
        // write vector
        Object[] vectorValue =
            new Object[measurementMNode.getSchema().getSubMeasurementsTSDataTypeList().size()];
        for (int j = 0; j < vectorValue.length; j++) {
          vectorValue[j] = values[columnIndex];
          columnIndex++;
        }
        memSize +=
            MemUtils.getVectorRecordSize(
                measurementMNode.getSchema().getSubMeasurementsTSDataTypeList(),
                vectorValue,
                disableMemControl);
        write(
            insertRowPlan.getPrefixPath().getFullPath(),
            measurementMNode.getSchema(),
            insertRowPlan.getTime(),
            vectorValue);
      }
    } else { // 如果是不对齐的
      for (IMeasurementMNode measurementMNode : measurementMNodes) {
        if (values[columnIndex] == null) { // 判断当前要插入传感器的值是否为空
          columnIndex++;
          continue;
        }
        memSize +=
            MemUtils.getRecordSize( // 根据数据类型计算给定数值的大小
                measurementMNode.getSchema().getType(), values[columnIndex], disableMemControl);

        write( // 往指定设备的指定传感器对应的memtable里的TVList写入待插入的数值,首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值
            insertRowPlan.getPrefixPath().getFullPath(), // 设备ID
            measurementMNode.getSchema(),
            insertRowPlan.getTime(),
            values[columnIndex]);
        columnIndex++; // 传感器索引+1，继续往下个传感器的memtable里写数据
      }
    }

    totalPointsNum += // 总的数据点等于原本的+此插入计划的传感器数量-失败传感器数量。每往一个传感器里插入一个数据就算一个数据点
        insertRowPlan.getMeasurements().length - insertRowPlan.getFailedMeasurementNumber();
  }

  @Override
  public void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    updatePlanIndexes(insertTabletPlan.getIndex());
    try {
      write(insertTabletPlan, start, end);
      memSize += MemUtils.getRecordSize(insertTabletPlan, start, end, disableMemControl);
      totalPointsNum +=
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void
      write( // 往指定设备的指定传感器对应的memtable里的TVList写入待插入的数值,首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值
      String deviceId, IMeasurementSchema schema, long insertTime, Object objectValue) {
    IWritableMemChunk memSeries =
        createIfNotExistAndGet(
            deviceId, schema); // 根据该设备和该传感器获取或创建对应传感器的Memtable，即IWritableMemChunk类对象
    memSeries.write(insertTime, objectValue); // 往该Chunk的memtable对应数据类型的TVList插入时间戳和数值，
    // 首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void write(InsertTabletPlan insertTabletPlan, int start, int end) {
    int columnIndex = 0;
    updatePlanIndexes(insertTabletPlan.getIndex());
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[columnIndex] == null) {
        columnIndex++;
        continue;
      }
      IWritableMemChunk memSeries =
          createIfNotExistAndGet(
              insertTabletPlan.getPrefixPath().getFullPath(),
              insertTabletPlan.getMeasurementMNodes()[i].getSchema());
      if (insertTabletPlan.isAligned()) {
        VectorMeasurementSchema vectorSchema =
            (VectorMeasurementSchema) insertTabletPlan.getMeasurementMNodes()[i].getSchema();
        Object[] columns = new Object[vectorSchema.getSubMeasurementsList().size()];
        BitMap[] bitMaps = new BitMap[vectorSchema.getSubMeasurementsList().size()];
        for (int j = 0; j < vectorSchema.getSubMeasurementsList().size(); j++) {
          columns[j] = insertTabletPlan.getColumns()[columnIndex];
          if (insertTabletPlan.getBitMaps() != null) {
            bitMaps[j] = insertTabletPlan.getBitMaps()[columnIndex];
          }
          columnIndex++;
        }
        memSeries.write(
            insertTabletPlan.getTimes(), columns, bitMaps, TSDataType.VECTOR, start, end);
        break;
      } else {
        memSeries.write(
            insertTabletPlan.getTimes(),
            insertTabletPlan.getColumns()[columnIndex],
            insertTabletPlan.getBitMaps() != null
                ? insertTabletPlan.getBitMaps()[columnIndex]
                : null,
            insertTabletPlan.getDataTypes()[columnIndex],
            start,
            end);
        columnIndex++;
      }
    }
  }

  @Override
  public boolean checkIfChunkDoesNotExist(
      String deviceId,
      String measurement) { // 检查该设备下的该传感器是否不存在memtable(IWritableMemChunk类对象)，若不存在返回true，存在返回false
    Map<String, IWritableMemChunk> memSeries =
        memTableMap.get(deviceId); // 获取该设备下每个传感器对应的IWritableMemChunk信息对象
    if (null == memSeries) { // 若内存不存在该设备的任何（传感器，IWritableMemChunk类）信息，则返回真
      return true;
    }

    return !memSeries.containsKey(measurement); // 判断内存是否不存在该设备的该传感器的IWritableMemChunk类信息，若不存在则返回真
  }

  @Override
  public int getCurrentChunkPointNum(
      String deviceId, String measurement) { // 返回此设备的此传感器的Chunk的TVList占用空间的大小
    Map<String, IWritableMemChunk> memSeries = memTableMap.get(deviceId);
    IWritableMemChunk memChunk = memSeries.get(measurement); // 获取该设备的该传感器对应的IWritableMemChunk对象
    return memChunk.getTVList().size();
  }

  @Override
  public int getSeriesNumber() {
    return seriesNumber;
  }

  @Override
  public long getTotalPointsNum() {
    return totalPointsNum;
  }

  @Override
  public long size() {
    long sum = 0;
    for (Map<String, IWritableMemChunk> seriesMap : memTableMap.values()) {
      for (IWritableMemChunk writableMemChunk : seriesMap.values()) {
        sum += writableMemChunk.count();
      }
    }
    return sum;
  }

  @Override
  public long memSize() {
    return memSize;
  }

  @Override
  public boolean reachTotalPointNumThreshold() {
    if (totalPointsNum == 0) {
      return false;
    }
    return totalPointsNum >= totalPointsNumThreshold;
  }

  @Override
  public void clear() {
    memTableMap.clear();
    memSize = 0;
    seriesNumber = 0;
    totalPointsNum = 0;
    totalPointsNumThreshold = 0;
    tvListRamCost = 0;
    maxPlanIndex = 0;
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public ReadOnlyMemChunk
      query( // 根据给定的设备路径和传感器名，以及传感器配置类对象等属性获取该Chunk里排好序的TVList数据，并创建返回只读的内存Chunk类对象
          String deviceId,
          String measurement,
          IMeasurementSchema partialVectorSchema, // 传感器配置类对象
          long ttlLowerBound,
          List<TimeRange> deletionList)
          throws IOException, QueryProcessException {
    if (partialVectorSchema.getType() == TSDataType.VECTOR) { // 如果当前是vector
      if (!memTableMap.containsKey(deviceId)) {
        return null;
      }
      IWritableMemChunk vectorMemChunk =
          memTableMap.get(deviceId).get(partialVectorSchema.getMeasurementId());
      if (vectorMemChunk == null) {
        return null;
      }

      List<String> measurementIdList = partialVectorSchema.getSubMeasurementsList();
      List<Integer> columns = new ArrayList<>();
      IMeasurementSchema vectorSchema = vectorMemChunk.getSchema();
      for (String queryingMeasurement : measurementIdList) {
        columns.add(vectorSchema.getSubMeasurementsList().indexOf(queryingMeasurement));
      }
      // get sorted tv list is synchronized so different query can get right sorted list reference
      TVList vectorTvListCopy = vectorMemChunk.getSortedTvListForQuery(columns);
      int curSize = vectorTvListCopy.size();
      return new ReadOnlyMemChunk(partialVectorSchema, vectorTvListCopy, curSize, deletionList);
    } else { // 若不是vector
      if (!checkPath(
          deviceId, measurement)) { // 若此TsFile的workmemTable里的memTableMap不包含此设备对应的此传感器，则返回null
        return null;
      }
      IWritableMemChunk memChunk =
          memTableMap
              .get(deviceId)
              .get(
                  partialVectorSchema
                      .getMeasurementId()); // 获取此TsFile的workmemTable里的memTableMap里指定设备和传感器对应的IWritableMemChunk
      // get sorted tv list is synchronized so different query can get right sorted list reference
      TVList chunkCopy =
          memChunk.getSortedTvListForQuery(); // 返回此Chunk的WritableMemChunk的被排过序的TVList
      int curSize = chunkCopy.size(); // 获取此Chunk存放的数据点的数量
      return new ReadOnlyMemChunk( // 新建只读的内存Chunk类对象，并返回
          measurement,
          partialVectorSchema.getType(),
          partialVectorSchema.getEncodingType(),
          chunkCopy,
          partialVectorSchema.getProps(),
          curSize,
          deletionList);
    }
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void delete( // 根据通配路径originalPath下的指定设备，找到所有待删除的明确时间序列路径，依次遍历该设备下的待删除传感器进行删除内存中数据：(1)
      // 若此传感器的内存memtable里的所有数据都要被删除掉，则直接从该workMemTable的memTableMap中把此传感器和对应的memtable直接删除掉（2）若此传感器内不是所有数据都要被删，则把该传感器的memtable对象里的TVList里对应时间范围里的数据删掉即可，该传感器的memtable仍然保存在对应TSFileProcessor的workMemTable的memTableMap中
      PartialPath originalPath,
      PartialPath devicePath,
      long startTimestamp,
      long
          endTimestamp) { // 注意：此时originalPath时间序列路径的存储组是确定的，但设备、传感器路径还不确定，可能包含通配符*，因此可能有多个设备。eg:root.ln.*.*.*
    Map<String, IWritableMemChunk> deviceMap =
        memTableMap.get(devicePath.getFullPath()); // 获取该设备下的每个传感器对应的memtable(IWritableMemChunk类对象)
    if (deviceMap == null) {
      return;
    }

    Iterator<Entry<String, IWritableMemChunk>> iter = deviceMap.entrySet().iterator();
    while (iter.hasNext()) { // 开始循环遍历deviceMap里每个传感器对应的memtable
      Entry<String, IWritableMemChunk> entry = iter.next();
      IWritableMemChunk chunk = entry.getValue(); // 获取到某传感器的memtable
      PartialPath fullPath = devicePath.concatNode(entry.getKey()); // 将设备路径和传感器名连接，获得完整的确定的时间序列路径对象
      IMeasurementSchema schema = chunk.getSchema(); // 获取该传感器的配置类对象
      if (originalPath.matchFullPath(fullPath)) { // 若通配的originalPath路径对象与具体的时间序列路径fullPath对象的路径匹配，则
        // （注意此处是遍历该设备下的所有传感器，可是有可能该方法第一个参数已经明确了某个传感器，因此要进行路径适配检查）
        if (startTimestamp == Long.MIN_VALUE
            && endTimestamp
                == Long.MAX_VALUE) { // 如果删除操作里要删除的时间范围是全部整数型范围（即说明该传感器的memtable的所有数据都要被删除），则
          iter.remove(); // 从deviceMap中（从该workMemTable的memTableMap中）把当前遍历到的 传感器和对应的memtable 直接删除掉
        }
        int deletedPointsNumber =
            chunk.delete(
                startTimestamp,
                endTimestamp); // 根据给出的时间范围，使用对应传感器的memtable对象删除其TVList里对应时间范围里的数据，返回被删除的数据点个数
        totalPointsNum -= deletedPointsNumber; // 更新此TsFile中删除操作后剩余所有数据点的数量
      }
      // for vector type
      else if (schema.getType() == TSDataType.VECTOR) {
        List<String> measurements = MetaUtils.getMeasurementsInPartialPath(originalPath);
        if (measurements.containsAll(schema.getSubMeasurementsList())) {
          if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
            iter.remove();
          }
          int deletedPointsNumber = chunk.delete(startTimestamp, endTimestamp);
          totalPointsNum -= deletedPointsNumber;
        }
      }
    }
  }

  @Override
  public void addTVListRamCost(long cost) {
    this.tvListRamCost += cost;
  }

  @Override
  public void releaseTVListRamCost(long cost) {
    this.tvListRamCost -= cost;
  }

  @Override
  public long getTVListsRamCost() {
    return tvListRamCost;
  }

  @Override
  public void addTextDataSize(long textDataSize) {
    this.memSize += textDataSize;
  }

  @Override
  public void releaseTextDataSize(long textDataSize) {
    this.memSize -= textDataSize;
  }

  @Override
  public void setShouldFlush() {
    shouldFlush = true;
  }

  @Override
  public boolean shouldFlush() {
    return shouldFlush;
  }

  @Override
  public void release() {
    for (Entry<String, Map<String, IWritableMemChunk>> entry : memTableMap.entrySet()) {
      for (Entry<String, IWritableMemChunk> subEntry : entry.getValue().entrySet()) {
        TVList list = subEntry.getValue().getTVList();
        if (list.getReferenceCount() == 0) {
          TVListAllocator.getInstance().release(list);
        }
      }
    }
  }

  @Override
  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  @Override
  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  void updatePlanIndexes(long index) {
    maxPlanIndex = Math.max(index, maxPlanIndex);
    minPlanIndex = Math.min(index, minPlanIndex);
  }

  @Override
  public long getCreatedTime() {
    return createdTime;
  }
}
