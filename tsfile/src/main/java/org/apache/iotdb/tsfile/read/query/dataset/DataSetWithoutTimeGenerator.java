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
package org.apache.iotdb.tsfile.read.query.dataset;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/** multi-way merging data set, no need to use TimeGenerator. */
public class DataSetWithoutTimeGenerator extends QueryDataSet {

  private List<AbstractFileSeriesReader> readers; //每个时间序列对应的文件序列读取器列表

  private List<BatchData> batchDataList;//存放每个时间序列的下个满足过滤器的Page的数据

  private List<Boolean> hasDataRemaining; //存放每个时间序列是否还存在下个未被读取的满足过滤器的page   。 若该列表的某元素为true，则说明某时间序列还有下个page，则batchDataList里对应序列的BatchData不为null

  /** heap only need to store time. */
  private PriorityQueue<Long> timeHeap; //时间戳队列，存放每个时间序列的当前page的最小起使时间戳 不重复。每次被使用就会拿走一个时间戳，先进先出

  private Set<Long> timeSet;  //时间戳集合，存放每个时间序列的当前page的起使时间戳 不重复。时间戳使用后不会被拿走

  /**
   * constructor of DataSetWithoutTimeGenerator.
   *
   * @param paths paths in List structure
   * @param dataTypes TSDataTypes in List structure
   * @param readers readers in List(FileSeriesReaderByTimestamp) structure
   * @throws IOException IOException
   */
  public DataSetWithoutTimeGenerator(
      List<Path> paths, List<TSDataType> dataTypes, List<AbstractFileSeriesReader> readers)
      throws IOException {
    super(paths, dataTypes);
    this.readers = readers;
    initHeap();//遍历当前查询结果集的每个时间序列获取对应的下个满足过滤器的Page的BatchData的第一个起使时间戳，去初始化timeHeap和timeSet
  }

  private void initHeap() throws IOException {//遍历当前查询结果集的每个时间序列获取对应的下个满足过滤器的Page的BatchData的第一个起使时间戳，去初始化timeHeap和timeSet
    hasDataRemaining = new ArrayList<>();//存放每个时间序列是否还有下个Page的数据
    batchDataList = new ArrayList<>();  //存放每个时间序列的下个Page的数据
    timeHeap = new PriorityQueue<>();
    timeSet = new HashSet<>();

    for (int i = 0; i < paths.size(); i++) {//遍历每个时间序列
      AbstractFileSeriesReader reader = readers.get(i);//获取当前遍历的序列的文件序列阅读器
      if (!reader.hasNextBatch()) {//若当前时间序列不存在满足过滤器的但没有被读取的Page的Chunk，则  （具体判断是若当前时间序列的当前Chunk阅读器为null或者当前Chunk里满足过滤器的数据都被读完了，则要初始化该时间序列的下一个Chunk对应的ChunkReader，去判断它是否有满足过滤器的数据还未被读取。
        batchDataList.add(new BatchData());//往batchDataList加入一个page量大小的空数据
        hasDataRemaining.add(false);//当前序列没有数据了
      } else {//若当前时间序列还存在满足过滤器的但没有被读取的Page的Chunk
        batchDataList.add(reader.nextBatch());// 读取该时间序列的当前Chunk的下一个page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里并返回。
        hasDataRemaining.add(true);//当前序列还有数据
      }
    }

    for (BatchData data : batchDataList) {//遍历每个时间序列的下个Page的数据BatchData
      if (data.hasCurrent()) {//如果当前序列的batchData有数据，则
        timeHeapPut(data.currentTime());//如果不存在此时间，则把该时间戳加入列表里
      }
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return timeHeap.size() > 0;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException { //获取该查询结果集的下一条RowRecord
    long minTime = timeHeapGet();//从时间队列里获取当前时间序列的当前page的最小时间戳

    RowRecord record = new RowRecord(minTime);  //拿取一个时间戳来初始化一条RowRecord数据，此时field列表还未空
    //下面开始初始化这个record的field列表
    for (int i = 0; i < paths.size(); i++) {//依次遍历该次查询的每个时间序列

      Field field = new Field(dataTypes.get(i));//创建一个field，并设置对应序列的数据类型

      if (!hasDataRemaining.get(i)) {//若当前时间序列是不存在未被读取的满足过滤器的page，则
        record.addField(null);  //则该条RecordRow对应的该时间序列field为null
        continue;
      }

      BatchData data = batchDataList.get(i);//获取该时间序列的下个满足过滤器的Page的数据

      if (data.hasCurrent() && data.currentTime() == minTime) {//如果该时间序列的该page有数据且当前时间戳为minTime，则
        putValueToField(data, field);
        data.next();

        if (!data.hasCurrent()) {
          AbstractFileSeriesReader reader = readers.get(i);
          if (reader.hasNextBatch()) {
            data = reader.nextBatch();
            if (data.hasCurrent()) {
              batchDataList.set(i, data);
              timeHeapPut(data.currentTime());
            } else {
              hasDataRemaining.set(i, false);
            }
          } else {
            hasDataRemaining.set(i, false);
          }
        } else {
          timeHeapPut(data.currentTime());
        }
        record.addField(field);
      } else {
        record.addField(null);
      }
    }
    return record;
  }

  /** keep heap from storing duplicate time. */
  private void timeHeapPut(long time) {
    if (!timeSet.contains(time)) {//如果不存在此时间，则把该时间戳加入列表里
      timeSet.add(time);
      timeHeap.add(time);
    }
  }

  private Long timeHeapGet() {//从时间队列里获取一个时间戳
    Long t = timeHeap.poll();
    timeSet.remove(t);
    return t;
  }

  private void putValueToField(BatchData col, Field field) {
    switch (col.getDataType()) {
      case BOOLEAN:
        field.setBoolV(col.getBoolean());
        break;
      case INT32:
        field.setIntV(col.getInt());
        break;
      case INT64:
        field.setLongV(col.getLong());
        break;
      case FLOAT:
        field.setFloatV(col.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(col.getDouble());
        break;
      case TEXT:
        field.setBinaryV(col.getBinary());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported" + col.getDataType());
    }
  }
}
