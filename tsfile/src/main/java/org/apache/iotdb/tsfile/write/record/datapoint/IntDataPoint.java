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
package org.apache.iotdb.tsfile.write.record.datapoint;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a subclass for Integer data type extends DataPoint.
 *
 * @see DataPoint DataPoint
 */
public class IntDataPoint extends DataPoint {

  private static final Logger LOG = LoggerFactory.getLogger(IntDataPoint.class);
  /** actual value. */
  private int value;

  /** constructor of IntDataPoint, the value type will be set automatically. */
  public IntDataPoint(String measurementId, int v) {
    super(TSDataType.INT32, measurementId);
    this.value = v;
  }

  @Override
  public void writeTo(
      long time,
      IChunkWriter
          writer) { // 根据指定的ChunkWriter，将给定的数据点（time,value）交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
    if (writer == null) {
      LOG.warn("given IChunkWriter is null, do nothing and return");
      return;
    }
    writer.write(time, value, false); // 使用指定ChunkWriter，把当前数据点的数据写入
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setInteger(int value) {
    this.value = value;
  }
}
