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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
                    //索引节点里的每个条目格式为<子节点名称，偏移量>,代表该索引节点有哪些子节点以及对应在TsFile文件里的偏移量，可以把它理解为当前节点的子节点索引项
public class MetadataIndexEntry { //节点条目项类，即在IndexOfTimeSeriesIndex索引类里，有一个个节点，每个节点里就会有一条条的索引条目项，指明了设备节点或传感器节点的名称，以及对应在TsFile的偏移位置

  private String name;  //索引条目名称
  private long offset;  //索引条目对应指向的名称为name的节点或者TimeseriesIndex在该TsFile里的开始偏移位置

  public MetadataIndexEntry(String name, long offset) {
    this.name = name;
    this.offset = offset;
  }

  public String getName() {
    return name;
  }

  public long getOffset() {
    return offset;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String toString() {
    return "<" + name + "," + offset + ">";
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.writeVar(name, outputStream);
    byteLen += ReadWriteIOUtils.write(offset, outputStream);
    return byteLen;
  }

  public static MetadataIndexEntry deserializeFrom(ByteBuffer buffer) { //从buffer里反序列化读取名称和偏移量，并用此创建节点条目对象
    String name = ReadWriteIOUtils.readVarIntString(buffer);//从buffer缓存里读取当前节点条目的名称字符串
    long offset = ReadWriteIOUtils.readLong(buffer);  //从buffer缓存里读取当前节点条目的偏移量
    return new MetadataIndexEntry(name, offset);  //使用子节点名称和对应的偏移量创建节点条目对象
  }
}
