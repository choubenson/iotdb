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
package org.apache.iotdb.db.query.reader.universal;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;

//元素类，一个序列里的每个数据点都对应一个该类对象，存放了某一序列的一个数据点、该数据点所在序列的数据点读取器，该数据点以及当前序列的优先级比较器
public class Element {

  //优先级比较器：我们令文件版本号越高的优先级越高；若版本号相同，则文件内偏移量offset越大的，优先级越高。compare方法返回负数代表当前对象的优先级低于参数对象的优先级
  public PriorityMergeReader.MergeReaderPriority priority;
  //某个序列的数据点读取器
  protected IPointReader reader;
  //该序列的下个数据点
  public TimeValuePair timeValuePair;

  public Element(
      IPointReader reader,
      TimeValuePair timeValuePair,
      PriorityMergeReader.MergeReaderPriority priority) {
    this.reader = reader;
    this.timeValuePair = timeValuePair;
    this.priority = priority;
  }

  public long currTime() {
    return timeValuePair.getTimestamp();
  }

  public TimeValuePair currPair() {
    return timeValuePair;
  }

  public boolean hasNext() throws IOException {
    return reader.hasNextTimeValuePair();
  }

  public void next() throws IOException {
    timeValuePair = reader.nextTimeValuePair();
  }

  public void close() throws IOException {
    reader.close();
  }

  public IPointReader getReader() {
    return reader;
  }

  public TimeValuePair getTimeValuePair() {
    return timeValuePair;
  }

  public PriorityMergeReader.MergeReaderPriority getPriority() {
    return priority;
  }
}
