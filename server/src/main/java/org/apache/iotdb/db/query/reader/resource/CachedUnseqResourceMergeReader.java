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

package org.apache.iotdb.db.query.reader.resource;

import org.apache.iotdb.db.query.reader.chunk.ChunkDataIterator;
import org.apache.iotdb.db.query.reader.universal.CachedPriorityMergeReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

public class CachedUnseqResourceMergeReader extends CachedPriorityMergeReader {

  //第一个参数存放某个待合并序列在所有乱序文件里的所有Chunk，按序放入
  // 将某待合并序列在所有乱序文件的所有Chunk的第一个数据点放入heap优先级队列里（越后面的Chunk说明数据越新，因此优先级越高）
  public CachedUnseqResourceMergeReader(List<Chunk> chunks, TSDataType dataType)
      throws IOException {
    super(dataType);
    int priorityValue = 1;
    for (Chunk chunk : chunks) {
      ChunkReader chunkReader = new ChunkReader(chunk, null);
      // 将待合并序列在该乱序Chunk里的第一个数据点放入heap，越后面的Chunk说明数据越新，因此优先级越高
      addReader(new ChunkDataIterator(chunkReader), priorityValue++);
    }
  }
}
