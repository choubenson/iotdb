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

//单个待合并序列在所有不同待合并乱序文件里的数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
public class CachedUnseqResourceMergeReader extends CachedPriorityMergeReader {

  //要注意，此处是该待合并序列在不同待合并乱序文件里的所有Chunk，因此Chunk之间可能存在overlap
  public CachedUnseqResourceMergeReader(List<Chunk> chunks, TSDataType dataType)
      throws IOException {
    super(dataType);
    int priorityValue = 1;
    for (Chunk chunk : chunks) {
      ChunkReader chunkReader = new ChunkReader(chunk, null);
      // the later chunk has higher priority
      //后面的乱序文件对应的优先级高，因为越后面代表文件越新
      addReader(new ChunkDataIterator(chunkReader), priorityValue++);
    }
  }
}
