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

package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;

import java.io.IOException;

/** To read one chunk from disk, and only used in iotdb server module */
public class DiskChunkLoader implements IChunkLoader {//该Chunk加载类是用在IOTDB server系统里的，有ChunkCache缓存，系统有配置是否允许开启缓存，以及采用的调度策略是LRU

  private final boolean debug;

  public DiskChunkLoader(boolean debug) {
    this.debug = debug;
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException {//若不允许缓存，则使用顺序读取器里的TsFileInput读取本地文件并反序列化成对应的Chunk对象；若允许缓存，则根据ChunkIndex从缓存获取对应的Chunk数据，并初始化其删除的数据范围和统计量
    return ChunkCache.getInstance().get(chunkMetaData, debug);
  }

  @Override
  public void close() {
    // do nothing
  }
}
