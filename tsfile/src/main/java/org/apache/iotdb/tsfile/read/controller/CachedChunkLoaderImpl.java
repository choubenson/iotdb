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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;

/** Read one Chunk and cache it into a LRUCache, only used in tsfile module. */
public class CachedChunkLoaderImpl implements IChunkLoader {  //该Chunk加载是用在tsfile这个项目里的（与IOTDB server是隔绝的），有缓存存放Chunk

  private static final int DEFAULT_CHUNK_CACHE_SIZE = 1000;
  private TsFileSequenceReader reader;
  private LRUCache<ChunkMetadata, Chunk> chunkCache;//该缓存存放了每个ChunkIndex和对应的Chunk对象数据,这些在缓存里的Chunk不会存储其删除的数据范围和该Chunk的统计量

  public CachedChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
    this(fileSequenceReader, DEFAULT_CHUNK_CACHE_SIZE);
  }

  /**
   * constructor of ChunkLoaderImpl.
   *
   * @param fileSequenceReader file sequence reader
   * @param cacheSize cache size
   */
  public CachedChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {

    this.reader = fileSequenceReader;

    chunkCache =
        new LRUCache<ChunkMetadata, Chunk>(cacheSize) {//将ChunkIndex和对应的Chunk对象数据存入缓存

          @Override
          public Chunk loadObjectByKey(ChunkMetadata metaData) throws IOException {
            return reader.readMemChunk(metaData);//根据传来的ChunkIndex，使用TsFileInput读取本地文件并反序列化成对应的Chunk对象
          }
        };
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException { //根据ChunkIndex从缓存获取对应的Chunk数据，并初始化其删除的数据范围和统计量
    chunkMetaData.setFilePath(reader.getFileName());
    Chunk chunk = chunkCache.get(chunkMetaData);  //根据ChunkIndex从缓存里获取对应的Chunk对象,这些在缓存里的Chunk不会存储其删除的数据范围和该Chunk的统计量
    return new Chunk(
        chunk.getHeader(),
        chunk.getData().duplicate(),//复制一个该ChunkData的二进制缓存buffer
        chunkMetaData.getDeleteIntervalList(),
        chunkMetaData.getStatistics());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
