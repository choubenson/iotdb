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

package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to cache <code>Chunk</code> of <code>ChunkMetaData</code> in IoTDB. The
 * caching strategy is LRU.
 */
public class ChunkCache { //该Chunk缓存类是用在IOTDB server系统里的，系统有配置是否允许开启缓存，以及采用的调度策略是LRU

  private static final Logger logger = LoggerFactory.getLogger(ChunkCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_CHUNK_CACHE =
      config.getAllocateMemoryForChunkCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();

  private final LoadingCache<ChunkMetadata, Chunk> lruCache;//该缓存存放了每个ChunkIndex和对应的Chunk对象数据,这些在缓存里的Chunk不会存储其删除的数据范围和该Chunk的统计量

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private ChunkCache() {
    if (CACHE_ENABLE) {
      logger.info("ChunkCache size = " + MEMORY_THRESHOLD_IN_CHUNK_CACHE);
    }
    lruCache =        //将ChunkIndex和对应的Chunk对象数据存入缓存lruCache里
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_CHUNK_CACHE)
            .weigher(
                (Weigher<ChunkMetadata, Chunk>)
                    (chunkMetadata, chunk) ->
                        (int)
                            (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                + RamUsageEstimator.sizeOf(chunk)))
            .recordStats()
            .build(
                chunkMetadata -> {
                  try {
                    TsFileSequenceReader reader =//从文件阅读器的管理类里获取该文件的顺序阅读器
                        FileReaderManager.getInstance()
                            .get(chunkMetadata.getFilePath(), chunkMetadata.isClosed());
                    return reader.readMemChunk(chunkMetadata); //根据传来的ChunkIndex，使用顺序读取器里的TsFileInput读取本地文件并反序列化成对应的Chunk对象
                  } catch (IOException e) {
                    logger.error("Something wrong happened in reading {}", chunkMetadata, e);
                    throw e;
                  }
                });
  }

  public static ChunkCache getInstance() {
    return ChunkCacheHolder.INSTANCE;
  }

  public Chunk get(ChunkMetadata chunkMetaData) throws IOException {
    return get(chunkMetaData, false);
  }

  public Chunk get(ChunkMetadata chunkMetaData, boolean debug) throws IOException {//若不允许缓存，则使用顺序读取器里的TsFileInput读取本地文件并反序列化成对应的Chunk对象；若允许缓存，则根据ChunkIndex从缓存获取对应的Chunk数据，并初始化其删除的数据范围和统计量
    if (!CACHE_ENABLE) {  //若系统是不允许ChunkIndex缓存的，则
      TsFileSequenceReader reader =//从文件阅读器的管理类里获取该文件的顺序阅读器
          FileReaderManager.getInstance()
              .get(chunkMetaData.getFilePath(), chunkMetaData.isClosed());//Get the reader of the file(tsfile or unseq tsfile) indicated by filePath. If the reader already exists, just get it from closedFileReaderMap or unclosedFileReaderMap depending on isClosing .Otherwise a new reader will be created and cached.
      Chunk chunk = reader.readMemChunk(chunkMetaData); //根据传来的ChunkIndex，使用顺序读取器里的TsFileInput读取本地文件并反序列化成对应的Chunk对象
      return new Chunk(
          chunk.getHeader(),
          chunk.getData().duplicate(),
          chunkMetaData.getDeleteIntervalList(),
          chunkMetaData.getStatistics());
    }

    //若系统允许缓存，则
    Chunk chunk = lruCache.get(chunkMetaData);  //从缓存里拿取对应ChunkIndex的Chunk对象

    if (debug) {
      DEBUG_LOGGER.info("get chunk from cache whose meta data is: " + chunkMetaData);
    }

    return new Chunk(//初始化其删除的数据范围和统计量
        chunk.getHeader(),
        chunk.getData().duplicate(),
        chunkMetaData.getDeleteIntervalList(),
        chunkMetaData.getStatistics());
  }

  public double calculateChunkHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return MEMORY_THRESHOLD_IN_CHUNK_CACHE;
  }

  public double getAverageLoadPenalty() {
    return lruCache.stats().averageLoadPenalty();
  }

  public long getAverageSize() {
    return entryAverageSize.get();
  }

  /** clear LRUCache. */
  public void clear() {
    lruCache.invalidateAll();
    lruCache.cleanUp();
  }

  public void remove(ChunkMetadata chunkMetaData) {
    lruCache.invalidate(chunkMetaData);
  }

  @TestOnly
  public boolean isEmpty() {
    return lruCache.asMap().isEmpty();
  }

  /** singleton pattern. */
  private static class ChunkCacheHolder {

    private static final ChunkCache INSTANCE = new ChunkCache();
  }
}
