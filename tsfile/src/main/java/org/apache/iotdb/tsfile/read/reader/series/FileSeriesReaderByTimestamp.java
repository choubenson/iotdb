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
package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

import java.io.IOException;
import java.util.List;

/**
 * Series reader is used to query one series of one tsfile, using this reader to query the value of
 * a series with given timestamps.
 */
public class FileSeriesReaderByTimestamp {//文件序列的时间戳阅读器，一个TsFile的一个时间序列对应有一个该类可以根据给定的时间戳来读取相应的数值value

  protected IChunkLoader chunkLoader; //该时间序列的chunk加载器
  protected List<IChunkMetadata> chunkMetadataList; //该时间序列的ChunkIndexli二标
  private int currentChunkIndex = 0;

  private ChunkReader chunkReader;  //该时间序列的chunk阅读器
  private long currentTimestamp;
  private BatchData data = null; // current batch data 当前某page的符合条件的PageData内容

  /** init with chunkLoader and chunkMetaDataList. */
  public FileSeriesReaderByTimestamp(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList) {
    this.chunkLoader = chunkLoader;
    this.chunkMetadataList = chunkMetadataList;
    currentTimestamp = Long.MIN_VALUE;
  }

  public TSDataType getDataType() {
    return chunkMetadataList.get(0).getDataType();
  }

  /** get value with time equals timestamp. If there is no such point, return null. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public Object getValueInTimestamp(long timestamp) throws IOException {//get value with time equals timestamp. If there is no such point, return null.
    this.currentTimestamp = timestamp;

    // first initialization, only invoked in the first time
    if (chunkReader == null) {
      if (!constructNextSatisfiedChunkReader()) {//获取当前时间序列的下一个符合条件的ChunkReader（ChunkReaderByTimestamp对象），返回true。若当前时序已经没有下个Chunk则返回false
        return null;
      }

      if (chunkReader.hasNextSatisfiedPage()) {
        data = chunkReader.nextPageData();// 读取Chunk下一个page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里并返回。每读完一个page，就把该pageReader从列表中移除
      } else {
        return null;
      }
    }

    while (data != null) {
      while (data.hasCurrent()) {
        if (data.currentTime() < timestamp) {
          data.next();
        } else {
          break;
        }
      }

      if (data.hasCurrent()) {
        if (data.currentTime() == timestamp) {
          Object value = data.currentValue();
          data.next();
          return value;
        }
        return null;
      } else {
        if (chunkReader.hasNextSatisfiedPage()) {
          data = chunkReader.nextPageData();
        } else if (!constructNextSatisfiedChunkReader()) {
          return null;
        }
      }
    }

    return null;
  }

  /**
   * Judge if the series reader has next time-value pair.
   *
   * @return true if has next, false if not.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public boolean hasNext() throws IOException {

    if (chunkReader != null) {
      if (data != null && data.hasCurrent()) {
        return true;
      }
      while (chunkReader.hasNextSatisfiedPage()) {
        data = chunkReader.nextPageData();
        if (data != null && data.hasCurrent()) {
          return true;
        }
      }
    }
    while (constructNextSatisfiedChunkReader()) {
      while (chunkReader.hasNextSatisfiedPage()) {
        data = chunkReader.nextPageData();
        if (data != null && data.hasCurrent()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean constructNextSatisfiedChunkReader() throws IOException {//获取当前时间序列的下一个符合条件的ChunkReader（ChunkReaderByTimestamp对象），返回true。若当前时序已经没有下个Chunk则返回false
    while (currentChunkIndex < chunkMetadataList.size()) {  //若当前序列的ChunkIndex索引小于其有的数量
      IChunkMetadata chunkMetaData = chunkMetadataList.get(currentChunkIndex++);//获取下一个ChunkIndex
      if (chunkSatisfied(chunkMetaData)) {//判断该ChunkIndex对应的Chunk是否满足条件。若当前CHunk的最后一个时间戳 大于等于 当前时间戳，则满足条件
        initChunkReader(chunkMetaData);//根据ChunkIndex从缓存获取对应的Chunk数据(并初始化其删除的数据范围和统计量)然后初始化当前ChunkReader为ChunkReaderByTimestamp
        ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp); //设置当前的时间戳
        return true;
      }
    }
    return false;
  }

  private void initChunkReader(IChunkMetadata chunkMetaData) throws IOException { //根据ChunkIndex从缓存获取对应的Chunk数据(并初始化其删除的数据范围和统计量)然后初始化当前ChunkReader为ChunkReaderByTimestamp
    Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);//根据ChunkIndex从缓存获取对应的Chunk数据，并初始化其删除的数据范围和统计量
    this.chunkReader = new ChunkReaderByTimestamp(chunk);//初始化当前的ChunkReader为ChunkReaderByTimestamp
  }

  private boolean chunkSatisfied(IChunkMetadata chunkMetaData) {
    return chunkMetaData.getEndTime() >= currentTimestamp;  //若当前CHunk的最后一个时间戳 大于等于 当前时间戳，则满足条件
  }
}
