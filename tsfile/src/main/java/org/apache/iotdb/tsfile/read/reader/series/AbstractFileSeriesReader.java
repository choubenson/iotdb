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

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

/** Series reader is used to query one series of one tsfile. */
public abstract class AbstractFileSeriesReader implements IBatchReader {//每个AbstractFileSeriesReader用来专门读取一个TsFile里一个时间序列（也就是说一个TsFile的一个时间序列的读取会有专属的AbstractFileSeriesReader）

  protected IChunkLoader chunkLoader; //该时间序列的Chunk加载器
  protected List<IChunkMetadata> chunkMetadataList;//该时间序列的ChunkIndex列表，由于一个TsFile里一个时间序列可能对应同设备ID的多个ChunkGroup里的Chunk，因此一个时间序列会有好几个Chunk，也就会有好几个ChunkIndex
  protected ChunkReader chunkReader;//该时间序列的某一个Chunk的读取器
  private int chunkToRead;  //下一个待读取Chunk的位置

  protected Filter filter;//该时间序列的读取查询过滤器

  /** constructor of FileSeriesReader. */
  public AbstractFileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    this.chunkLoader = chunkLoader;
    this.chunkMetadataList = chunkMetadataList;
    this.filter = filter;
    this.chunkToRead = 0;
  }

  @Override
  public boolean hasNextBatch() throws IOException {//判断当前时间序列的Chunk阅读器对应的当前Chunk是否还有存在满足过滤器的但没有被读取的Page。若当前时间序列的当前Chunk阅读器为null或者当前Chunk里满足过滤器的数据都被读完了，则要初始化该时间序列的下一个Chunk对应的ChunkReader，去判断它是否有满足过滤器的数据还未被读取。

    // current chunk has additional batch
    if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {  //若当前TsFile的该时间序列的ChunkReader不为null且该Chunk是否还存在满足过滤器的Page没有被读取，则返回真
      return true;
    }

    // current chunk does not have additional batch, init new chunk reader
    while (chunkToRead < chunkMetadataList.size()) {  //若下一个待读取Chunk的位置小于ChunkIndex列表的数量，即还未超过全部的数量，则

      IChunkMetadata chunkMetaData = nextChunkMeta();//从缓存里获取下一个ChunkIndex，并把chunkToRead+1
      if (chunkSatisfied(chunkMetaData)) {//根据传来的ChunkIndex的统计量和该时间序列的过滤器判断该Chunk的数据是否存在符合过滤条件的数据，若过滤器为null则直接为true
        // chunk metadata satisfy the condition
        initChunkReader(chunkMetaData);//根据ChunkIndex使用加载器从缓存获取对应的Chunk数据（并初始化其删除的数据范围和统计量）然后用chunk对象和filter过滤器初始化该文件序列读取器的ChunkReader对象并初始化其所有满足条件（存在未被删除且满足过滤器的数据的page）的PageReader

        if (chunkReader.hasNextSatisfiedPage()) {//若该Chunk是否还存在满足过滤器的Page没有被读取
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return chunkReader.nextPageData(); // 读取该时间序列的当前Chunk的下一个page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里并返回。每读完一个page，就把该pageReader从列表中移除
  }

  protected abstract void initChunkReader(IChunkMetadata chunkMetaData) throws IOException;

  protected abstract boolean chunkSatisfied(IChunkMetadata chunkMetaData);

  @Override
  public void close() throws IOException {
    chunkLoader.close();
  }

  private IChunkMetadata nextChunkMeta() {  //从缓存里获取下一个ChunkIndex，并把chunkToRead+1
    return chunkMetadataList.get(chunkToRead++);
  }
}
