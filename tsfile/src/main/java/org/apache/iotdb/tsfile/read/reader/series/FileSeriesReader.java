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
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

/**
 * Series reader is used to query one series of one TsFile, and this reader has a filter operating
 * on the same series.
 */
public class FileSeriesReader extends AbstractFileSeriesReader {//每个FileSeriesReader用来专门读取一个TsFile里一个时间序列

  public FileSeriesReader(  //根据该时间序列的Chunk加载器和器ChunkIndex列表和查询过滤器，创建该TsFile的该时间序列的专属文件序列阅读器
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    super(chunkLoader, chunkMetadataList, filter);
  }

  @Override
  protected void initChunkReader(IChunkMetadata chunkMetaData) throws IOException {//根据ChunkIndex使用加载器从缓存获取对应的Chunk数据（并初始化其删除的数据范围和统计量）然后用chunk对象和filter过滤器初始化该文件序列读取器的ChunkReader对象并初始化其所有满足条件（存在未被删除且满足过滤器的数据的page）的PageReader
    Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);//根据ChunkIndex使用加载器从缓存获取对应的Chunk数据，并初始化其删除的数据范围和统计量
    //Todo:此处要区分是否为Vector
   // if(chunkMetaData.getDataType().equals())
    this.chunkReader = new ChunkReader(chunk, filter);//创建对应的ChunkReader并初始化其所有满足条件（存在未被删除且满足过滤器的数据的page）的PageReader
  }

  @Override
  protected boolean chunkSatisfied(IChunkMetadata chunkMetaData) {  //根据传来的ChunkIndex的统计量和该时间序列的过滤器判断该Chunk的数据是否存在符合过滤条件的数据，若过滤器为null则直接为true
    return filter == null || filter.satisfy(chunkMetaData.getStatistics());
  }
}
