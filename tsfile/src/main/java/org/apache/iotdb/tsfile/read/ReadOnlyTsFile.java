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
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor;

import java.io.IOException;

public class ReadOnlyTsFile implements AutoCloseable {

  private TsFileSequenceReader fileReader;
  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;
  private TsFileExecutor tsFileExecutor;

  /** constructor, create ReadOnlyTsFile with TsFileSequenceReader. */
  public ReadOnlyTsFile(TsFileSequenceReader fileReader) throws IOException {
    this.fileReader = fileReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
    this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
  }

  public QueryDataSet query(QueryExpression queryExpression) throws IOException { //此方法允许用户对该TsFile查询里面的某些时间序列满足过滤器的数据，而这些时间序列和过滤器都是封装在QueryExpression查询表达式对象里的。
    return tsFileExecutor.execute(queryExpression);//根据给定的查询表达式，（1）首先把此次查询中属于该TsFile的时间序列路径加入列表里（一个查询可能涉及到多个不同TsFile的多个时间序列）（2）获取该次查询在该TsFile的每个时间序列对应的所有ChunkIndex放入该TsFile的元数据查询器里的chunkMetaDataCache缓存里（3）通过判断该次查询是否有过滤器，有的话则创建DataSetWithTimeGenerator查询结果集对象并返回，没有则创建DataSetWithoutTimeGenerator查询结果集对象返回
  }

  public QueryDataSet query(
      QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset)
      throws IOException {
    return tsFileExecutor.execute(queryExpression, partitionStartOffset, partitionEndOffset);
  }

  @Override
  public void close() throws IOException {
    fileReader.close();
  }
}
