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
package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

import java.io.IOException;
import java.util.List;

public class TsFileTimeGenerator extends TimeGenerator {

  private IChunkLoader chunkLoader; //Chunk加载器
  private IMetadataQuerier metadataQuerier; //该TsFile的元数据查询器

  public TsFileTimeGenerator(
      IExpression iexpression, IChunkLoader chunkLoader, IMetadataQuerier metadataQuerier)
      throws IOException {
    this.chunkLoader = chunkLoader;
    this.metadataQuerier = metadataQuerier;

    super.constructNode(iexpression);//根据查询的表达式IExpression，若是：（1）SingleSeriesExpression则获取对应时间序列的文件序列阅读区并封装入叶子节点后并装入leafNodeCache缓存（2）否则是二元表达式，则依次对左、右子表达式递归次方法直至表达式为SingleSeriesExpression，则获取对应时间序列表达式的叶子节点（包含文件序列阅读器）
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {   //根据此次查询的单序列表达式获取对应时间序列的ChunkIndex列表，进而创建该TsFile的该时间序列的专属文件序列阅读器
    List<IChunkMetadata> chunkMetadataList =
        metadataQuerier.getChunkMetaDataList(expression.getSeriesPath()); //使用该TsFile的元数据查询器获取该次查询的单时间序列表达式里的时间序列获取对应的ChunkIndex列表
    return new FileSeriesReader(chunkLoader, chunkMetadataList, expression.getFilter());  //创建该TsFile的该时间序列的专属文件序列阅读器
  }

  @Override
  protected boolean isAscending() {
    return true;
  }
}
