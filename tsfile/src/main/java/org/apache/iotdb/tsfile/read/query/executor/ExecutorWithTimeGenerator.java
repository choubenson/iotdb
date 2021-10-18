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
package org.apache.iotdb.tsfile.read.query.executor;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class ExecutorWithTimeGenerator implements QueryExecutor {

  private IMetadataQuerier metadataQuerier; //该TsFile文件的元数据查询器
  private IChunkLoader chunkLoader; //Chunk加载器

  public ExecutorWithTimeGenerator(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  /**
   * All leaf nodes of queryFilter in queryExpression are SeriesFilters, We use a TimeGenerator to
   * control query processing. for more information, see DataSetWithTimeGenerator
   *
   * @return DataSet with TimeGenerator
   */
  @Override
  public DataSetWithTimeGenerator execute(QueryExpression queryExpression) throws IOException { //通过查询表达式计算该次查询的所有时间序列对应的是否有过滤器以及对应的“文件序列的时间戳阅读器”和一个TsFileTimeGenerator对象，以此创建DataSetWithTimeGenerator数据结果集对象并返回

    IExpression expression = queryExpression.getExpression(); //获取此次查询的表达式
    List<Path> selectedPathList = queryExpression.getSelectedSeries();  //获取此次查询的时间序列列表

    // get TimeGenerator by IExpression
    TimeGenerator timeGenerator = new TsFileTimeGenerator(expression, chunkLoader, metadataQuerier);  //使用此次查询的表达式和该TsFile的Chunk加载器和元数据查询器创建TsFileTimeGenerator对象，并用该查询表达式构建对应的序列阅读器树

    // the size of hasFilter is equal to selectedPathList, if a series has a filter, it is true,
    // otherwise false
    List<Boolean> cached =
        markFilterdPaths(expression, selectedPathList, timeGenerator.hasOrNode()); //根据给定的该次查询的表达式和该次查询涉及到的所有时间序列路径，判断该次查询的每个时间序列是否有数值过滤器。即数值过滤器是应用到哪些序列上的
    List<FileSeriesReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();  //存放了此次查询每个时间序列的“文件序列的时间戳阅读器”
    List<TSDataType> dataTypes = new ArrayList<>();//每个序列对应的数据类型

    Iterator<Boolean> cachedIterator = cached.iterator();
    Iterator<Path> selectedPathIterator = selectedPathList.iterator();
    while (cachedIterator.hasNext()) {
      boolean cachedValue = cachedIterator.next();
      Path selectedPath = selectedPathIterator.next();

      List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(selectedPath);//从该文件的元数据查询器的chunkMetaDataCache缓存里获取该时间序列路径对应的ChunkIndex列表
      if (chunkMetadataList.size() != 0) {  //若列表不为空
        dataTypes.add(chunkMetadataList.get(0).getDataType());  //往dataTypes列表加入该时间序列的数据类型
        if (cachedValue) {//若该次查询里的该时间序列存在过滤器，则
          readersOfSelectedSeries.add(null);//放入Null
          continue;
        }
        FileSeriesReaderByTimestamp seriesReader =  //创建该TsFile的该时间序列的“文件序列的时间戳阅读器”
            new FileSeriesReaderByTimestamp(chunkLoader, chunkMetadataList);
        readersOfSelectedSeries.add(seriesReader);
      } else {//若列表为空，则说明该TsFile里不存在该时间序列，则从selectedPathIterator、cachedIterator移除此序列的信息
        selectedPathIterator.remove();
        cachedIterator.remove();
      }
    }

    return new DataSetWithTimeGenerator(  //创建DataSetWithTimeGenerator数据结果集对象
        selectedPathList, cached, dataTypes, timeGenerator, readersOfSelectedSeries);
  }

  public static List<Boolean> markFilterdPaths( //根据给定的该次查询的表达式和该次查询涉及到的所有时间序列路径，判断该次查询的每个时间序列是否有数值过滤器。即数值过滤器是应用到哪些序列上的
      IExpression expression, List<Path> selectedPaths, boolean hasOrNode) {
    List<Boolean> cached = new ArrayList<>();
    if (hasOrNode) {  //若该Expression的阅读树有OR节点，则每个时间序列都标记为不存在Filter过滤器，标记存入cached列表里
      for (Path ignored : selectedPaths) {
        cached.add(false);
      }
      return cached;
    }

    HashSet<Path> filteredPaths = new HashSet<>();
    getAllFilteredPaths(expression, filteredPaths);//根据给定的expression表达式，判断其对应的时间序列表达式是否包含过滤器Filter，有过滤器的序列路径放入filteredPaths列表里。若该表达式是SingleSeriesExpression，则才有过滤器，若是二元表达式则分别获取其左右子表达式判断是否是SingleSeriesExpression，进而判断对应的子表达式是否有过滤器

    for (Path selectedPath : selectedPaths) { //根据filteredPaths列表判断每个selectedPaths是否有过滤器
      cached.add(filteredPaths.contains(selectedPath));
    }

    return cached;
  }

  private static void getAllFilteredPaths(IExpression expression, HashSet<Path> paths) {//根据给定的expression表达式，判断其对应的时间序列表达式是否包含过滤器Filter，将含有过滤器的序列加入paths参数里。若该表达式是SingleSeriesExpression，则才有过滤器，若是二元表达式则分别获取其左右子表达式判断是否是SingleSeriesExpression，进而判断对应的子表达式是否有过滤器
    if (expression instanceof BinaryExpression) {//如果是二元表达式
      getAllFilteredPaths(((BinaryExpression) expression).getLeft(), paths);
      getAllFilteredPaths(((BinaryExpression) expression).getRight(), paths);
    } else if (expression instanceof SingleSeriesExpression) {//如果是一元的SingleSeriesExpression表达式，则该表达式里的时间序列是有对应的过滤器Filter的
      paths.add(((SingleSeriesExpression) expression).getSeriesPath());
    }
  }
}
