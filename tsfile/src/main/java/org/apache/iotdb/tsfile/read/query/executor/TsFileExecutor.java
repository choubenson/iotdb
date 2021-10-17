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

import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.EmptyFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsFileExecutor implements QueryExecutor {  //TsFile执行器，用来执行该TsFile的查询操作

  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;

  public TsFileExecutor(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  @Override
  public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
    // bloom filter
    BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();//获取该文件的TsFileMetadata对象（即IndexOfTimeseriesIndex索引内容）里的布隆过滤器
    List<Path> filteredSeriesPath = new ArrayList<>();  //用于存放该TsFile包含了此次查询的哪些时间序列路径
    if (bloomFilter != null) {
      for (Path path : queryExpression.getSelectedSeries()) {//获取该次查询的时间序列路径列表
        if (bloomFilter.contains(path.getFullPath())) {//通过该TsFile里的布隆过滤器迅速判断：该TsFile是否包含该条时间序列。若包含，则
          filteredSeriesPath.add(path);   //将该时间序列路径加入filteredSeriesPath列表里
        }
      }
      queryExpression.setSelectSeries(filteredSeriesPath);  //重新设置此次查询表达式queryExpression的时间序列路径列表
    }

    metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());//针对给定时间序列路径列表，获取每个时间序列各自对应在该TsFile里的所有ChunkIndex放入chunkMetaDataCache缓存里。具体做法是：1. 首先将整理每个DeviceID对应有哪些MeasurementId  2.遍历每个设备ID和对应的传感器集合：（1）获得对应的TimeseriesIndex列表（2）对每个TimeseriesIndex获取其所有的ChunkIndex依次放入一个列表里（3）遍历所有的ChunkIndex列表，把属于该次遍历的传感器的ChunkIndex对象加入对应时间序列的缓存变量里
    if (queryExpression.hasQueryFilter()) { //若该次查询有查询的条件过滤器，则
      try {
        IExpression expression = queryExpression.getExpression(); //获取该次查询的表达式
        IExpression regularIExpression =  //优化后的表达式
            ExpressionOptimizer.getInstance()
                .optimize(expression, queryExpression.getSelectedSeries()); // 对表达式进行优化：若是一元表达式（GlobalTimeExpression和SingleSeriesExpression）则不优化，若是二元表达式（AndExpression等等），则进行相关合并等优化操作
        queryExpression.setExpression(regularIExpression);//对queryExpression重新设置优化后的表达式

        if (regularIExpression instanceof GlobalTimeExpression) { //若表达式是GlobalTimeExpression类型，则
          return execute(
              queryExpression.getSelectedSeries(), (GlobalTimeExpression) regularIExpression);
        } else {
          return new ExecutorWithTimeGenerator(metadataQuerier, chunkLoader)
              .execute(queryExpression);
        }
      } catch (QueryFilterOptimizationException | NoMeasurementException e) {
        throw new IOException(e);
      }
    } else {
      try {
        return execute(queryExpression.getSelectedSeries());
      } catch (NoMeasurementException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Query with the space partition constraint.
   *
   * @param queryExpression query expression
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos the end position of the space partition
   * @return QueryDataSet
   */
  public QueryDataSet execute(
      QueryExpression queryExpression, long spacePartitionStartPos, long spacePartitionEndPos)
      throws IOException {
    // convert the space partition constraint to the time partition constraint
    ArrayList<TimeRange> resTimeRanges =
        new ArrayList<>(
            metadataQuerier.convertSpace2TimePartition(
                queryExpression.getSelectedSeries(), spacePartitionStartPos, spacePartitionEndPos));

    // check if resTimeRanges is empty
    if (resTimeRanges.isEmpty()) {
      return new DataSetWithoutTimeGenerator(
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList()); // return an empty QueryDataSet
    }

    // construct an additional time filter based on the time partition constraint
    IExpression addTimeExpression = resTimeRanges.get(0).getExpression();
    for (int i = 1; i < resTimeRanges.size(); i++) {
      addTimeExpression =
          BinaryExpression.or(addTimeExpression, resTimeRanges.get(i).getExpression());
    }

    // combine the original query expression and the additional time filter
    if (queryExpression.hasQueryFilter()) {
      IExpression combinedExpression =
          BinaryExpression.and(queryExpression.getExpression(), addTimeExpression);
      queryExpression.setExpression(combinedExpression);
    } else {
      queryExpression.setExpression(addTimeExpression);
    }

    // Having converted the space partition constraint to an additional time filter, we can now
    // query as normal.
    return execute(queryExpression);
  }

  /**
   * no filter, can use multi-way merge.
   *
   * @param selectedPathList all selected paths
   * @return DataSet without TimeGenerator
   */
  private QueryDataSet execute(List<Path> selectedPathList)
      throws IOException, NoMeasurementException {
    return executeMayAttachTimeFiler(selectedPathList, null);
  }

  /**
   * has a GlobalTimeExpression, can use multi-way merge.
   *
   * @param selectedPathList all selected paths
   * @param timeFilter GlobalTimeExpression that takes effect to all selected paths
   * @return DataSet without TimeGenerator
   */
  private QueryDataSet execute(List<Path> selectedPathList, GlobalTimeExpression timeFilter)  //第一个参数是时间序列路径列表，第二个是GlobalTimeExpression表达式
      throws IOException, NoMeasurementException {
    return executeMayAttachTimeFiler(selectedPathList, timeFilter);
  }

  /**
   * @param selectedPathList completed path
   * @param timeExpression a GlobalTimeExpression or null
   * @return DataSetWithoutTimeGenerator
   */
  private QueryDataSet executeMayAttachTimeFiler(
      List<Path> selectedPathList, GlobalTimeExpression timeExpression)
      throws IOException, NoMeasurementException {
    List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>(); //存放了该TsFIle里每个指定的时间序列对应的数据类型，若不存在此时序则类型为Null

    for (Path path : selectedPathList) {//遍历给定的时间序列路径列表
      List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(path);//从chunkMetaDataCache缓存里获取该时间序列路径对应的所有ChunkIndex
      AbstractFileSeriesReader seriesReader;
      if (chunkMetadataList.isEmpty()) {  //若该时间序列对应的ChunkIndex列表为空，则说明该TsFile里不存在此时间序列
        seriesReader = new EmptyFileSeriesReader(); //创建一个空的文件序列读取器
        dataTypes.add(metadataQuerier.getDataType(path));//获取该时间序列对应的数据类型,若该TsFile不存在此时间序列则返回null，具体做法是获取其TimeseriesIndex对象里的所有ChunkIndex列表，然后拿第一个ChunkIndex获取其数据类型。此处应该为null
      } else {
        if (timeExpression == null) {
          seriesReader = new FileSeriesReader(chunkLoader, chunkMetadataList, null);
        } else {
          seriesReader =
              new FileSeriesReader(chunkLoader, chunkMetadataList, timeExpression.getFilter());
        }
        dataTypes.add(chunkMetadataList.get(0).getDataType());
      }
      readersOfSelectedSeries.add(seriesReader);
    }
    return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
  }
}
