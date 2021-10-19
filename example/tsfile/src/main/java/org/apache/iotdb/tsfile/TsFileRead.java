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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The class is to show how to read TsFile file named "test.tsfile". The TsFile file "test.tsfile"
 * is generated from class TsFileWriteWithTSRecord or TsFileWriteWithTablet. Run
 * TsFileWriteWithTSRecord or TsFileWriteWithTablet to generate the test.tsfile first
 */
public class TsFileRead {

  private static final String DEVICE1 = "device_1";

  private static void queryAndPrint(
      ArrayList<Path> paths, ReadOnlyTsFile readTsFile, IExpression statement) throws IOException {
    QueryExpression queryExpression =
        QueryExpression.create(paths, statement); // 使用时间序列路径列表和表达式创建查询表达式类
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);//根据给定的查询表达式，（1）首先把此次查询中属于该TsFile的时间序列路径加入列表里（一个查询可能涉及到多个不同TsFile的多个时间序列）（2）获取该次查询在该TsFile的每个时间序列对应的所有ChunkIndex放入该TsFile的元数据查询器里的chunkMetaDataCache缓存里（3）通过判断该次查询是否有过滤器，有的话则创建DataSetWithTimeGenerator查询结果集对象并返回，没有则创建DataSetWithoutTimeGenerator查询结果集对象返回
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
  }

  public static void main(String[] args) throws IOException {

    // file path
    String path = "C:\\IOTDB\\sourceCode\\choubenson\\iotdb\\test.tsfile";

    // create reader and get the readTsFile interface
    try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {

      // use these paths(all measurements) for all the queries
      ArrayList<Path> paths = new ArrayList<>(); // 存放了一堆时间序列路径
//      paths.add(new Path(DEVICE1, "sensor_1"));
//      paths.add(new Path(DEVICE1, "sensor_2"));
      paths.add(new Path(DEVICE1, "sensor_77"));

      // no filter, should select 1 2 3 4 6 7 8
      queryAndPrint(paths, readTsFile, null);

      // time filter : 4 <= time <= 10, should select 4 6 7 8
      IExpression timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilter.gtEq(4L)),
              new GlobalTimeExpression(TimeFilter.ltEq(10L)));
      queryAndPrint(paths, readTsFile, timeFilter);

      // value filter : device_1.sensor_2 <= 20, should select 1 2 4 6 7 //Todo:bug?test!!
      IExpression valueFilter =
          new SingleSeriesExpression(new Path(DEVICE1, "sensor_2"), ValueFilter.ltEq(20L));
      queryAndPrint(paths, readTsFile, valueFilter);

      // time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20, should select 4 7 8
      timeFilter =
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilter.gtEq(4L)),
              new GlobalTimeExpression(TimeFilter.ltEq(10L)));
      valueFilter =
          new SingleSeriesExpression(new Path(DEVICE1, "sensor_3"), ValueFilter.gtEq(20L));
      IExpression finalFilter = BinaryExpression.and(timeFilter, valueFilter);
      queryAndPrint(paths, readTsFile, finalFilter);
    }
  }
}
