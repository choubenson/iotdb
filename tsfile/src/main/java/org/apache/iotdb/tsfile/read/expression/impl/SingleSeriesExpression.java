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
package org.apache.iotdb.tsfile.read.expression.impl;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.Serializable;

//一次查询的表达式是GlobalTimeExpression（存放了此次查询的时间相关的过滤器），而该次查询里可能涉及到多条时间序列，就需要依次用每个时间序列路径和GlobalTimeExpression里的时间过滤器去初始化SingleSeriesExpression表达式，因此可以把该表达式理解成某次查询里的某一条时间序列对应的单时间序列表达式
public class SingleSeriesExpression implements IUnaryExpression, Serializable { //SingleSeriesExpression表达式是一个单元过滤器，它包含了序列路径对象和过滤器对象，过滤器可能是二元过滤器或者一元过滤器，但是最终都是与时间过滤器相关的

  private static final long serialVersionUID = 7131207370394865228L;
  private Path seriesPath;  //此次查询的某一时间序列路径
  private Filter filter;  //过滤器可能是二元过滤器或者一元过滤器，最终都是与时间或数值过滤器相关的

  public SingleSeriesExpression(Path seriesDescriptor, Filter filter) {
    this.seriesPath = seriesDescriptor;
    this.filter = filter;
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.SERIES;
  }

  @Override
  public IExpression clone() {
    return new SingleSeriesExpression(seriesPath.clone(), filter.copy());
  }

  @Override
  public Filter getFilter() {
    return filter;
  }

  @Override
  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @Override
  public String toString() {
    return "[" + seriesPath + ":" + filter + "]";
  }

  public Path getSeriesPath() {
    return this.seriesPath;
  }

  public void setSeriesPath(Path seriesPath) {
    this.seriesPath = seriesPath;
  }
}
