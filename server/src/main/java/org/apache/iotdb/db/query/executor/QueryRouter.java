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

package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByTimeDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.EmptyDataSet;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Query entrance class of IoTDB query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by EngineQueryRouter.
 */
public class QueryRouter implements IQueryRouter {

  private static Logger logger = LoggerFactory.getLogger(QueryRouter.class);

  @Override
  public QueryDataSet rawDataQuery(RawDataQueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException { // 根据查询计划和查询环境进行原始数据查询
    IExpression expression =
        queryPlan
            .getExpression(); // 获取此次查询计划的表达式，即此查询的where子句后的内容，比如"select .. from .. where time>1000
    // or time<300"，则该查询的表达式就是"time>1000 ||
    // time<300"的，它是GlobalTimeExpression的，且过滤器是orFilter，而orFilter又是继承一个二元过滤器BinaryFilter，二元过滤器的左右操作过滤器又是个时间过滤器TimeFilter对象
    List<PartialPath> deduplicatedPaths =
        queryPlan
            .getDeduplicatedPaths(); // 获取此查询包含的所有时间序列路径，如"select * from root.*.*.*"可能就会包含多条时间序列路径

    IExpression optimizedExpression;
    try {
      optimizedExpression =
          expression == null
              ? null
              : ExpressionOptimizer.getInstance()
                  .optimize(
                      expression,
                      new ArrayList<>(
                          deduplicatedPaths)); // 对表达式进行优化：若是一元表达式（GlobalTimeExpression和SingleSeriesExpression）则不优化，若是二元表达式（AndExpression等等），则进行相关合并等优化操作
    } catch (QueryFilterOptimizationException e) {
      throw new StorageEngineException(e.getMessage());
    }
    queryPlan.setExpression(optimizedExpression); // 对查询计划设置优化后的expression表达式

    RawDataQueryExecutor rawDataQueryExecutor =
        getRawDataQueryExecutor(queryPlan); // 根据原数据查询计划创建并获取一个新的原数据查询执行器对象

    if (!queryPlan.isAlignByTime()) { // 如果该查询不是disable align的，则
      return rawDataQueryExecutor.executeNonAlign(context);
    }

    if (optimizedExpression
            != null // 如果优化后的expression不为空（只要该查询的expression不为空，优化后的Expression就不为空）且优化后表达式类型不为GlobalTime，则
        && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME) {
      return rawDataQueryExecutor.executeWithValueFilter(context);
    } else if (optimizedExpression != null
        && optimizedExpression.getType()
            == ExpressionType.GLOBAL_TIME) { // 如果优化后的expression不为空且优化后表达式类型为GlobalTime，则
      Filter timeFilter =
          ((GlobalTimeExpression) queryPlan.getExpression())
              .getFilter(); // 获取此次查询GlobalTime一元表达式的过滤器，可能是一元时间过滤器或者一个二元过滤器（AndFilter或者OrFilter），它包含了左右两个时间过滤器
      TimeValuePairUtils.Intervals intervals =
          TimeValuePairUtils.extractTimeInterval(
              timeFilter); // 根据该一元或者二元包含时间的过滤器，获取具体的时间范围。eg:若此查询的时间范围是（100，500），则此时间范围列表会存放两个元素，分别是上限101和下限499
      if (intervals.isEmpty()) { // 如果时间范围列表为空，则说明此查询where子句里没有时间范围限制。
        logger.warn("The interval of the filter {} is empty.", timeFilter);
        return new EmptyDataSet(); // 返回空数据集
      }
    }

    // Currently, we only group the vector partial paths for raw query without value filter
    queryPlan.transformToVector();
    return rawDataQueryExecutor.executeWithoutValueFilter(context); // 目前查询条件只能是时间相关的，不能有数值相关的。
  }

  protected RawDataQueryExecutor getRawDataQueryExecutor(
      RawDataQueryPlan queryPlan) { // 根据原数据查询计划创建并获取一个新的原数据查询执行器对象
    return new RawDataQueryExecutor(queryPlan);
  }

  @Override
  public QueryDataSet aggregate(AggregationPlan aggregationPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "paths:"
              + aggregationPlan.getPaths()
              + " level:"
              + aggregationPlan.getLevel()
              + " duplicatePaths:"
              + aggregationPlan.getDeduplicatedPaths()
              + " deduplicatePaths:"
              + aggregationPlan.getDeduplicatedAggregations());
    }

    IExpression expression = aggregationPlan.getExpression();
    List<PartialPath> deduplicatedPaths = aggregationPlan.getDeduplicatedPaths();

    // optimize expression to an executable one
    IExpression optimizedExpression =
        expression == null
            ? null
            : ExpressionOptimizer.getInstance()
                .optimize(expression, new ArrayList<>(deduplicatedPaths));

    aggregationPlan.setExpression(optimizedExpression);

    AggregationExecutor engineExecutor = getAggregationExecutor(context, aggregationPlan);

    QueryDataSet dataSet = null;

    if (optimizedExpression != null
        && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME) {
      dataSet = engineExecutor.executeWithValueFilter(aggregationPlan);
    } else {
      dataSet = engineExecutor.executeWithoutValueFilter(aggregationPlan);
    }

    return dataSet;
  }

  protected AggregationExecutor getAggregationExecutor(
      QueryContext context, AggregationPlan aggregationPlan) {
    return new AggregationExecutor(context, aggregationPlan);
  }

  @Override
  public QueryDataSet groupBy(GroupByTimePlan groupByTimePlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {

    if (logger.isDebugEnabled()) {
      logger.debug("paths:" + groupByTimePlan.getPaths() + " level:" + groupByTimePlan.getLevel());
    }

    GroupByEngineDataSet dataSet = null;
    IExpression expression = groupByTimePlan.getExpression();
    List<PartialPath> selectedSeries = groupByTimePlan.getDeduplicatedPaths();
    GlobalTimeExpression timeExpression = getTimeExpression(groupByTimePlan);

    if (expression == null) {
      expression = timeExpression;
    } else {
      expression = BinaryExpression.and(expression, timeExpression);
    }

    // optimize expression to an executable one
    IExpression optimizedExpression =
        ExpressionOptimizer.getInstance().optimize(expression, new ArrayList<>(selectedSeries));
    groupByTimePlan.setExpression(optimizedExpression);

    if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
      dataSet = getGroupByWithoutValueFilterDataSet(context, groupByTimePlan);
    } else {
      dataSet = getGroupByWithValueFilterDataSet(context, groupByTimePlan);
    }

    // we support group by level for count operation
    // details at https://issues.apache.org/jira/browse/IOTDB-622
    // and UserGuide/Operation Manual/DML
    if (groupByTimePlan.getLevel() >= 0) {
      return groupByLevelWithoutTimeIntervalDataSet(context, groupByTimePlan, dataSet);
    }
    return dataSet;
  }

  private GlobalTimeExpression getTimeExpression(GroupByTimePlan plan)
      throws QueryProcessException {
    if (plan.isSlidingStepByMonth() || plan.isIntervalByMonth()) {
      if (!plan.isAscending()) {
        throw new QueryProcessException("Group by month doesn't support order by time desc now.");
      }
      return new GlobalTimeExpression(
          (new GroupByMonthFilter(
              plan.getInterval(),
              plan.getSlidingStep(),
              plan.getStartTime(),
              plan.getEndTime(),
              plan.isSlidingStepByMonth(),
              plan.isIntervalByMonth(),
              SessionManager.getInstance().getCurrSessionTimeZone())));
    } else {
      return new GlobalTimeExpression(
          new GroupByFilter(
              plan.getInterval(), plan.getSlidingStep(), plan.getStartTime(), plan.getEndTime()));
    }
  }

  protected GroupByWithoutValueFilterDataSet getGroupByWithoutValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan)
      throws StorageEngineException, QueryProcessException {
    return new GroupByWithoutValueFilterDataSet(context, plan);
  }

  protected GroupByWithValueFilterDataSet getGroupByWithValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan)
      throws StorageEngineException, QueryProcessException {
    return new GroupByWithValueFilterDataSet(context, plan);
  }

  protected GroupByTimeDataSet groupByLevelWithoutTimeIntervalDataSet(
      QueryContext context, GroupByTimePlan plan, GroupByEngineDataSet dataSet)
      throws QueryProcessException, IOException {
    return new GroupByTimeDataSet(context, plan, dataSet);
  }

  @Override
  public QueryDataSet fill(FillQueryPlan fillQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    FillQueryExecutor fillQueryExecutor = getFillExecutor(fillQueryPlan);
    return fillQueryExecutor.execute(context);
  }

  protected FillQueryExecutor getFillExecutor(FillQueryPlan plan) {
    return new FillQueryExecutor(plan);
  }

  @Override
  public QueryDataSet groupByFill(GroupByTimeFillPlan groupByFillPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {
    GroupByEngineDataSet groupByEngineDataSet =
        (GroupByEngineDataSet) groupBy(groupByFillPlan, context);
    return new GroupByFillDataSet(
        groupByFillPlan.getDeduplicatedPaths(),
        groupByFillPlan.getDeduplicatedDataTypes(),
        groupByEngineDataSet,
        groupByFillPlan.getFillType(),
        context,
        groupByFillPlan);
  }

  @Override
  public QueryDataSet lastQuery(LastQueryPlan lastQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    LastQueryExecutor lastQueryExecutor = getLastQueryExecutor(lastQueryPlan);
    return lastQueryExecutor.execute(context, lastQueryPlan);
  }

  protected LastQueryExecutor getLastQueryExecutor(LastQueryPlan lastQueryPlan) {
    return new LastQueryExecutor(lastQueryPlan);
  }

  @Override
  public QueryDataSet udtfQuery(UDTFPlan udtfPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException, InterruptedException {
    IExpression expression = udtfPlan.getExpression();
    IExpression optimizedExpression;
    try {
      optimizedExpression =
          expression == null
              ? null
              : ExpressionOptimizer.getInstance()
                  .optimize(expression, new ArrayList<>(udtfPlan.getDeduplicatedPaths()));
    } catch (QueryFilterOptimizationException e) {
      throw new StorageEngineException(e.getMessage());
    }
    udtfPlan.setExpression(optimizedExpression);

    boolean withValueFilter =
        optimizedExpression != null && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME;
    UDTFQueryExecutor udtfQueryExecutor = new UDTFQueryExecutor(udtfPlan);

    if (udtfPlan.isAlignByTime()) {
      return withValueFilter
          ? udtfQueryExecutor.executeWithValueFilterAlignByTime(context)
          : udtfQueryExecutor.executeWithoutValueFilterAlignByTime(context);
    } else {
      return withValueFilter
          ? udtfQueryExecutor.executeWithValueFilterNonAlign(context)
          : udtfQueryExecutor.executeWithoutValueFilterNonAlign(context);
    }
  }
}
