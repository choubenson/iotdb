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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowQueryProcesslistPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to monitor the executing time of each query. Once one is over the threshold,
 * it will be killed and return the time out exception.
 */
public class QueryTimeManager implements IService { //查询操作的时间管理类，该类用于监控管理查询操作的时间，一旦某次查询时间操作了临界值，该查询线程任务就会被停止，然后抛出异常

  private static final Logger logger = LoggerFactory.getLogger(QueryTimeManager.class);
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * the key of queryInfoMap is the query id and the value of queryInfoMap is the start time, the
   * statement of this query.
   */
  private Map<Long, QueryInfo> queryInfoMap;  //存放了每个查询ID对应的查询信息类对象

  private ScheduledExecutorService executorService;

  private Map<Long, ScheduledFuture<?>> queryScheduledTaskMap;  //存放了每个查询ID对应的执行情况类对象

  private QueryTimeManager() {
    queryInfoMap = new ConcurrentHashMap<>();
    queryScheduledTaskMap = new ConcurrentHashMap<>();
    executorService = IoTDBThreadPoolFactory.newScheduledThreadPool(1, "query-time-manager");
  }

  public void registerQuery(long queryId, long startTime, String sql, long timeout) { //往该查询时间管理器注册一次查询操作，让该管理类监控并管理此次查询操作服务的执行时间，若查过临界值则会被停止。最后把该次查询ID和对应的结果放入queryScheduledTaskMap里
    final long finalTimeout = timeout == 0 ? config.getQueryTimeoutThreshold() : timeout; //获取此查询设定的超时时间
    queryInfoMap.put(queryId, new QueryInfo(startTime, sql));     //往map里放入此次查询ID对应的查询信息类对象
    // submit a scheduled task to judge whether query is still running after timeout
    ScheduledFuture<?> scheduledFuture =
        executorService.schedule(   //该executorService执行的任务是当此查询ID的查询服务时间太久了就会kill停止指定ID的查询，然后返回一个可以用于取消或检查运行状态的Future对象。
            () -> {
              killQuery(queryId); //关闭某查询，将其对应的查询信息类的中断属性设置为true
              logger.warn(
                  String.format("Query is time out (%dms) with queryId %d", finalTimeout, queryId));
            },
            finalTimeout,
            TimeUnit.MILLISECONDS);
    queryScheduledTaskMap.put(queryId, scheduledFuture);//往map里放入此查询ID的执行情况
  }

  public void registerQuery(  //注册查询:往该查询时间管理器注册一次查询操作，让该管理类监控并管理此次查询操作服务的执行时间，若查过临界值则会被停止。最后把该次查询ID和对应的结果放入queryScheduledTaskMap里
      long queryId, long startTime, String sql, long timeout, PhysicalPlan plan) {
    if (plan instanceof ShowQueryProcesslistPlan) {
      return;
    }
    registerQuery(queryId, startTime, sql, timeout); //往该查询时间管理器注册一次查询操作，让该管理类监控并管理此次查询操作服务的执行时间，若查过临界值则会被停止。最后把该次查询ID和对应的结果放入queryScheduledTaskMap里
  }

  public void killQuery(long queryId) { //关闭某查询，将其对应的查询信息类的中断属性设置为true
    if (queryInfoMap.get(queryId) == null) {
      return;
    }
    queryInfoMap.get(queryId).setInterrupted(true); //根据查询ID获取其对应的查询信息类对象，并设置中断属性为true。
  }

  public AtomicBoolean unRegisterQuery(long queryId) {
    // This is used to make sure the QueryTimeoutRuntimeException is thrown once
    AtomicBoolean successRemoved = new AtomicBoolean(false);
    queryInfoMap.computeIfPresent(
        queryId,
        (k, v) -> {
          successRemoved.set(true);
          ScheduledFuture<?> scheduledFuture = queryScheduledTaskMap.remove(queryId);
          if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
          }
          SessionTimeoutManager.getInstance()
              .refresh(SessionManager.getInstance().getSessionIdByQueryId(queryId));
          return null;
        });
    return successRemoved;
  }

  public AtomicBoolean unRegisterQuery(long queryId, PhysicalPlan plan) {
    return plan instanceof ShowQueryProcesslistPlan ? null : unRegisterQuery(queryId);
  }

  public static void checkQueryAlive(long queryId) {
    QueryInfo queryInfo = getInstance().queryInfoMap.get(queryId);
    if (queryInfo != null && queryInfo.isInterrupted()) {
      if (getInstance().unRegisterQuery(queryId).get()) {
        throw new QueryTimeoutRuntimeException(
            QueryTimeoutRuntimeException.TIMEOUT_EXCEPTION_MESSAGE);
      }
    }
  }

  public Map<Long, QueryInfo> getQueryInfoMap() {
    return queryInfoMap;
  }

  public static QueryTimeManager getInstance() {
    return QueryTimeManagerHelper.INSTANCE;
  }

  @Override
  public void start() {
    // Do Nothing
  }

  @Override
  public void stop() {
    if (executorService == null || executorService.isShutdown()) {
      return;
    }
    executorService.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.QUERY_TIME_MANAGER;
  }

  private static class QueryTimeManagerHelper {

    private static final QueryTimeManager INSTANCE = new QueryTimeManager();

    private QueryTimeManagerHelper() {}
  }

  public class QueryInfo {    //查询信息类，存储了该查询的开始时间和sql

    /**
     * To reduce the cost of memory, we only keep the a certain size statement. For statement whose
     * length is over this, we keep its head and tail.
     */
    private static final int MAX_STATEMENT_LENGTH = 64;

    private final long startTime; //开始时间
    private final String statement; //sql

    private volatile boolean isInterrupted = false; //当前查询是否被中断，默认否

    public QueryInfo(long startTime, String statement) {
      this.startTime = startTime;
      if (statement.length() <= 64) {
        this.statement = statement;
      } else {//若sql长度超过64字节，则用...替代中间字符串
        this.statement =
            statement.substring(0, MAX_STATEMENT_LENGTH / 2)
                + "..."
                + statement.substring(statement.length() - MAX_STATEMENT_LENGTH / 2);
      }
    }

    public long getStartTime() {
      return startTime;
    }

    public String getStatement() {
      return statement;
    }

    public void setInterrupted(boolean interrupted) {
      isInterrupted = interrupted;
    }

    public boolean isInterrupted() {
      return isInterrupted;
    }
  }
}
