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

package org.apache.iotdb.db.query.expression;

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code ResultColumn} is used to represent a result column of a query.
 *
 * <p>Assume that we have time series in db as follows: <br>
 * [ root.sg.d.a, root.sg.d.b, root.sg.e.a, root.sg.e.b ]
 *
 * <ul>
 *   Example 1: select a, a + b, udf(udf(b)) from root.sg.d, root.sg.e;
 *   <li>Step 1: constructed by sql visitor in logical operator: <br>
 *       result columns: <br>
 *       [a, a + b, udf(udf(b))]
 *   <li>Step 2: concatenated with prefix paths in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.a, root.sg.e.a, root.sg.d.a + root.sg.d.b, root.sg.d.a + root.sg.e.b,
 *       root.sg.e.a + root.sg.d.b, root.sg.e.a + root.sg.e.b, udf(udf(root.sg.d.b)),
 *       udf(udf(root.sg.e.b))]
 *   <li>Step 3: remove wildcards in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.a, root.sg.e.a, root.sg.d.a + root.sg.d.b, root.sg.d.a + root.sg.e.b,
 *       root.sg.e.a + root.sg.d.b, root.sg.e.a + root.sg.e.b, udf(udf(root.sg.d.b)),
 *       udf(udf(root.sg.e.b))]
 * </ul>
 *
 * <ul>
 *   Example 2: select *, a + *, udf(udf(*)) from root.sg.d;
 *   <li>Step 1: constructed by sql visitor in logical operator: <br>
 *       result columns: <br>
 *       [*, a + * , udf(udf(*))]
 *   <li>Step 2: concatenated with prefix paths in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.*, root.sg.d.a + root.sg.d.*, udf(udf(root.sg.d.*))]
 *   <li>Step 3: remove wildcards in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.a, root.sg.d.b, root.sg.d.a + root.sg.d.a, root.sg.d.a + root.sg.d.b,
 *       udf(udf(root.sg.d.a)), udf(udf(root.sg.d.b))]
 * </ul>
 */
public class ResultColumn { // 结果列类，该类用于表示某次查询结果的某列，包含了该列的表达式和别名

  private final Expression expression; // 表达式，可以是多元表达式or单元表达式，如查询语句是select a+b,-a,a%b from
  // root.sg.r1,则该查询结果会有三个结果列类，每个列类对应的表达式分别为"a+b","-a","a%b"
  private final String alias; // 对于查询结果的每列可以设定各自的别名

  private TSDataType dataType;

  public ResultColumn(Expression expression, String alias) {
    this.expression = expression;
    this.alias = alias;
  }

  public ResultColumn(Expression expression) {
    this.expression = expression;
    alias = null;
  }

  /**
   * @param prefixPaths prefix paths in the from clause 前缀路径，from子句里可能存在多个前缀路径
   * @param resultColumns used to collect the result columns //存放该查询结果的每列对应的列对象
   */
  public void concat(
      List<PartialPath> prefixPaths,
      List<ResultColumn>
          resultColumns) // 将当前结果列对象里的表达式与指定的prefixPaths列表里的所有前缀路径连接所生成的一个或多个结果列对象放入第二个参数resultColumns列表里
      throws LogicalOptimizeException {
    List<Expression> resultExpressions = new ArrayList<>();
    expression.concat(
        prefixPaths,
        resultExpressions); // 将当前列对象依次与所有的前缀路径合并连接，生成的连接后的表达式依次放入resultExpressions列表里。如前缀路径有"root.sg.*"和"root.t1.a"，而当前列类表达式为"a"，因此合并后为"root.sg.*.a"和"root.t1.a.a"
    if (hasAlias()
        && 1
            < resultExpressions
                .size()) { // 若当前结果列是有别名的，而from子句里的路径前缀不止一个，就会导致结果列不止一个，而别名只能针对一个结果列来使用，因此会报错
      throw new LogicalOptimizeException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
    for (Expression resultExpression : resultExpressions) {
      resultColumns.add(new ResultColumn(resultExpression, alias));
    }
  }

  /**
   * @param wildcardsRemover used to remove wildcards from {@code expression} and apply slimit &
   *     soffset control
   * @param resultColumns used to collect the result columns
   */
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<ResultColumn> resultColumns)
      throws
          LogicalOptimizeException { // 对当前列对象的表达式进行去除通配符，生成一个或多个列对象放入第二个参数resultColumns里。eg：有列对象表达式为"root.sg.a.*"，去除通配符有"root.sg.a.b"和"root.sg.a.c"
    List<Expression> resultExpressions = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressions);
    if (hasAlias() && 1 < resultExpressions.size()) {
      throw new LogicalOptimizeException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
    for (Expression resultExpression : resultExpressions) {
      resultColumns.add(new ResultColumn(resultExpression, alias));
    }
  }

  public Set<PartialPath> collectPaths() {
    Set<PartialPath> pathSet = new HashSet<>();
    expression.collectPaths(pathSet);
    return pathSet;
  }

  public Expression getExpression() {
    return expression;
  }

  public boolean hasAlias() {
    return alias != null;
  }

  public String getAlias() {
    return alias;
  }

  public String getResultColumnName() { // 获取当前结果列对象的名称，若有别名则返回别名，否则返回表达式字符串
    return alias != null ? alias : expression.getExpressionString();
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public TSDataType getDataType() {
    return dataType;
  }
}
