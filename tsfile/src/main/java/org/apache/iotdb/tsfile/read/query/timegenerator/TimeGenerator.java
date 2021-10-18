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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * All SingleSeriesExpression involved in a IExpression will be transferred to a TimeGenerator tree
 * whose leaf nodes are all SeriesReaders, The TimeGenerator tree can generate the next timestamp
 * that satisfies the filter condition. Then we use this timestamp to get values in other series
 * that are not included in IExpression
 */
public abstract class TimeGenerator {

  private HashMap<Path, List<LeafNode>> leafNodeCache = new HashMap<>();  //叶子节点缓存，存放了每个时间序列对应的叶子节点（叶子节点里存放了该时间序列的阅读器）
  private HashMap<Path, List<Object>> leafValuesCache;
  protected Node operatorNode;  //
  private boolean hasOrNode;  //是否有OR这个节点

  public boolean hasNext() throws IOException {
    return operatorNode.hasNext();
  }

  public long next() throws IOException {
    if (!hasOrNode) {
      if (leafValuesCache == null) {
        leafValuesCache = new HashMap<>();
      }
      leafNodeCache.forEach(
          (path, nodes) ->
              leafValuesCache
                  .computeIfAbsent(path, k -> new ArrayList<>())
                  .add(nodes.get(0).currentValue()));
    }
    return operatorNode.next();
  }

  /** ATTENTION: this method should only be used when there is no `OR` node */
  public Object[] getValues(Path path) throws IOException {
    if (hasOrNode) {
      throw new IOException(
          "getValues() method should not be invoked when there is OR operator in where clause");
    }
    if (leafValuesCache.get(path) == null) {
      throw new IOException(
          "getValues() method should not be invoked by non-existent path in where clause");
    }
    return leafValuesCache.remove(path).toArray();
  }

  /** ATTENTION: this method should only be used when there is no `OR` node */
  public Object getValue(Path path) throws IOException {
    if (hasOrNode) {
      throw new IOException(
          "getValue() method should not be invoked when there is OR operator in where clause");
    }
    if (leafValuesCache.get(path) == null) {
      throw new IOException(
          "getValue() method should not be invoked by non-existent path in where clause");
    }
    return leafValuesCache.get(path).remove(0);
  }

  public void constructNode(IExpression expression) throws IOException {//根据查询的表达式IExpression，设置该类中的operatorNode操作节点对象。若IExpression是：（1）SingleSeriesExpression则获取对应时间序列的文件序列阅读区并封装入叶子节点后并装入leafNodeCache缓存（2）否则是二元表达式，则依次对左、右子表达式递归次方法直至表达式为SingleSeriesExpression，则获取对应时间序列表达式的叶子节点（包含文件序列阅读器）。
    operatorNode = construct(expression);//可能是（1）包含该时间序列的文件序列阅读器的叶子节点（2）OR或者And节点，其中其左右子节点又分别是包含该时间序列的文件序列阅读器的叶子节点
  }

  /** construct the tree that generate timestamp. */
  protected Node construct(IExpression expression) throws IOException {//根据查询的表达式IExpression，若是：（1）SingleSeriesExpression则获取对应时间序列的文件序列阅读区并封装入叶子节点后并装入leafNodeCache缓存，返回该叶子节点（2）否则是二元表达式，则依次对左、右子表达式递归次方法直至表达式为SingleSeriesExpression，则获取对应时间序列表达式的叶子节点（包含文件序列阅读器）

    if (expression.getType() == ExpressionType.SERIES) {//如果此次查询的表达式类型是SingleSeriesExpression
      SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;//进行表达式的类型转换
      IBatchReader seriesReader = generateNewBatchReader(singleSeriesExp);//根据此次查询的单序列表达式获取对应时间序列的ChunkIndex列表，进而创建该TsFile的该时间序列的专属文件序列阅读器
      Path path = singleSeriesExp.getSeriesPath();//获取此次查询的该单时间序列表达式的时间序列路径

      // put the current reader to valueCache
      LeafNode leafNode = new LeafNode(seriesReader);//使用该时间序列的读取器创建叶子节点
      leafNodeCache.computeIfAbsent(path, p -> new ArrayList<>()).add(leafNode);//将该时间序列及其对应的包含阅读器的叶子节点放入leafNodeCache变量缓存里

      return leafNode;  //返回叶子节点
    } else {  //否则表达式是二元表达式
      Node leftChild = construct(((IBinaryExpression) expression).getLeft()); //获取左孩子表达式，并递归
      Node rightChild = construct(((IBinaryExpression) expression).getRight());//获取右孩子表达式，并递归

      if (expression.getType() == ExpressionType.OR) {  //若是OR表达式，则
        hasOrNode = true;
        return new OrNode(leftChild, rightChild, isAscending());  //返回OR节点
      } else if (expression.getType() == ExpressionType.AND) {  //若是And表达式，则
        return new AndNode(leftChild, rightChild, isAscending()); //返回And节点
      }
      throw new UnSupportedDataTypeException(
          "Unsupported ExpressionType when construct OperatorNode: " + expression.getType());
    }
  }

  protected abstract IBatchReader generateNewBatchReader(SingleSeriesExpression expression)//根据此次查询的单序列表达式获取对应时间序列的ChunkIndex列表，进而创建该TsFile的该时间序列的专属文件序列阅读器
      throws IOException;

  public boolean hasOrNode() {//是否存在or节点
    return hasOrNode;
  }

  protected abstract boolean isAscending();
}
