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
package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class BooleanTVList extends TVList {

  // list of primitive array, add 1 when expanded -> boolean primitive array
  // index relation: arrayIndex -> elementIndex
  private List<boolean[]> values; // 存放了一个个boolean类型数组的列表

  private boolean[][] sortedValues;

  private boolean pivotValue;

  BooleanTVList() {
    super();
    values = new ArrayList<>();
  }

  @Override
  public void putBoolean(
      long timestamp,
      boolean
          value) { // 首先判断是否要对此TVList的values和timestamps列表，然后往该BooleanTVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值
    checkExpansion(); // 检查是否需要扩容，即判断该TVList的values和timestamps列表是否需要扩容，每次扩容都是分别新增一个ARRAY_SIZE数量的数组，放入values列表和timestamps列表中
    int arrayIndex =
        size / ARRAY_SIZE; // 计算当前values和timestamps列表里的数组索引，即要往values和timestamps列表中哪个数组插入数据
    int elementIndex = size % ARRAY_SIZE; // 元素索引，即插入具体数组的哪个位置
    minTime = Math.min(minTime, timestamp); // 记录此TVList的最小时间戳
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    size++; // 数据点数量+1
    if (sorted
        && size > 1
        && timestamp < getTime(size - 2)) { // 若原先该TVList是顺序的，且此次插入的时间戳小于上一个数据点的时间戳，则此TVList为乱序的
      sorted = false;
    }
  }

  @Override
  public boolean getBoolean(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return values.get(arrayIndex)[elementIndex];
  }

  protected void set(int index, long timestamp, boolean value) { // 将此TVList第index个位置换成新的值
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE; // 计算数组索引，即是第几个数组
    int elementIndex = index % ARRAY_SIZE; // 计算元素索引，即是数组里第几个元素
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
  }

  @Override
  public BooleanTVList clone() {
    BooleanTVList cloneList = new BooleanTVList();
    cloneAs(cloneList);
    for (boolean[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  private boolean[] cloneValue(boolean[] array) {
    boolean[] cloneArray = new boolean[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  @Override
  public void sort() {
    if (sortedTimestamps == null || sortedTimestamps.length < size) {
      sortedTimestamps =
          (long[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, size);
    }
    if (sortedValues == null || sortedValues.length < size) {
      sortedValues =
          (boolean[][]) PrimitiveArrayManager.createDataListsByType(TSDataType.BOOLEAN, size);
    }
    sort(0, size);
    clearSortedValue();
    clearSortedTime();
    sorted = true;
  }

  @Override
  void clearValue() {
    if (values != null) {
      for (boolean[] dataArray : values) {
        PrimitiveArrayManager.release(dataArray);
      }
      values.clear();
    }
  }

  @Override
  void clearSortedValue() {
    if (sortedValues != null) {
      sortedValues = null;
    }
  }

  @Override
  protected void setFromSorted(int src, int dest) {
    set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedValues[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  @Override
  protected void set(
      int src, int dest) { // 该方法用来重置此TVList，把原先TVList中第src个元素的时间戳和数值放到此TVList第dest个位置
    long srcT = getTime(src); // 根据给定的src索引获取对应的时间戳
    boolean srcV = getBoolean(src); // 根据给定的src索引获取对应的数值
    set(dest, srcT, srcV);
  }

  @Override
  protected void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getTime(src);
    sortedValues[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = getBoolean(src);
  }

  @Override
  protected void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = getTime(lo);
      boolean loV = getBoolean(lo);
      long hiT = getTime(hi);
      boolean hiV = getBoolean(hi);
      set(lo++, hiT, hiV);
      set(hi--, loT, loV);
    }
  }

  @Override
  protected void expandValues() { // 对此TVList进行扩容，每次扩容都是新增一个ARRAY_SIZE数量的数组，放入values列表中
    values.add(
        (boolean[]) getPrimitiveArraysByType(TSDataType.BOOLEAN)); // 往values列表里增加下一个新的boolean数值数组
  }

  @Override
  protected void saveAsPivot(int pos) {
    pivotTime = getTime(pos);
    pivotValue = getBoolean(pos);
  }

  @Override
  protected void setPivotTo(int pos) {
    set(pos, pivotTime, pivotValue);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(
        getTime(index), TsPrimitiveType.getByType(TSDataType.BOOLEAN, getBoolean(index)));
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(
        time, TsPrimitiveType.getByType(TSDataType.BOOLEAN, getBoolean(index)));
  }

  @Override
  protected void releaseLastValueArray() { // 释放该TVList的最后一个数值数组
    PrimitiveArrayManager.release(
        values.remove(
            values.size() - 1)); // 首先从该TVList的数值数组列表中移除最后一个数值数组，并把它还给系统，系统会把其整理清空后继续添加进系统的可用数组列表中
  }

  @Override
  public void putBooleans(long[] time, boolean[] value, BitMap bitMap, int start, int end) {
    checkExpansion();

    int idx = start;
    // constraint: time.length + timeIdxOffset == value.length
    int timeIdxOffset = 0;
    if (bitMap != null && !bitMap.isAllUnmarked()) {
      // time array is a reference, should clone necessary time values
      long[] clonedTime = new long[end - start];
      System.arraycopy(time, start, clonedTime, 0, end - start);
      time = clonedTime;
      timeIdxOffset = start;
      // drop null at the end of value array
      int nullCnt =
          dropNullValThenUpdateMinTimeAndSorted(time, value, bitMap, start, end, timeIdxOffset);
      end -= nullCnt;
    } else {
      updateMinTimeAndSorted(time, start, end);
    }

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = size / ARRAY_SIZE;
      int elementIdx = size % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, inputRemaining);
        size += inputRemaining;
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, internalRemaining);
        idx += internalRemaining;
        size += internalRemaining;
        checkExpansion();
      }
    }
  }

  // move null values to the end of time array and value array, then return number of null values
  int dropNullValThenUpdateMinTimeAndSorted(
      long[] time, boolean[] values, BitMap bitMap, int start, int end, int tIdxOffset) {
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;

    int nullCnt = 0;
    for (int vIdx = start; vIdx < end; vIdx++) {
      if (bitMap.isMarked(vIdx)) {
        nullCnt++;
        continue;
      }
      // move value ahead to replace null
      int tIdx = vIdx - tIdxOffset;
      if (nullCnt != 0) {
        time[tIdx - nullCnt] = time[tIdx];
        values[vIdx - nullCnt] = values[vIdx];
      }
      // update minTime and sorted
      tIdx = tIdx - nullCnt;
      inPutMinTime = Math.min(inPutMinTime, time[tIdx]);
      if (inputSorted && tIdx > 0 && time[tIdx - 1] > time[tIdx]) {
        inputSorted = false;
      }
    }
    minTime = Math.min(inPutMinTime, minTime);
    sorted = sorted && inputSorted && (size == 0 || inPutMinTime >= getTime(size - 1));
    return nullCnt;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }
}
