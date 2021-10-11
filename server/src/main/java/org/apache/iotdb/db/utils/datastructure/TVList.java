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
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

// 每个传感器Chunk都有一个自己数据类型的TVList，来存放数据点。
public abstract class TVList { // TVList类是Time-Value
  // List，相当于一个List，存放着timestamps和value，它们分别是两个存放了对应数据数组的列表，当然还存了其他相关属性。

  protected static final int SMALL_ARRAY_LENGTH = 32;
  protected static final String ERR_DATATYPE_NOT_CONSISTENT = "DataType not consistent";
  protected List<long[]> timestamps; // 存放了一个个long型数组的列表
  protected int size; // 此TVList里数据点的数量

  protected long[][] sortedTimestamps;
  protected boolean sorted = true;
  // record reference count of this tv list
  // currently this reference will only be increase because we can't know when to decrease it
  protected AtomicInteger
      referenceCount; // 用于记录某Chunk对应的WritableMemChunk的此TVList被调用了几次，当对此TsFile的此Chunk进行查询操作的时候就会调用到此TVList，就会把它+1
  protected long pivotTime;
  protected long minTime; // 存放当前TVList里所有数据点中最小的时间戳

  private long version;

  public TVList() {
    timestamps = new ArrayList<>();
    size = 0;
    minTime = Long.MAX_VALUE;
    referenceCount = new AtomicInteger();
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
        return new BinaryTVList();
      case FLOAT:
        return new FloatTVList();
      case INT32:
        return new IntTVList();
      case INT64:
        return new LongTVList();
      case DOUBLE:
        return new DoubleTVList();
      case BOOLEAN:
        return new BooleanTVList();
      default:
        break;
    }
    return null;
  }

  public static TVList newVectorList(List<TSDataType> datatypes) {
    return new VectorTVList(datatypes);
  }

  public static long tvListArrayMemSize(TSDataType type) { // 计算此TVList的数组在内存中占用的大小，包括时间字段+值字段的占用大小和
    long size = 0;
    // time size
    size +=
        (long) PrimitiveArrayManager.ARRAY_SIZE * 8L; // 该TVList的时间数组占用的大小是"数组元素个数*Long类型大小8byte"
    // value size
    size +=
        (long) PrimitiveArrayManager.ARRAY_SIZE
            * (long) type.getDataTypeSize(); // 该TVList的数值数组占用的大小是"数组元素个数*数值类型TsDataType大小"
    return size;
  }

  /**
   * For Vector data type.
   *
   * @param types the types in the vector
   * @return VectorTvListArrayMemSize
   */
  public static long vectorTvListArrayMemSize(List<TSDataType> types) {
    long size = 0;
    // time size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // index size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * 4L;
    // value size
    for (TSDataType type : types) {
      size += (long) PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    }
    return size;
  }

  public boolean isSorted() {
    return sorted;
  }

  public void increaseReferenceCount() {
    referenceCount.incrementAndGet();
  }

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public int size() {
    return size;
  }

  public long getTime(int index) { // 根据数据索引获取该TVList里对应数据的时间戳
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return timestamps.get(arrayIndex)[elementIndex];
  }

  public void putLong(long time, long value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInt(long time, int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloat(long time, float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDouble(long time, double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinary(long time, Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBoolean(long time, boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putVector(long time, Object[] value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putLongs(long[] time, long[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInts(long[] time, int[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloats(long[] time, float[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDoubles(long[] time, double[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinaries(long[] time, Binary[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBooleans(long[] time, boolean[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putVectors(long[] time, Object[] value, BitMap[] bitMaps, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public int getInt(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public float getFloat(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Binary getBinary(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Object getVector(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public TVList getTvListByColumnIndex(List<Integer> columnIndexList) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public int getValueIndex(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public abstract void sort();

  public long getMinTime() {
    return minTime;
  }

  public long getVersion() {
    return version;
  }

  protected abstract void set(
      int src, int dest); // 该方法用来重置此TVList，把原先TVList中第src个元素的时间戳和数值放到此TVList第dest个位置

  protected abstract void setFromSorted(int src, int dest);

  protected abstract void setToSorted(int src, int dest);

  protected abstract void reverseRange(int lo, int hi);

  protected abstract void expandValues();

  @Override
  public abstract TVList clone();

  public TVList clone(long version) {
    this.version = version;
    return clone();
  }

  protected abstract void releaseLastValueArray();

  protected void releaseLastTimeArray() { // 释放该TVList的最后一个时间数组
    PrimitiveArrayManager.release(
        timestamps.remove(
            timestamps.size()
                - 1)); // 首先从该TVList的时间数组列表中移除最后一个时间数组，并把它还给系统，系统会把其整理清空后继续添加进系统的可用数组列表中
  }

  public int delete(
      long lowerBound, long upperBound) { // 根据给出的时间范围，删除该TVList里对应时间范围里的数据，返回被删除的数据点个数
    // 下面这种方法是：比如原先时间和数值分别有7个数组，删除后剩5个数组，若每个数组可装10个数据点，删除后剩43个数据点，则第5个数组还存在之前未被删掉的7个脏数据点他们是不会被删掉的，而第6、7个数组则会被移除清空返回给系统。虽然第5个数组里有脏数据，但这美关系，后面若还要存进来则直接覆盖就可以了。
    int newSize = 0; // 记录该TVList共有几个数据点不在待删除的时间范围内，可以理解为此次删除操作后此TVList新的数据点个数
    minTime = Long.MAX_VALUE; // 存放当前TVList所有数据点中最小的时间戳
    for (int i = 0; i < size; i++) { // 循环遍历当前TVList的元素
      long time = getTime(i); // 获取当前元素时间戳
      if (time < lowerBound || time > upperBound) { // 若当前数据的时间戳不在待删除的时间范围内
        set(
            i,
            newSize++); // 该方法用来重置此TVList，把原先TVList中第i个元素的时间戳和数值放到此TVList第newSize个位置//其实就是把TVList中那些未被删除的数据点依次进行往前挪动，按照原来的顺序放到TVList对应时间数组列表和值数组列表的前面
        minTime = Math.min(time, minTime); // 记录当前TVList最小时间戳
      }
    }
    int deletedNumber = size - newSize; // 被删除数据的元素个数=原先TVList数据点个数-未被删除数据点个数
    size = newSize; // 更新此TVList数据点的个数
    // release primitive arrays that are empty  释放删除操作后TVList中那些空的数组
    // 计算新的数组个数
    int newArrayNum = newSize / ARRAY_SIZE; // 计算新的数组个数
    if (newSize % ARRAY_SIZE != 0) { // 若数据点没有正好放满数组，则数组个数要+1
      newArrayNum++;
    }
    for (int releaseIdx = newArrayNum;
        releaseIdx < timestamps.size();
        releaseIdx++) { // 从废弃数组开始，依次遍历TVList中剩余的数组进行释放空间。（如原先有7个数组，删除后剩5个，则最后两个是废弃数组，索引从5开始）//此处采用从最后一个数组开始清空
      releaseLastTimeArray(); // 释放该TVList的最后一个时间数组
      releaseLastValueArray(); // 释放该TVList的最后一个数值数组
    }
    if (getDataType() == TSDataType.VECTOR) {
      return deletedNumber * ((VectorTVList) this).getTsDataTypes().size();
    }
    return deletedNumber;
  }

  // TODO: THIS METHOLD IS FOR DELETING ONE COLUMN OF A VECTOR
  public int delete(long lowerBound, long upperBound, int columnIndex) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  protected void cloneAs(TVList cloneList) {
    for (long[] timestampArray : timestamps) {
      cloneList.timestamps.add(cloneTime(timestampArray));
    }
    cloneList.size = size;
    cloneList.sorted = sorted;
    cloneList.minTime = minTime;
  }

  public void clear() {
    size = 0;
    sorted = true;
    minTime = Long.MAX_VALUE;
    clearTime();
    clearSortedTime();

    clearValue();
    clearSortedValue();
  }

  protected void clearTime() {
    if (timestamps != null) {
      for (long[] dataArray : timestamps) {
        PrimitiveArrayManager.release(dataArray);
      }
      timestamps.clear();
    }
  }

  protected void clearSortedTime() {
    if (sortedTimestamps != null) {
      sortedTimestamps = null;
    }
  }

  abstract void clearValue();

  /**
   * The arrays for sorting are not including in write memory now, the memory usage is considered as
   * temporary memory.
   */
  abstract void clearSortedValue();

  protected void
      checkExpansion() { // 判断该TVList的values和timestamps列表是否需要扩容，每次扩容都是分别新增一个ARRAY_SIZE数量的数组，放入values列表和timestamps列表中
    if ((size % ARRAY_SIZE) == 0) { // 如果此TVList数据点的数量到达了系统设置的数组大小的整数倍
      expandValues(); // 进行values列表扩容，每次扩容都是新增一个ARRAY_SIZE数量的数组，放入values列表中
      timestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64)); // 对timestamps列表也要进行扩容
    }
  }

  protected Object getPrimitiveArraysByType(TSDataType dataType) { // 根据数据类型获取数组(就是普通的数据类型数组)
    return PrimitiveArrayManager.allocate(dataType);
  }

  protected long[] cloneTime(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  protected void sort(int lo, int hi) {
    if (sorted) {
      return;
    }

    if (lo == hi) {
      return;
    }
    if (hi - lo <= SMALL_ARRAY_LENGTH) {
      int initRunLen = countRunAndMakeAscending(lo, hi);
      binarySort(lo, hi, lo + initRunLen);
      return;
    }
    int mid = (lo + hi) >>> 1;
    sort(lo, mid);
    sort(mid, hi);
    merge(lo, mid, hi);
  }

  protected int countRunAndMakeAscending(int lo, int hi) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi) {
      return 1;
    }

    // Find end of run, and reverse range if descending
    if (getTime(runHi++) < getTime(lo)) { // Descending
      while (runHi < hi && getTime(runHi) < getTime(runHi - 1)) {
        runHi++;
      }
      reverseRange(lo, runHi);
    } else { // Ascending
      while (runHi < hi && getTime(runHi) >= getTime(runHi - 1)) {
        runHi++;
      }
    }

    return runHi - lo;
  }

  protected int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  protected abstract void saveAsPivot(int pos);

  protected abstract void setPivotTo(int pos);

  /** From TimSort.java */
  protected void binarySort(int lo, int hi, int start) {
    assert lo <= start && start <= hi;
    if (start == lo) {
      start++;
    }
    for (; start < hi; start++) {

      saveAsPivot(start);
      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (compare(start, mid) < 0) {
          right = mid;
        } else {
          left = mid + 1;
        }
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left; // The number of elements to move
      for (int i = n; i >= 1; i--) {
        set(left + i - 1, left + i);
      }
      setPivotTo(left);
    }
    for (int i = lo; i < hi; i++) {
      setToSorted(i, i);
    }
  }

  protected void merge(int lo, int mid, int hi) {
    // end of sorting buffer
    int tmpIdx = 0;

    // start of unmerged parts of each sequence
    int leftIdx = lo;
    int rightIdx = mid;

    // copy the minimum elements to sorting buffer until one sequence is exhausted
    int endSide = 0;
    while (endSide == 0) {
      if (compare(leftIdx, rightIdx) <= 0) {
        setToSorted(leftIdx, lo + tmpIdx);
        tmpIdx++;
        leftIdx++;
        if (leftIdx == mid) {
          endSide = 1;
        }
      } else {
        setToSorted(rightIdx, lo + tmpIdx);
        tmpIdx++;
        rightIdx++;
        if (rightIdx == hi) {
          endSide = 2;
        }
      }
    }

    // copy the remaining elements of another sequence
    int start;
    int end;
    if (endSide == 1) {
      start = rightIdx;
      end = hi;
    } else {
      start = leftIdx;
      end = mid;
    }
    for (; start < end; start++) {
      setToSorted(start, lo + tmpIdx);
      tmpIdx++;
    }

    // copy from sorting buffer to the original arrays so that they can be further sorted
    // potential speed up: change the place of sorting buffer and origin data between merge
    // iterations
    for (int i = lo; i < hi; i++) {
      setFromSorted(i, i);
    }
  }

  void updateMinTimeAndSorted(long[] time) {
    updateMinTimeAndSorted(time, 0, time.length);
  }

  void updateMinTimeAndSorted(long[] time, int start, int end) {
    int length = time.length;
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;
    for (int i = start; i < end; i++) {
      inPutMinTime = Math.min(inPutMinTime, time[i]);
      if (inputSorted && i < length - 1 && time[i] > time[i + 1]) {
        inputSorted = false;
      }
    }
    minTime = Math.min(inPutMinTime, minTime);
    sorted = sorted && inputSorted && (size == 0 || inPutMinTime >= getTime(size - 1));
  }

  /** for log */
  public abstract TimeValuePair getTimeValuePair(int index);

  protected abstract TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding);

  public TimeValuePair getTimeValuePairForTimeDuplicatedRows(
      List<Integer> timeDuplicatedVectorRowIndexList,
      long time,
      Integer floatPrecision,
      TSEncoding encoding) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  @TestOnly
  public IPointReader getIterator() {
    return new Ite();
  }

  public IPointReader getIterator(
      int floatPrecision, TSEncoding encoding, int size, List<TimeRange> deletionList) {
    return new Ite(floatPrecision, encoding, size, deletionList);
  }

  private class Ite implements IPointReader {

    private TimeValuePair cachedTimeValuePair;
    private boolean hasCachedPair;
    private int cur;
    private Integer floatPrecision;
    private TSEncoding encoding;
    private int deleteCursor = 0;
    /**
     * because TV list may be share with different query, each iterator has to record it's own size
     */
    private int iteSize = 0;
    /** this field is effective only in the Tvlist in a RealOnlyMemChunk. */
    private List<TimeRange> deletionList;

    public Ite() {
      this.iteSize = TVList.this.size;
    }

    public Ite(int floatPrecision, TSEncoding encoding, int size, List<TimeRange> deletionList) {
      this.floatPrecision = floatPrecision;
      this.encoding = encoding;
      this.iteSize = size;
      this.deletionList = deletionList;
    }

    @Override
    public boolean hasNextTimeValuePair() {
      if (hasCachedPair) {
        return true;
      }

      List<Integer> timeDuplicatedVectorRowIndexList = null;
      while (cur < iteSize) {
        long time = getTime(cur);
        if (isPointDeleted(time) || (cur + 1 < size() && (time == getTime(cur + 1)))) {
          // record the time duplicated row index list for vector type
          if (getDataType() == TSDataType.VECTOR) {
            if (timeDuplicatedVectorRowIndexList == null) {
              timeDuplicatedVectorRowIndexList = new ArrayList<>();
              timeDuplicatedVectorRowIndexList.add(getValueIndex(cur));
            }
            timeDuplicatedVectorRowIndexList.add(getValueIndex(cur + 1));
          }
          cur++;
          continue;
        }
        TimeValuePair tvPair;
        if (getDataType() == TSDataType.VECTOR && timeDuplicatedVectorRowIndexList != null) {
          tvPair =
              getTimeValuePairForTimeDuplicatedRows(
                  timeDuplicatedVectorRowIndexList, time, floatPrecision, encoding);
          timeDuplicatedVectorRowIndexList = null;
        } else {
          tvPair = getTimeValuePair(cur, time, floatPrecision, encoding);
        }
        cur++;
        if (tvPair.getValue() != null) {
          cachedTimeValuePair = tvPair;
          hasCachedPair = true;
          return true;
        }
      }

      return false;
    }

    private boolean isPointDeleted(long timestamp) {
      while (deletionList != null && deleteCursor < deletionList.size()) {
        if (deletionList.get(deleteCursor).contains(timestamp)) {
          return true;
        } else if (deletionList.get(deleteCursor).getMax() < timestamp) {
          deleteCursor++;
        } else {
          return false;
        }
      }
      return false;
    }

    @Override
    public TimeValuePair nextTimeValuePair() throws IOException {
      if (hasCachedPair || hasNextTimeValuePair()) {
        hasCachedPair = false;
        return cachedTimeValuePair;
      } else {
        throw new IOException("no next time value pair");
      }
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return cachedTimeValuePair;
    }

    @Override
    public void close() throws IOException {
      // Do nothing because of this is an in memory object
    }
  }

  public abstract TSDataType getDataType();

  public long getLastTime() {
    return getTime(size - 1);
  }
}
