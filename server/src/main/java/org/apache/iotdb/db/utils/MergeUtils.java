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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }

  public static void writeTVPair(TimeValuePair timeValuePair, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader)
      throws IOException {
    return sequenceReader.getAllPaths();
  }

  public static long collectFileSizes(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getTsFileSize();
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      totalSize += tsFileResource.getTsFileSize();
    }
    return totalSize;
  }

  public static int writeChunkWithoutUnseq(Chunk chunk, IChunkWriter chunkWriter)
      throws IOException {
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    int ptWritten = 0;
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      for (int i = 0; i < batchData.length(); i++) {
        writeBatchPoint(batchData, i, chunkWriter);
      }
      ptWritten += batchData.length();
    }
    return ptWritten;
  }

  public static void writeBatchPoint(BatchData batchData, int i, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i));
        break;
      case DOUBLE:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i));
        break;
      case BOOLEAN:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i));
        break;
      case INT64:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i));
        break;
      case INT32:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i));
        break;
      case FLOAT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i));
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  public static long[] findTotalAndLargestSeriesChunkNum(
      TsFileResource tsFileResource, TsFileSequenceReader sequenceReader) throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = sequenceReader.getChunkMetadataList(path, true);
      totalChunkNum += chunkMetadataList.size();
      maxChunkNum = chunkMetadataList.size() > maxChunkNum ? chunkMetadataList.size() : maxChunkNum;
    }
    logger.debug(
        "In file {}, total chunk num {}, series max chunk num {}",
        tsFileResource,
        totalChunkNum,
        maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) {
    return seqFile.getTsFileSize() - sequenceReader.getFileMetadataPos();
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a file,
   * this method follows the order of positions of chunks instead of the order of timeseries, which
   * reduce disk seeks.
   *
   * @param paths names of the timeseries
   */
  // 将所有待合并序列在所有乱序文件里的所有Chunk依次放入ret列表数组里（ret数组长度为待合并序列数量），如有s0 s1 s2,其中s2在第1 3 5个乱序文件里都有好几个Chunk，则在ret[2]列表里存放该序列分别在1 3 5乱序文件的所有Chunk
  public static List<Chunk>[] collectUnseqChunks(
      List<PartialPath> paths, List<TsFileResource> unseqResources, MergeResource mergeResource)
      throws IOException {
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader tsFileReader = mergeResource.getFileReader(tsFileResource);
      // prepare metaDataList
      //将所有待合并序列在当前乱序文件里的ChunkMetadataList依次放入chunkMetaHeap队列，该队列元素为（待合并序列index,该序列在该乱序文件里的ChunkMetadataList）
      buildMetaHeap(paths, tsFileReader, mergeResource, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      //将所有待合并序列在该乱序文件里的所有Chunk依次放入ret里，ret长度为待合并序列数量，每个元素存放该序列在该乱序文件里的所有Chunk（包含删除区间）
      collectUnseqChunks(chunkMetaHeap, tsFileReader, ret);
    }
    return ret;
  }

  //将所有待合并序列在当前乱序文件里的ChunkMetadataList依次放入chunkMetaHeap队列，该队列元素为（待合并序列index,该序列在该乱序文件里的ChunkMetadataList）
  private static void buildMetaHeap(
      List<PartialPath> paths,
      TsFileSequenceReader tsFileReader,
      MergeResource resource,
      TsFileResource tsFileResource,
      PriorityQueue<MetaListEntry> chunkMetaHeap)
      throws IOException {
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      List<ChunkMetadata> metaDataList = tsFileReader.getChunkMetadataList(path, true);
      if (metaDataList.isEmpty()) {
        continue;
      }
      List<Modification> pathModifications = resource.getModifications(tsFileResource, path);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      MetaListEntry entry = new MetaListEntry(i, metaDataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  //将所有待合并序列在该乱序文件里的所有Chunk依次放入ret里，ret长度为待合并序列数量，每个元素存放该序列在该乱序文件里的所有Chunk（包含删除区间）
  private static void collectUnseqChunks(
      PriorityQueue<MetaListEntry> chunkMetaHeap,//（待合并序列index,该序列在该乱序文件里的ChunkMetadataList）
      TsFileSequenceReader tsFileReader,
      List<Chunk>[] ret)
      throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetadata currMeta = metaListEntry.current();
      Chunk chunk = tsFileReader.readMemChunk(currMeta);
      ret[metaListEntry.pathId].add(chunk);
      if (metaListEntry.hasNext()) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
  }

  // 判断当前待合并序列的是否有与该顺序文件的当前Chunk有否overlap，true则后续要解chunk，否则可以不解chunk
  // 当最小的乱序数据点时间戳<=顺序文件里该Chunk的结束时间，返回true
  // 若是最后一个chunk且乱序点时间小于该顺序文件该device的EndTime，返回true
  public static boolean isChunkOverflowed(
      TimeValuePair timeValuePair, //该待合并序列在所有乱序文件里时间戳最小的数据点
      ChunkMetadata metaData, //顺序文件里该序列的第一个chunkMetadata
      boolean isLastChunk,
      long currentResourceEndTime) {
    return timeValuePair != null
        && (timeValuePair.getTimestamp() <= metaData.getEndTime()
            || (isLastChunk && timeValuePair.getTimestamp() <= currentResourceEndTime));
  }

  // 判断是否太小，若否说明足够大，则后续可以直接将该顺序Chunk刷盘，无需解chunk；若为true，则后续要解该顺序Chunk：
  // 若在重写上个顺序Chunk的时候还有数据点仍留在目标ChunkWriter未被刷盘，则直接返回true，后续要解chunk；
  // 若当前顺序Chunk点很少且不是最后一个chunk,则返回true，后续要解chunk
  public static boolean isChunkTooSmall(
      int ptWritten, ChunkMetadata chunkMetaData, boolean isLastChunk, int minChunkPointNum) {
    return ptWritten > 0
        || (minChunkPointNum >= 0
            && chunkMetaData.getNumOfPoints() < minChunkPointNum
            && !isLastChunk);
  }

  public static List<List<PartialPath>> splitPathsByDevice(List<PartialPath> paths) {
    if (paths.isEmpty()) {
      return Collections.emptyList();
    }
    paths.sort(Comparator.comparing(PartialPath::getFullPath));

    String currDevice = null;
    List<PartialPath> currList = null;
    List<List<PartialPath>> ret = new ArrayList<>();
    for (PartialPath path : paths) {
      if (currDevice == null) {
        currDevice = path.getDevice();
        currList = new ArrayList<>();
        currList.add(path);
      } else if (path.getDevice().equals(currDevice)) {
        currList.add(path);
      } else {
        ret.add(currList);
        currDevice = path.getDevice();
        currList = new ArrayList<>();
        currList.add(path);
      }
    }
    ret.add(currList);
    return ret;
  }

  public static class MetaListEntry implements Comparable<MetaListEntry> {

    private int pathId;
    private int listIdx;
    private List<ChunkMetadata> chunkMetadataList;

    public MetaListEntry(int pathId, List<ChunkMetadata> chunkMetadataList) {
      this.pathId = pathId;
      this.listIdx = -1;
      this.chunkMetadataList = chunkMetadataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(
          this.current().getOffsetOfChunkHeader(), o.current().getOffsetOfChunkHeader());
    }

    public ChunkMetadata current() {
      return chunkMetadataList.get(listIdx);
    }

    public boolean hasNext() {
      return listIdx + 1 < chunkMetadataList.size();
    }

    public ChunkMetadata next() {
      return chunkMetadataList.get(++listIdx);
    }

    public int getPathId() {
      return pathId;
    }
  }
}
