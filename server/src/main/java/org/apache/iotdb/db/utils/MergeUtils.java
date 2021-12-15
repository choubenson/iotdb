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

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }

  // 使用ChunkWriter将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
  public static void writeTVPair(TimeValuePair timeValuePair, ChunkWriterImpl chunkWriter) {
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

  // 获取该TsFile的所有时间序列
  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader)
      throws IOException {
    return sequenceReader.getAllPaths();
  }

  //获取顺序和乱序文件的总文件大小
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

  /**
   * 将该待合并顺序文件里的某一待合并序列传感器的Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
   *
   * @param chunk 该待合并顺序文件里的某一待合并序列传感器的Chunk
   * @param chunkWriter 该待被合并序列的ChunkWriter，用于往临时目标文件写入
   * @return
   * @throws IOException
   */
  public static int writeChunkWithoutUnseq(Chunk chunk, ChunkWriterImpl chunkWriter)
      throws IOException {
    //创建该待合并顺序文件里的该待合并序列传感器的Chunk的阅读器
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    //总共往临时目标文件该序列的ChunkWriter里写入的数据点数量，起始就是参数chunk的数据点数量
    int ptWritten = 0;
    while (chunkReader.hasNextSatisfiedPage()) {
      //获取该Chunk的下一page的数据
      BatchData batchData = chunkReader.nextPageData();
      for (int i = 0; i < batchData.length(); i++) {
        //将batchData第i个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
        writeBatchPoint(batchData, i, chunkWriter);
      }
      ptWritten += batchData.length();
    }
    return ptWritten;
  }

  //将batchData第i个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
  public static void writeBatchPoint(BatchData batchData, int i, ChunkWriterImpl chunkWriter) {
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
        // 将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则往对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里写入该page的pageHeader和pageData（即pageWriter对象里输出流timeOut和valueOut的缓存数据），最后重置该pageWriter
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
  // 获取该TsFile里所有时间序列的Chunk的总数量和以及单时间序列的最大Chunk数量
  public static long[] findTotalAndLargestSeriesChunkNum(
      TsFileResource tsFileResource, TsFileSequenceReader sequenceReader) throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    // 获取该TsFile的所有时间序列
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

  // 获取该文件的索引区的大小
  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) {
    return seqFile.getTsFileSize()
        - sequenceReader.getFileMetadataPos(); // 文件长度-该TsFile的IndexOfTimeseriesIndex索引的开始处所在的偏移位置
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a file,
   * this method follows the order of positions of chunks instead of the order of timeseries, which
   * reduce disk seeks.
   *
   * @param paths names of the timeseries
   */
  //按照序列在列表的位置i，把所有的时间序列在所有乱序文件里的所有Chunk,追加放到数组的第i个元素里，该元素是列表类型，依次存放着该位置的序列在所有乱序文件里的所有Chunk（可能出现某乱序文件不存在此序列），最后返回该数组
  public static List<Chunk>[] collectUnseqChunks(
      List<PartialPath> paths,  //时间序列路径列表
      List<TsFileResource> unseqResources,  //待被合并的乱序文件
      CrossSpaceMergeResource mergeResource)  //跨空间合并资源管理器
      throws IOException {
    //该数组里每个元素存放了每个时间序列的所有Chunk
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    //遍历每个乱序待合并文件
    for (TsFileResource tsFileResource : unseqResources) {
      //获取该乱序待合并文件的顺序阅读器
      TsFileSequenceReader tsFileReader = mergeResource.getFileReader(tsFileResource);
      // prepare metaDataList
      //遍历每个时间序列，获取他们在指定乱序文件里的所有ChunkMetadataList，并根据相应删除操作过滤和修改（若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)），然后把“每个时间序列是在列表中的第几个，以及他们对应ChunkMetadataList”封装到MetaListEntry对象，并放入chunkMetaHeap队列
      buildMetaHeap(paths, tsFileReader, mergeResource, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      //将每个序列在该乱序文件里的所有Chunk追加放入ret数组里，该序列在列表里是第几个，就追加放到ret里第几个位置，因此ret可能出现某位置元素为null，因为该乱序文件里没有该序列
      collectUnseqChunks(chunkMetaHeap, tsFileReader, ret);
    }
    return ret;
  }

  //遍历每个时间序列，获取他们在指定乱序文件里的所有ChunkMetadataList，并根据相应删除操作过滤和修改（若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)），然后把“每个时间序列是在列表中的第几个，以及他们对应ChunkMetadataList”封装到MetaListEntry对象，并放入chunkMetaHeap队列
  private static void buildMetaHeap(
      List<PartialPath> paths,  //时间序列路径列表
      TsFileSequenceReader tsFileReader,  //乱序待合并文件的顺序阅读器
      CrossSpaceMergeResource resource,
      TsFileResource tsFileResource,  //乱序待合并文件
      PriorityQueue<MetaListEntry> chunkMetaHeap)
      throws IOException {
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      //根据给定的时间序列path，获取其TimeseriesMetadata对象里的所有ChunkMetadata，并按照每个ChunkMetadata的开始时间戳从小到大进行排序并返回
      List<ChunkMetadata> metaDataList = tsFileReader.getChunkMetadataList(path, true);
      if (metaDataList.isEmpty()) {
        continue;
      }
      //获取该TsFile对该序列的所有删除操作
      List<Modification> pathModifications = resource.getModifications(tsFileResource, path);
      if (!pathModifications.isEmpty()) {
        //根据给定的对该序列的删除操作列表和该序列的ChunkMetadata列表，若chunkmetadata对应chunk的数据被完全删除了，则从列表中移除此chunkMetadata，否则将其setModified(true)
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      //存放了当前序列是在列表中的第几个，并把它的ChunkMetadataList封装到MetaListEntry对象
      MetaListEntry entry = new MetaListEntry(i, metaDataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  //将每个序列在该乱序文件里的所有Chunk放入ret数组里，该序列在列表里是第几个，就放到ret里第几个位置，因此ret可能出现某位置元素为null，因为该乱序文件里没有该序列
  private static void collectUnseqChunks(
      PriorityQueue<MetaListEntry> chunkMetaHeap,
      TsFileSequenceReader tsFileReader,
      List<Chunk>[] ret)
      throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetadata currMeta = metaListEntry.current();
      //根据ChunkMetadata获取其在对应乱序TsFile里的所有Chunk
      Chunk chunk = tsFileReader.readMemChunk(currMeta);
      ret[metaListEntry.pathId].add(chunk);
      if (metaListEntry.hasNext()) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
  }
  //此处是查看该序列在所有乱序文件里是否存在数据点与该顺序Chunk有overlap，那么就要求当乱序数据时间戳小于等于该顺序Chunk的最大时间，就称为有overlap
  //判断该序列在所有乱序文件里是否存在数据点其时间是小于等于该顺序Chunk的结束时间，当存在小于等于的数据则说明该序列的那些乱序文件里存在数据与该顺序Chunk有overlap。
  public static boolean isChunkOverflowed(TimeValuePair timeValuePair, ChunkMetadata metaData) {
    return timeValuePair != null && timeValuePair.getTimestamp() <= metaData.getEndTime();
  }

  //判断当前顺序待合并文件里的该Chunk的数据点数量是否太少，小于系统预设的数量100000
  public static boolean isChunkTooSmall(
      int ptWritten, ChunkMetadata chunkMetaData, boolean isLastChunk, int minChunkPointNum) {
    return ptWritten > 0
        || (minChunkPointNum >= 0
            && chunkMetaData.getNumOfPoints() < minChunkPointNum
            && !isLastChunk);
  }

  //根据设备种类把传来的序列路径进行分开存放
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
    //该序列路径是队列中第几个
    private int pathId;
    private int listIdx;
    //该时间序列对应在某文件里的所有ChunkMetadata
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
