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

package org.apache.iotdb.db.engine.compaction.cross.inplace.manage;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.reader.resource.CachedUnseqResourceMergeReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask.MERGE_SUFFIX;

/**
 * CrossSpaceMergeResource manages files and caches of readers, writers, MeasurementSchemas and
 * modifications to avoid unnecessary object creations and file openings.
 */
public class CrossSpaceMergeResource { // 跨空间合并的资源管理器

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  // 每个TsFile对应的TsFileSequenceReader
  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();
  private Map<TsFileResource, RestorableTsFileIOWriter> fileWriterCache = new HashMap<>();
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();
  //存放某待合并TsFile的某设备ID的开始和结束时间
  private Map<TsFileResource, Map<String, Pair<Long, Long>>> startEndTimeCache =
      new HashMap<>(); // pair<startTime, endTime>
  //该跨空间合并的存储组logicalStorageGroup下的所有时间序列路径和对应的schema，Todo:bug!!为啥是物理存储组？因为每次跨空间合并是物理存储组下的某个虚拟存储组下的某个时间分区里的顺序和乱序文件
  private Map<PartialPath, IMeasurementSchema> measurementSchemaMap =
      new HashMap<>(); // is this too waste?
  private Map<IMeasurementSchema, ChunkWriterImpl> chunkWriterCache = new ConcurrentHashMap<>();

  private long ttlLowerBound = Long.MIN_VALUE;

  private boolean cacheDeviceMeta = false;

  public CrossSpaceMergeResource(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
    this.unseqFiles = unseqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
  }

  /** If returns true, it means to participate in the merge */
  private boolean filterResource(TsFileResource res) {
    return res.getTsFile().exists()
        && !res.isDeleted()
        && (!res.isClosed() || res.stillLives(ttlLowerBound))
        && !res.isMerging();
  }

  public CrossSpaceMergeResource(
      Collection<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, long ttlLowerBound) {
    this.ttlLowerBound = ttlLowerBound;
    this.seqFiles = seqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
    this.unseqFiles = unseqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
  }

  public void clear() throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }
    for (RestorableTsFileIOWriter writer : fileWriterCache.values()) {
      writer.close();
    }

    fileReaderCache.clear();
    fileWriterCache.clear();
    modificationCache.clear();
    measurementSchemaMap.clear();
    chunkWriterCache.clear();
  }

  public IMeasurementSchema getSchema(PartialPath path) {
    return measurementSchemaMap.get(path);
  }

  /**
   * Construct a new or get an existing RestorableTsFileIOWriter of a merge temp file for a SeqFile.
   * The path of the merge temp file will be the seqFile's + ".merge".
   *
   * @return A RestorableTsFileIOWriter of a merge temp file for a SeqFile.
   */
  //跨空间合并的临时目标文件是"顺序文件名.tsfile.merge" ，即xxx.tsfile.merge
  public RestorableTsFileIOWriter getMergeFileWriter(TsFileResource resource) throws IOException {
    RestorableTsFileIOWriter writer = fileWriterCache.get(resource);
    if (writer == null) {
      writer =
          new RestorableTsFileIOWriter(
              FSFactoryProducer.getFSFactory().getFile(resource.getTsFilePath() + MERGE_SUFFIX));
      fileWriterCache.put(resource, writer);
    }
    return writer;
  }

  /**
   * Query ChunkMetadata of a timeseries from the given TsFile (seq or unseq). The ChunkMetadata is
   * not cached since it is usually huge.
   *
   * @param path name of the time series
   */
  public List<ChunkMetadata> queryChunkMetadata(PartialPath path, TsFileResource seqFile)
      throws IOException {
    TsFileSequenceReader sequenceReader = getFileReader(seqFile);
    return sequenceReader.getChunkMetadataList(path, true);
  }

  /**
   * Construct the a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @return a TsFileSequenceReader
   */
  public TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getTsFilePath(), true, cacheDeviceMeta);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  /**
   * Construct UnseqResourceMergeReaders of for each timeseries over all seqFiles. The readers are
   * not cached since the method is only called once for each timeseries.
   *
   * @param paths names of the timeseries
   * @return an array of UnseqResourceMergeReaders each corresponding to a timeseries in paths
   */
  //首先获取每个序列在所有待合并乱序文件里的所有Chunk，并以此创建每个序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
  public IPointReader[] getUnseqReaders(List<PartialPath> paths) throws IOException {
    //按照序列在列表的位置i，把所有的时间序列在所有乱序文件里的所有Chunk,追加放到数组的第i个元素里，该元素是列表类型，依次存放着该位置的序列在所有乱序文件里的所有Chunk（可能出现某乱序文件不存在此序列），最后返回该数组
    List<Chunk>[] pathChunks = MergeUtils.collectUnseqChunks(paths, unseqFiles, this);
    //为每个序列
    IPointReader[] ret = new IPointReader[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      //该序列的数据类型
      TSDataType dataType = getSchema(paths.get(i)).getType();
      //使用该序列在所有待合并乱序文件里的所有Chunk 为该序列创建对应的数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
      ret[i] = new CachedUnseqResourceMergeReader(pathChunks[i], dataType);
    }
    return ret;
  }

  /**
   * Construct the a new or get an existing ChunkWriter of a measurement. Different timeseries of
   * the same measurement and data type shares the same instance.
   */
  public ChunkWriterImpl getChunkWriter(IMeasurementSchema measurementSchema) {
    return chunkWriterCache.computeIfAbsent(measurementSchema, ChunkWriterImpl::new);
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile.
   *
   * @param path name of the time series
   */
  //获取该TsFile对该序列的所有删除操作
  public List<Modification> getModifications(TsFileResource tsFileResource, PartialPath path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            tsFileResource, resource -> new LinkedList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().matchFullPath(path)) {
        pathModifications.add(modification);
      }
    }
    return pathModifications;
  }

  /**
   * Remove and close the writer of the merge temp file of a SeqFile. The merge temp file is also
   * deleted.
   *
   * @param tsFileResource the SeqFile
   */
  public void removeFileAndWriter(TsFileResource tsFileResource) throws IOException {
    RestorableTsFileIOWriter newFileWriter = fileWriterCache.remove(tsFileResource);
    if (newFileWriter != null) {
      newFileWriter.close();
      newFileWriter.getFile().delete();
    }
  }

  /**
   * Remove and close the reader of the TsFile. The TsFile is NOT deleted.
   *
   * @param resource the SeqFile
   */
  public void removeFileReader(TsFileResource resource) throws IOException {
    TsFileSequenceReader sequenceReader = fileReaderCache.remove(resource);
    if (sequenceReader != null) {
      sequenceReader.close();
    }
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  public void setSeqFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public void setUnseqFiles(List<TsFileResource> unseqFiles) {
    this.unseqFiles = unseqFiles;
  }

  // 移除此次合并任务里未被选中的文件的SequenceReader,清缓存
  public void removeOutdatedSeqReaders() throws IOException {
    Iterator<Entry<TsFileResource, TsFileSequenceReader>> entryIterator =
        fileReaderCache.entrySet().iterator();
    // 该虚拟存储组下该时间分区的跨空间合并资源管理器的顺序文件列表
    HashSet<TsFileResource> fileSet = new HashSet<>(seqFiles);
    while (entryIterator.hasNext()) {
      Entry<TsFileResource, TsFileSequenceReader> entry = entryIterator.next();
      TsFileResource tsFile = entry.getKey();
      if (!fileSet.contains(tsFile)) {
        TsFileSequenceReader reader = entry.getValue();
        reader.close();
        entryIterator.remove();
      }
    }
  }

  public void setCacheDeviceMeta(boolean cacheDeviceMeta) {
    this.cacheDeviceMeta = cacheDeviceMeta;
  }

  public void setMeasurementSchemaMap(Map<PartialPath, IMeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public void clearChunkWriterCache() {
    this.chunkWriterCache.clear();
  }

  public void updateStartTime(TsFileResource tsFileResource, String device, long startTime) {
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap =
        startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
    Pair<Long, Long> startEndTimePair =
        deviceStartEndTimePairMap.getOrDefault(device, new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE));
    long newStartTime = startEndTimePair.left > startTime ? startTime : startEndTimePair.left;
    deviceStartEndTimePairMap.put(device, new Pair<>(newStartTime, startEndTimePair.right));
    startEndTimeCache.put(tsFileResource, deviceStartEndTimePairMap);
  }

  public void updateEndTime(TsFileResource tsFileResource, String device, long endTime) {
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap =
        startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
    Pair<Long, Long> startEndTimePair =
        deviceStartEndTimePairMap.getOrDefault(device, new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE));
    long newEndTime = startEndTimePair.right < endTime ? endTime : startEndTimePair.right;
    deviceStartEndTimePairMap.put(device, new Pair<>(startEndTimePair.left, newEndTime));
    startEndTimeCache.put(tsFileResource, deviceStartEndTimePairMap);
  }

  public Map<String, Pair<Long, Long>> getStartEndTime(TsFileResource tsFileResource) {
    return startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
  }
}
