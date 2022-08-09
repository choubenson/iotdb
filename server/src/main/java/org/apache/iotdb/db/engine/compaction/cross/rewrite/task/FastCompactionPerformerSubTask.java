package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.reader.resource.CachedUnseqResourceMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class FastCompactionPerformerSubTask implements Callable<Void> {

  private final TsFileResource seqFile;

  private final int seqFileIndex;

  private final boolean isLastFile;

  // all measurements id of this device
  private final List<String> allMeasurements;

  // the indexs of the timseries to be compacted to which the current sub thread is assigned
  private final List<Integer> pathsIndex;

  // chunk metadata list of all timeseries of this device in this seq file
  private final List<List<ChunkMetadata>> allSensorMetadatas;

  private final FastCrossCompactionWriter fastCrossCompactionWriter;

  private final FastCompactionPerformer fastCompactionPerformer;

  private final String deviceID;

  private final int subTaskId;

  public FastCompactionPerformerSubTask(
      int seqFileIndex,
      List<String> allMeasurements,
      List<Integer> pathsIndex,
      List<List<ChunkMetadata>> allSensorMetadatas,
      FastCrossCompactionWriter fastCrossCompactionWriter,
      FastCompactionPerformer fastCompactionPerformer,
      String deviceID,
      int subTaskId) {
    this.seqFile = fastCompactionPerformer.getSeqFiles().get(seqFileIndex);
    this.seqFileIndex = seqFileIndex;
    this.isLastFile = seqFileIndex == fastCompactionPerformer.getSeqFiles().size() - 1;
    this.allMeasurements = allMeasurements;
    this.pathsIndex = pathsIndex;
    this.allSensorMetadatas = allSensorMetadatas;
    this.fastCrossCompactionWriter = fastCrossCompactionWriter;
    this.fastCompactionPerformer = fastCompactionPerformer;
    this.deviceID = deviceID;
    this.subTaskId = subTaskId;
  }

  @Override
  public Void call() throws Exception {
    for (Integer index : pathsIndex) {
      // get unseq reader of timeseries
      IPointReader unseqReader =
          fastCompactionPerformer.unseqReaders.computeIfAbsent(
              index,
              integer -> {
                try {
                  return getUnseqReaders(new PartialPath(deviceID, allMeasurements.get(index)));
                } catch (IOException | IllegalPathException e) {
                  throw new RuntimeException(e);
                }
              });

      fastCrossCompactionWriter.startMeasurement(
          Collections.singletonList(
              fastCompactionPerformer.schemaMapCache.get(allMeasurements.get(index))),
          false,
          subTaskId);

      // iterate seq chunk
      List<ChunkMetadata> curSensorMetadataList = allSensorMetadatas.get(index);
      if (curSensorMetadataList != null) {
        for (int i = 0; i < curSensorMetadataList.size(); i++) {
          ChunkMetadata curChunkMetadata = curSensorMetadataList.get(i);
          curChunkMetadata.setFilePath(seqFile.getTsFilePath());

          boolean isLastChunk = i == curSensorMetadataList.size() - 1;
          boolean isChunkOverlap =
              isChunkOverlap(
                  unseqReader.currentTimeValuePair(),
                  curChunkMetadata,
                  isLastChunk,
                  seqFile.getEndTime(deviceID));
          boolean isChunkModified =
              (curChunkMetadata.getDeleteIntervalList() != null
                  && !curChunkMetadata.getDeleteIntervalList().isEmpty());
          Chunk chunk = ChunkCache.getInstance().get(curChunkMetadata);
          if (isChunkOverlap) {
            fastCrossCompactionWriter.compactWithOverlapSeqChunk(chunk, unseqReader, subTaskId);
          } else {
            fastCrossCompactionWriter.compactWithNonOverlapSeqChunk(
                chunk, isChunkModified, curChunkMetadata, seqFileIndex, subTaskId);
          }
        }
      }
      // write remaining unseq data points
      fastCrossCompactionWriter.writeRemainingUnseqPoints(
          unseqReader, isLastFile ? Long.MAX_VALUE : seqFile.getEndTime(deviceID), subTaskId);
      fastCrossCompactionWriter.endMeasurment(seqFileIndex, subTaskId);
    }
    return null;
  }

  private boolean isChunkOverlap(
      TimeValuePair timeValuePair,
      ChunkMetadata metaData,
      boolean isLastChunk,
      long currentResourceEndTime) {
    return timeValuePair != null
        && (timeValuePair.getTimestamp() <= metaData.getEndTime()
            || (isLastChunk && timeValuePair.getTimestamp() <= currentResourceEndTime));
  }

  public IPointReader getUnseqReaders(PartialPath path) throws IOException {
    // 将所有待合并序列在所有乱序文件里的所有Chunk依次放入ret列表数组里（ret数组长度为待合并序列数量），如有s0 s1 s2,其中s2在第1 3
    // 5个乱序文件里都有好几个Chunk，则在ret[2]列表里存放该序列分别在1 3 5乱序文件的所有Chunk
    List<Chunk> pathChunks = collectUnseqChunks(path, fastCompactionPerformer.getUnseqFiles());

    int size = pathChunks.size();
    TSDataType dataType =
        size > 0 ? pathChunks.get(size - 1).getHeader().getDataType() : TSDataType.TEXT;
    return new CachedUnseqResourceMergeReader(pathChunks, dataType);
  }

  public List<Chunk> collectUnseqChunks(PartialPath path, List<TsFileResource> unseqResources)
      throws IOException {
    List<Chunk> chunks = new ArrayList<>();
    List<ChunkMetadata> unseqChunkMetadataList = new ArrayList<>();
    for (TsFileResource tsFileResource : unseqResources) {
      TsFileSequenceReader unseqReader = fastCompactionPerformer.getReaderFromCache(tsFileResource);
      // prepare metaDataList
      // 将所有待合并序列在当前乱序文件里的ChunkMetadataList依次放入chunkMetaHeap队列，该队列元素为（待合并序列index,该序列在该乱序文件里的ChunkMetadataList）
      buildMetaHeap(path, unseqReader, tsFileResource, unseqChunkMetadataList);

      // read all chunks of timeseries in the unseq file in order
      for (ChunkMetadata unseqChunkMetadata : unseqChunkMetadataList) {
        chunks.add(unseqReader.readMemChunk(unseqChunkMetadata));
      }
      unseqChunkMetadataList.clear();
    }
    return chunks;
  }

  /**
   * Put all the ChunkMetadataList of the timeseries to be compacted in the unseq file into the
   * chunkMetaHeap queue.
   */
  private void buildMetaHeap(
      PartialPath path,
      TsFileSequenceReader tsFileReader,
      TsFileResource tsFileResource,
      List<ChunkMetadata> unseqChunkMetadataList)
      throws IOException {
    unseqChunkMetadataList.addAll(tsFileReader.getChunkMetadataList(path, true));
    if (unseqChunkMetadataList.isEmpty()) {
      return;
    }
    List<Modification> pathModifications =
        fastCompactionPerformer.getModifications(tsFileResource, path);
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(unseqChunkMetadataList, pathModifications);
    }
  }

  /** MetaListEntry stores all chunkmetadatas of a timeseries in an unseq file. */
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
