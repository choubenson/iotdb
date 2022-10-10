package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.FileElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.writer.NewFastCrossCompactionWriter;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NonAlignedFastCompactionPerformerSubTask extends FastCompactionPerformerSubTask {
  // the indexs of the timseries to be compacted to which the current sub thread is assigned
  private List<Integer> pathsIndex;

  protected List<String> allMeasurements;

  protected int currentSensorIndex = 0;

  boolean hasStartMeasurement = false;

  public NonAlignedFastCompactionPerformerSubTask(
      NewFastCrossCompactionWriter compactionWriter,
      String deviceId,
      List<Integer> pathsIndex,
      List<String> allMeasurements,
      FastCompactionPerformer fastCompactionPerformer,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      int subTaskId) {
    super(
        compactionWriter,
        fastCompactionPerformer,
        timeseriesMetadataOffsetMap,
        deviceId,
        false,
        subTaskId);
    this.pathsIndex = pathsIndex;
    this.allMeasurements = allMeasurements;
  }

  @Override
  public Void call()
      throws IOException, PageException, WriteProcessException, IllegalPathException {

    for (Integer index : pathsIndex) {
      fastCompactionPerformer.getSortedSourceFiles().forEach(x -> fileList.add(new FileElement(x)));

      currentSensorIndex = index;
      hasStartMeasurement = false;

      compactFiles();
      if (hasStartMeasurement) {
        compactionWriter.endMeasurement(subTaskId);
      }
    }

    return null;
  }

  protected void startMeasurement() throws IOException {
    if (!hasStartMeasurement && !chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      MeasurementSchema measurementSchema =
          fastCompactionPerformer
              .getReaderFromCache(firstChunkMetadataElement.fileElement.resource)
              .getMeasurementSchema(
                  Collections.singletonList(firstChunkMetadataElement.chunkMetadata));
      compactionWriter.startMeasurement(Collections.singletonList(measurementSchema), subTaskId);
      hasStartMeasurement = true;
    }
  }

  /** Deserialize files into chunk metadatas and put them into the chunk metadata queue. */
  void deserializeFileIntoQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException {
    for (FileElement fileElement : fileElements) {
      TsFileResource resource = fileElement.resource;
      Pair<Long, Long> timeseriesMetadataOffset =
          timeseriesMetadataOffsetMap.get(allMeasurements.get(currentSensorIndex)).get(resource);
      if (timeseriesMetadataOffset == null) {
        // tsfile does not contain this timeseries
        removeFile(fileElement);
        continue;
      }
      List<IChunkMetadata> iChunkMetadataList =
          fastCompactionPerformer
              .getReaderFromCache(resource)
              .getChunkMetadataListByTimeseriesMetadataOffset(
                  timeseriesMetadataOffset.left, timeseriesMetadataOffset.right);

      if (iChunkMetadataList.size() > 0) {
        // modify chunk metadatas
        QueryUtils.modifyChunkMetaData(
            iChunkMetadataList,
            fastCompactionPerformer.getModifications(
                resource,
                new PartialPath(deviceId, iChunkMetadataList.get(0).getMeasurementUid())));
        if (iChunkMetadataList.size() == 0) {
          // all chunks has been deleted in this file, just remove it
          removeFile(fileElement);
        }
      }

      for (int i = 0; i < iChunkMetadataList.size(); i++) {
        IChunkMetadata chunkMetadata = iChunkMetadataList.get(i);
        // set file path
        chunkMetadata.setFilePath(resource.getTsFilePath());

        // add into queue
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                chunkMetadata,
                (int) resource.getVersion(),
                i == iChunkMetadataList.size() - 1,
                fileElement));
      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement) throws IOException {
    Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetadataElement.chunkMetadata);
    ChunkReader chunkReader = new ChunkReader(chunk);
    ByteBuffer chunkDataBuffer = chunk.getData();
    ChunkHeader chunkHeader = chunk.getHeader();
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

      boolean isLastPage = chunkDataBuffer.remaining() <= 0;
      pageQueue.add(
          new PageElement(
              pageHeader,
              compressedPageData,
              chunkReader,
              chunkMetadataElement,
              isLastPage,
              chunkMetadataElement.priority));
    }
  }

  /**
   * -1 means that no data on this page has been deleted. <br>
   * 0 means that there is data on this page been deleted. <br>
   * 1 means that all data on this page has been deleted.
   */
  protected int isPageModified(PageElement pageElement) {
    long startTime = pageElement.startTime;
    long endTime = pageElement.pageHeader.getEndTime();
    return checkIsModified(
        startTime, endTime, pageElement.chunkMetadataElement.chunkMetadata.getDeleteIntervalList());
  }
}
