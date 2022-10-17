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
package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FastCrossCompactionWriter extends AbstractCrossCompactionWriter {

  public FastCrossCompactionWriter(
      List<TsFileResource> targetResources, List<TsFileResource> seqSourceResources)
      throws IOException {
    super(targetResources, seqSourceResources);
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {}

  /**
   * Flush chunk to tsfile directly. Return whether the chunk is flushed to tsfile successfully or
   * not. Return false if the unsealed chunk is too small or the end time of chunk exceeds the end
   * time of file, else return true. Notice: if sub-value measurement is null, then flush empty
   * value chunk.
   */
  public boolean flushChunkToFileWriter(
      IChunkMetadata iChunkMetadata, TsFileSequenceReader reader, int subTaskId)
      throws IOException {
    checkTimeAndMayFlushChunkToCurrentFile(iChunkMetadata.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean isUnsealedChunkLargeEnough =
        chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(
            chunkSizeLowerBoundInCompaction, chunkPointNumLowerBoundInCompaction);
    if (!isUnsealedChunkLargeEnough
        || (iChunkMetadata.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize the
      // chunk
      return false;
    }

    TsFileIOWriter tsFileIOWriter = targetFileWriters.get(fileIndex);

    synchronized (tsFileIOWriter) {
      // seal last chunk to file writer
      chunkWriters[subTaskId].writeToFileWriter(tsFileIOWriter);
      if (iChunkMetadata instanceof AlignedChunkMetadata) {
        AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) iChunkMetadata;
        // flush time chunk
        ChunkMetadata chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
        tsFileIOWriter.writeChunk(reader.readMemChunk(chunkMetadata), chunkMetadata);
        // flush value chunks
        for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
          IChunkMetadata valueChunkMetadata =
              alignedChunkMetadata.getValueChunkMetadataList().get(i);
          if (valueChunkMetadata == null) {
            // sub sensor does not exist in current file or value chunk has been deleted completely
            AlignedChunkWriterImpl alignedChunkWriter =
                (AlignedChunkWriterImpl) chunkWriters[subTaskId];
            ValueChunkWriter valueChunkWriter = alignedChunkWriter.getValueChunkWriterByIndex(i);
            tsFileIOWriter.writeEmptyValueChunk(
                valueChunkWriter.getMeasurementId(),
                valueChunkWriter.getCompressionType(),
                valueChunkWriter.getDataType(),
                valueChunkWriter.getEncodingType(),
                valueChunkWriter.getStatistics());
            continue;
          }
          chunkMetadata = (ChunkMetadata) valueChunkMetadata;
          tsFileIOWriter.writeChunk(reader.readMemChunk(chunkMetadata), chunkMetadata);
        }
      } else {
        ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
        tsFileIOWriter.writeChunk(reader.readMemChunk(chunkMetadata), chunkMetadata);
      }
    }
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  /**
   * Flush aligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true. Notice: if sub-value measurement is null, then
   * flush empty value page.
   */
  public boolean flushAlignedPageToChunkWriter(
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId)
      throws IOException, PageException {
    checkTimeAndMayFlushChunkToCurrentFile(timePageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean isUnsealedPageLargeEnough =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction);
    if (!isUnsealedPageLargeEnough
        || (timePageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // unsealed page is too small or page.endTime > file.endTime, then deserialize the page
      return false;
    }

    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    alignedChunkWriter.sealCurrentPage();
    // flush new time page to chunk writer directly
    alignedChunkWriter.writePageHeaderAndDataIntoTimeBuff(compressedTimePageData, timePageHeader);

    // flush new value pages to chunk writer directly
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      if (valuePageHeaders.get(i) == null) {
        // sub sensor does not exist in current file or value page has been deleted completely
        alignedChunkWriter.getValueChunkWriterByIndex(i).writeEmptyPageToPageBuffer();
        continue;
      }
      alignedChunkWriter.writePageHeaderAndDataIntoValueBuff(
          compressedValuePageDatas.get(i), valuePageHeaders.get(i), i);
    }

    chunkPointNumArray[subTaskId] += timePageHeader.getStatistics().getCount();

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), alignedChunkWriter, subTaskId, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }

  /**
   * Flush nonAligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true.
   */
  public boolean flushPageToChunkWriter(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    checkTimeAndMayFlushChunkToCurrentFile(pageHeader.getStartTime(), subTaskId);
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean isUnsealedPageLargeEnough =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction);
    if (!isUnsealedPageLargeEnough
        || (pageHeader.getEndTime() > currentDeviceEndTime[fileIndex]
            && fileIndex != targetFileWriters.size() - 1)) {
      // unsealed page is too small or page.endTime > file.endTime, then deserialize the page
      return false;
    }

    ChunkWriterImpl chunkWriter = (ChunkWriterImpl) chunkWriters[subTaskId];
    // seal current page
    chunkWriter.sealCurrentPage();
    // flush new page to chunk writer directly
    chunkWriter.writePageHeaderAndDataIntoBuff(compressedPageData, pageHeader);

    chunkPointNumArray[subTaskId] += pageHeader.getStatistics().getCount();

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriter, subTaskId, true);
    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    return true;
  }
}
