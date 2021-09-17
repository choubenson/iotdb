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

package org.apache.iotdb.db.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.v2.read.TsFileSequenceReaderForV2;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileRewriteTool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewriteTool.class);

  /**
   * TsFileName -> modificationFile, it stores the name of TsFile currently being rewritten and the
   * tmp ModificationFile where deletions to this TsFile can be added to this tmp ModificationFile
   * during the rewritting process.
   */
  public static Map<String, ModificationFile> tmpFileModificationMap = new HashMap<>();

  protected TsFileSequenceReader reader;
  protected File oldTsFile;
  protected List<Modification> oldModification;
  protected TsFileResource oldTsFileResource;
  protected Iterator<Modification> modsIterator;

  protected Deletion currentMod;
  protected Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
  protected Decoder valueDecoder;

  /**
   * PartitionId -> TsFileIOWriter
   */
  protected Map<Long, TsFileIOWriter> partitionWriterMap;

  /**
   * Maximum index of plans executed within this TsFile.
   */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /**
   * Minimum index of plans executed within this TsFile.
   */
  protected long minPlanIndex = Long.MAX_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the real data and rewrite to some
   * new tsFiles.
   *
   * @throws IOException If some I/O error occurs
   */
  public TsFileRewriteTool(TsFileResource resourceToBeRewritten) throws IOException {
    oldTsFileResource = resourceToBeRewritten;
    oldTsFile = resourceToBeRewritten.getTsFile();
    String file = oldTsFile.getAbsolutePath();
    reader = new TsFileSequenceReader(file);
    partitionWriterMap = new HashMap<>();
    if (FSFactoryProducer.getFSFactory().getFile(file + ModificationFile.FILE_SUFFIX).exists()) {
      oldModification = (List<Modification>) resourceToBeRewritten.getModFile().getModifications();
      modsIterator = oldModification.iterator();
    }
  }

  public TsFileRewriteTool(TsFileResource resourceToBeRewritten, boolean needReaderForV2)
      throws IOException {
    oldTsFileResource = resourceToBeRewritten;
    oldTsFile = resourceToBeRewritten.getTsFile();
    String file = oldTsFile.getAbsolutePath();
    if (needReaderForV2) {
      reader = new TsFileSequenceReaderForV2(file);
    } else {
      reader = new TsFileSequenceReader(file);
    }
    partitionWriterMap = new HashMap<>();
    if (FSFactoryProducer.getFSFactory().getFile(file + ModificationFile.FILE_SUFFIX).exists()) {
      oldModification = (List<Modification>) resourceToBeRewritten.getModFile().getModifications();
      modsIterator = oldModification.iterator();
    }
  }


  /**
   * This method is used to move a new TsFile and its corresponding resource file to the correct
   *  folder.
   * @param oldTsFileResource
   * @param newTsFileResource if the old TsFile has not any deletions or all the data in which has been deleted , then this param will be null.
   * @throws IOException
   */
  public static void moveNewTsFile(TsFileResource oldTsFileResource,
      TsFileResource newTsFileResource) throws IOException {
    File newPartionDir = new File(oldTsFileResource.getTsFile().getParent() + File.separator + oldTsFileResource.getTimePartition());
    try {
      FSFactory fsFactory = FSFactoryProducer.getFSFactory();
      if (newTsFileResource == null) {
        return;
      }
      File oldTsFile = oldTsFileResource.getTsFile();
      File newTsFile = newTsFileResource.getTsFile();
      //newPartionDir = newTsFile.getParentFile();

      //move TsFile
      oldTsFile.delete();
      fsFactory.moveFile(newTsFile, oldTsFile);

      //move .resource File
      newTsFileResource.setFile(fsFactory.getFile(oldTsFile.getParent(), newTsFile.getName()));
      newTsFileResource.setClosed(true);
      try {
        newTsFileResource.serialize();
      } catch (IOException e) {
        e.printStackTrace();
      }
      File tmpResourceFile = fsFactory
          .getFile(newPartionDir, newTsFile.getName() + TsFileResource.RESOURCE_SUFFIX);
      if (tmpResourceFile.exists()) {
        tmpResourceFile.delete();
      }
    }finally {
      if(newPartionDir.exists())
        newPartionDir.delete(); //if the folder is empty, then it will be deleted
    }

  }

  public static void moveNewTsFiles(List<TsFileResource> oldTsFileResources,
      List<TsFileResource> newTsFileResources) {
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    File newPartionDir = null;
    for (int i = 0; i < oldTsFileResources.size(); i++) {
      if (newTsFileResources.get(i) == null) {
        continue;
      }
      File oldTsFile = oldTsFileResources.get(i).getTsFile();
      File newTsFile = newTsFileResources.get(i).getTsFile();
      newPartionDir = newTsFile.getParentFile();
      oldTsFile.delete();
      fsFactory.moveFile(newTsFile, oldTsFile);
      newTsFileResources.get(i)
          .setFile(fsFactory.getFile(oldTsFile.getParent(), newTsFile.getName()));
      newTsFileResources.get(i).setClosed(true);
      try {
        newTsFileResources.get(i).serialize();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (newPartionDir.exists()) {
      String[] childrenFiles = newPartionDir.list();
      for (String file : childrenFiles) {
        File f = new File(newPartionDir, file);
        f.delete();
      }
      newPartionDir.delete();
    }
  }

  /**
   * Rewrite an old file to the latest version
   *
   * @param resourceToBeRewritten the tsfile which to be rewrite
   * @param rewrittenResources the rewritten files
   */
  public static void rewriteTsFile(
      TsFileResource resourceToBeRewritten, List<TsFileResource> rewrittenResources)
      throws IOException, WriteProcessException {
    try (TsFileRewriteTool rewriteTool = new TsFileRewriteTool(resourceToBeRewritten)) {
      rewriteTool.parseAndRewriteFile(rewrittenResources);
    }
  }

  /**
   * this method is used to write the modifications produced during the rewritting process to the
   * corresponding modification file of the new TsFile.
   */
  public static void writeNewModification(TsFileResource oldTsFileResource,
      TsFileResource newTsfileResource)
      throws IOException {
    ModificationFile tmpModFile = tmpFileModificationMap
        .get(oldTsFileResource.getTsFile().getName());
    Collection<Modification> newMods=null;
    if(tmpModFile!=null)
      newMods = tmpModFile.getModifications();
    tmpModFile.remove();
    if (newMods.size() == 0) {
      return;
    }
    if (newTsfileResource != null) {
      ModificationFile newModsFile = new ModificationFile(
          oldTsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX);
      for (Modification mod : newMods) {
        mod.setFileOffset(newTsfileResource.getTsFileSize());
        newModsFile.write(mod);
      }
      newTsfileResource.setModFile(newModsFile);
    }
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  /**
   * Parse the old files and generate some new files according to the time partition interval.
   *
   * @throws IOException WriteProcessException
   */
  @SuppressWarnings({"squid:S3776", "deprecation"}) // Suppress high Cognitive Complexity warning
  public void parseAndRewriteFile(List<TsFileResource> rewrittenResources)
      throws IOException, WriteProcessException {
    // check if the TsFile has correct header
    if (!fileCheck()) {
      return;
    }
    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
    reader.position(headerLength);
    // start to scan chunks and chunkGroups
    byte marker;

    String deviceId = null;
    boolean firstChunkInChunkGroup = true;
    try {
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            deviceId = chunkGroupHeader.getDeviceID();
            firstChunkInChunkGroup = true;
            endChunkGroup();
            break;
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    header.getMeasurementID(),
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());
            TSDataType dataType = header.getDataType();
            TSEncoding encoding = header.getEncodingType();
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            List<Boolean> needToDecodeInfo = new ArrayList<>();
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              // a new Page
              PageHeader pageHeader =
                  reader.readPageHeader(dataType, header.getChunkType() == MetaMarker.CHUNK_HEADER);
              boolean needToDecode = checkIfNeedToDecode(dataType, encoding, pageHeader);
              needToDecodeInfo.add(needToDecode);
              ByteBuffer pageData =
                  !needToDecode
                      ? reader.readCompressedPage(pageHeader)
                      : reader.readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
              dataSize -= pageHeader.getSerializedPageSize();
            }
            reWriteChunk(
                deviceId,
                firstChunkInChunkGroup,
                measurementSchema,
                pageHeadersInChunk,
                dataInChunk,
                needToDecodeInfo);
            firstChunkInChunkGroup = false;
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            // write plan indices for ending memtable
            for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
              long tmpMinPlanIndex = reader.getMinPlanIndex();
              if (tmpMinPlanIndex < minPlanIndex) {
                minPlanIndex = tmpMinPlanIndex;
              }

              long tmpMaxPlanIndex = reader.getMaxPlanIndex();
              if (tmpMaxPlanIndex < maxPlanIndex) {
                maxPlanIndex = tmpMaxPlanIndex;
              }

              tsFileIOWriter.setMaxPlanIndex(tmpMinPlanIndex);
              tsFileIOWriter.setMaxPlanIndex(tmpMaxPlanIndex);
              tsFileIOWriter.writePlanIndices();
            }
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      endChunkGroup();
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        rewrittenResources.add(endFileAndGenerateResource(tsFileIOWriter));
      }
      deleteOldModificationFile();

    } catch (IOException e2) {
      throw new IOException(
          "TsFile rewrite process cannot proceed at position "
              + reader.position()
              + "because: "
              + e2.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * this method is used to delete the old modification file after finishing rewriting.
   */
  public void deleteOldModificationFile() throws IOException {
    oldTsFileResource.removeModFile();
  }

  /**
   * This method will be invoked when all the data in this tsfile has been deleted.
   */
  protected void dealWithEmptyFile() throws IOException {
    deleteOldModificationFile();
    oldTsFileResource.delete();
  }

  /**
   * If the page have no statistics or crosses multi partitions, will return true, otherwise return
   * false.
   */
  protected boolean checkIfNeedToDecode(
      TSDataType dataType, TSEncoding encoding, PageHeader pageHeader) {
    if (pageHeader.getStatistics() == null) {
      return true;
    }
    // Decode is required if the page has data to be deleted. Otherwise, decode is not required
    if (oldModification != null) {
      modsIterator = oldModification.iterator();
      Deletion currentDeletion = null;
      while (modsIterator.hasNext()) {
        currentDeletion = (Deletion) modsIterator.next();
        if (pageHeader.getStartTime() <= currentDeletion.getEndTime()
            && pageHeader.getEndTime() >= currentDeletion.getStartTime()) {
          return true;
        }
      }
    }
    return StorageEngine.getTimePartition(pageHeader.getStartTime())
        != StorageEngine.getTimePartition(pageHeader.getEndTime());
  }

  /**
   * This method is for rewriting the Chunk which data is in the different time partitions. In this
   * case, we have to decode the data to points, and then rewrite the data points to different
   * chunkWriters, finally write chunks to their own upgraded TsFiles.
   */
  protected void reWriteChunk(
      String deviceId,
      boolean firstChunkInChunkGroup,
      MeasurementSchema schema,
      List<PageHeader> pageHeadersInChunk,
      List<ByteBuffer> pageDataInChunk,
      List<Boolean> needToDecodeInfoInChunk)
      throws IOException, PageException {
    valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());
    Map<Long, ChunkWriterImpl> partitionChunkWriterMap = new HashMap<>();
    for (int i = 0; i < pageDataInChunk.size(); i++) {
      if (Boolean.TRUE.equals(needToDecodeInfoInChunk.get(i))) {
        decodeAndWritePage(schema, pageDataInChunk.get(i), partitionChunkWriterMap);
      } else {
        writePage(
            schema, pageHeadersInChunk.get(i), pageDataInChunk.get(i), partitionChunkWriterMap);
      }
    }
    for (Entry<Long, ChunkWriterImpl> entry : partitionChunkWriterMap.entrySet()) {
      long partitionId = entry.getKey();
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);
      if (firstChunkInChunkGroup || !tsFileIOWriter.isWritingChunkGroup()) {
        tsFileIOWriter.startChunkGroup(deviceId);
      }
      // write chunks to their own upgraded tsFiles
      IChunkWriter chunkWriter = entry.getValue();
      chunkWriter.writeToFileWriter(tsFileIOWriter);
    }
  }

  protected void endChunkGroup() throws IOException {
    for (TsFileIOWriter tsFileIoWriter : partitionWriterMap.values()) {
      tsFileIoWriter.endChunkGroup();
    }
  }

  public String upgradeTsFileName(String oldTsFileName) {
    return oldTsFileName;
  }

  protected TsFileIOWriter getOrDefaultTsFileIOWriter(File oldTsFile, long partition) {
    return partitionWriterMap.computeIfAbsent(
        partition,
        k -> {
          File partitionDir =
              FSFactoryProducer.getFSFactory()
                  .getFile(oldTsFile.getParent() + File.separator + partition);
          if (!partitionDir.exists()) {
            partitionDir.mkdirs();
          }
          File newFile =
              FSFactoryProducer.getFSFactory()
                  .getFile(partitionDir + File.separator + upgradeTsFileName(oldTsFile.getName()));
          try {
            if (newFile.exists()) {
              logger.debug("delete uncomplated file {}", newFile);
              Files.delete(newFile.toPath());
            }
            if (!newFile.createNewFile()) {
              logger.error("Create new TsFile {} failed because it exists", newFile);
            }
            TsFileIOWriter writer = new TsFileIOWriter(newFile);
            addTmpModsOfCurrentTsFile(partition);
            return writer;
          } catch (IOException e) {
            logger.error("Create new TsFile {} failed ", newFile, e);
            return null;
          }
        });
  }

  protected void addTmpModsOfCurrentTsFile(long partition) throws IOException {
    if (!tmpFileModificationMap.containsKey(oldTsFile.getName())) {
      File f=new File(oldTsFile.getParent() + File.separator + partition+ File.separator+upgradeTsFileName(oldTsFile.getName())+ ModificationFile.FILE_SUFFIX);
      if(!f.getParentFile().exists()){
        f.getParentFile().mkdirs();
      }
      tmpFileModificationMap.put(oldTsFile.getName(),
          new ModificationFile(f.getAbsolutePath()));
    }
  }

  protected void writePage(
      MeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap)
      throws PageException {
    long partitionId = StorageEngine.getTimePartition(pageHeader.getStartTime());
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
    ChunkWriterImpl chunkWriter =
        partitionChunkWriterMap.computeIfAbsent(partitionId, v -> new ChunkWriterImpl(schema));
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader);
  }

  protected void decodeAndWritePage(
      MeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap)
      throws IOException {
    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    // read delete time range from old modification file
    List<TimeRange> deleteIntervalList = new ArrayList<>();
    if (oldModification != null) {
      modsIterator = oldModification.iterator();
      Deletion currentDeletion = null;
      while (modsIterator.hasNext()) {
        currentDeletion = (Deletion) modsIterator.next();
        deleteIntervalList.add(
            new TimeRange(currentDeletion.getStartTime(), currentDeletion.getEndTime()));
      }
    }
    pageReader.setDeleteIntervalList(deleteIntervalList);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    rewritePageIntoFiles(batchData, schema, partitionChunkWriterMap);
  }

  protected void rewritePageIntoFiles(
      BatchData batchData,
      MeasurementSchema schema,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap) {
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      long partitionId = StorageEngine.getTimePartition(time);

      ChunkWriterImpl chunkWriter =
          partitionChunkWriterMap.computeIfAbsent(partitionId, v -> new ChunkWriterImpl(schema));
      getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
      switch (schema.getType()) {
        case INT32:
          chunkWriter.write(time, (int) value, false);
          break;
        case INT64:
          chunkWriter.write(time, (long) value, false);
          break;
        case FLOAT:
          chunkWriter.write(time, (float) value, false);
          break;
        case DOUBLE:
          chunkWriter.write(time, (double) value, false);
          break;
        case BOOLEAN:
          chunkWriter.write(time, (boolean) value, false);
          break;
        case TEXT:
          chunkWriter.write(time, (Binary) value, false);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schema.getType()));
      }
      batchData.next();
    }
    partitionChunkWriterMap
        .values()
        .forEach(
            writer -> {
              writer.sealCurrentPage();
            });
  }

  /**
   * check if the file has correct magic strings and version number
   */
  protected boolean fileCheck() throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    byte versionNumber = reader.readVersionNumber();
    if (versionNumber != TSFileConfig.VERSION_NUMBER) {
      logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }

  protected TsFileResource endFileAndGenerateResource(TsFileIOWriter tsFileIOWriter)
      throws IOException {
    tsFileIOWriter.endFile();
    TsFileResource tsFileResource = new TsFileResource(tsFileIOWriter.getFile());
    Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        tsFileIOWriter.getDeviceTimeseriesMetadataMap();
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      String device = entry.getKey();
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.setMinPlanIndex(minPlanIndex);
    tsFileResource.setMaxPlanIndex(maxPlanIndex);
    tsFileResource.setClosed(true);
    tsFileResource.serialize();
    return tsFileResource;
  }
}
