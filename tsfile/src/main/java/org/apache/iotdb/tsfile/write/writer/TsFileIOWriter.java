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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.tsmiterator.TSMIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import static org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor.addCurrentIndexNodeToQueue;
import static org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor.checkAndBuildLevelIndex;
import static org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor.generateRootNode;

/**
 * TsFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter implements AutoCloseable {

  protected static final byte[] MAGIC_STRING_BYTES;
  public static final byte VERSION_NUMBER_BYTE;
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  static {
    MAGIC_STRING_BYTES = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    VERSION_NUMBER_BYTE = TSFileConfig.VERSION_NUMBER;
  }

  protected TsFileOutput out;
  protected boolean canWrite = true;
  protected File file;

  // current flushed Chunk
  protected ChunkMetadata currentChunkMetadata;
  // current flushed ChunkGroup
  protected List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  // all flushed ChunkGroups
  protected List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();

  private long markedPosition;
  private String currentChunkGroupDeviceId;

  // for upgrade tool and split tool
  Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap;

  // the two longs marks the index range of operations in current MemTable
  // and are serialized after MetaMarker.OPERATION_INDEX_RANGE to recover file-level range
  private long minPlanIndex;
  private long maxPlanIndex;

  // the following variable is used for memory control
  protected long maxMetadataSize;
  protected long currentChunkMetadataSize = 0L;
  protected File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected volatile boolean hasChunkMetadataInDisk = false;
  protected String currentSeries = null;
  // record the total num of path in order to make bloom filter
  protected int pathCount = 0;
  protected boolean enableMemoryControl = false;
  Path lastSerializePath = null;
  private LinkedList<Long> endPosInCMTForDevice = new LinkedList<>();
  public static final String CHUNK_METADATA_TEMP_FILE_SUFFIX = ".cmt";

  /** empty construct function. */
  protected TsFileIOWriter() {}

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this.out = FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), false);
    this.file = file;
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} writer is opened.", file.getName());
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.out = output;
    startFile();
  }

  /** for test only */
  public TsFileIOWriter(TsFileOutput output, boolean test) {
    this.out = output;
  }

  /** for write with memory control */
  public TsFileIOWriter(File file, boolean enableMemoryControl, long maxMetadataSize)
      throws IOException {
    this(file);
    this.enableMemoryControl = enableMemoryControl;
    this.maxMetadataSize = maxMetadataSize;
  }

  /**
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes) throws IOException {
    bytes.writeTo(out.wrapAsStream());
  }

  protected void startFile() throws IOException {
    out.write(MAGIC_STRING_BYTES);
    out.write(VERSION_NUMBER_BYTE);
  }

  public int startChunkGroup(String deviceId) throws IOException {
    this.currentChunkGroupDeviceId = deviceId;
    if (logger.isDebugEnabled()) {
      logger.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
    }
    chunkMetadataList = new ArrayList<>();
    ChunkGroupHeader chunkGroupHeader = new ChunkGroupHeader(currentChunkGroupDeviceId);
    return chunkGroupHeader.serializeTo(out.wrapAsStream());
  }

  /**
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup() throws IOException {
    if (currentChunkGroupDeviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }
    chunkGroupMetadataList.add(
        new ChunkGroupMetadata(currentChunkGroupDeviceId, chunkMetadataList));
    currentChunkGroupDeviceId = null;
    chunkMetadataList = null;
    out.flush();
  }

  /**
   * For TsFileReWriteTool / UpgradeTool. Use this method to determine if needs to start a
   * ChunkGroup.
   *
   * @return isWritingChunkGroup
   */
  public boolean isWritingChunkGroup() {
    return currentChunkGroupDeviceId != null;
  }

  /**
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param measurementId - measurementId of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @param mask - 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(
      String measurementId,
      CompressionType compressionCodecName,
      TSDataType tsDataType,
      TSEncoding encodingType,
      Statistics<? extends Serializable> statistics,
      int dataSize,
      int numOfPages,
      int mask)
      throws IOException {

    currentChunkMetadata =
        new ChunkMetadata(measurementId, tsDataType, out.getPosition(), statistics);
    currentChunkMetadata.setMask((byte) mask);

    ChunkHeader header =
        new ChunkHeader(
            measurementId,
            dataSize,
            tsDataType,
            compressionCodecName,
            encodingType,
            numOfPages,
            mask);
    header.serializeTo(out.wrapAsStream());
  }

  /** Write a whole chunk in another file into this file. Providing fast merge for IoTDB. */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata =
        new ChunkMetadata(
            chunkHeader.getMeasurementID(),
            chunkHeader.getDataType(),
            out.getPosition(),
            chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "end flushing a chunk:{}, totalvalue:{}",
          chunkHeader.getMeasurementID(),
          chunkMetadata.getNumOfPoints());
    }
  }

  /** end chunk and write some log. */
  public void endCurrentChunk() {
    if (enableMemoryControl) {
      this.currentChunkMetadataSize += currentChunkMetadata.calculateRamSize();
    }
    chunkMetadataList.add(currentChunkMetadata);
    currentChunkMetadata = null;
  }

  protected Map<Path, List<IChunkMetadata>> groupChunkMetadataListBySeries() {
    // group ChunkMetadata by series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = new TreeMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      List<ChunkMetadata> chunkMetadatas = chunkGroupMetadata.getChunkMetadataList();
      for (IChunkMetadata chunkMetadata : chunkMetadatas) {
        Path series = new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());
        chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);
      }
    }

    if (chunkMetadataList != null && chunkMetadataList.size() > 0) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Path series = new Path(currentChunkGroupDeviceId, chunkMetadata.getMeasurementUid());
        chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);
      }
    }
    return chunkMetadataListMap;
  }

  /**
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @throws IOException if I/O error occurs
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void endFile() throws IOException {
    readChunkMetadataAndConstructIndexTree();

    long footerIndex = out.getPosition();
    if (logger.isDebugEnabled()) {
      logger.debug("start to flush the footer,file pos:{}", footerIndex);
    }

    // write magic string
    out.write(MAGIC_STRING_BYTES);

    // close file
    out.close();
    if (resourceLogger.isDebugEnabled() && file != null) {
      resourceLogger.debug("{} writer is closed.", file.getName());
    }
    canWrite = false;
  }

  private void readChunkMetadataAndConstructIndexTree() throws IOException {
    if (tempOutput != null) {
      tempOutput.close();
    }
    long metaOffset = out.getPosition();

    // serialize the SEPARATOR of MetaData
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());

    TSMIterator tsmIterator =
        hasChunkMetadataInDisk
            ? TSMIterator.getTSMIteratorInDisk(
                chunkMetadataTempFile, chunkGroupMetadataList, endPosInCMTForDevice)
            : TSMIterator.getTSMIteratorInMemory(chunkGroupMetadataList);
    Map<String, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();
    Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
    String currentDevice = null;
    String prevDevice = null;
    MetadataIndexNode currentIndexNode =
        new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int seriesIdxForCurrDevice = 0;
    BloomFilter filter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), pathCount);

    int indexCount = 0;
    while (tsmIterator.hasNext()) {
      // read in all chunk metadata of one series
      // construct the timeseries metadata for this series
      TimeseriesMetadata timeseriesMetadata = tsmIterator.next();

      indexCount++;
      // build bloom filter
      filter.add(currentSeries);
      // construct the index tree node for the series
      Path currentPath = null;
      if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
        // this series is the time column of the aligned device
        // the full series path will be like "root.sg.d."
        // we remove the last . in the series id here
        currentPath = new Path(currentSeries);
        currentDevice = currentSeries.substring(0, currentSeries.length() - 1);
      } else {
        currentPath = new Path(currentSeries, true);
        currentDevice = currentPath.getDevice();
      }
      if (!currentDevice.equals(prevDevice)) {
        if (prevDevice != null) {
          addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
          deviceMetadataIndexMap.put(
              prevDevice,
              generateRootNode(
                  measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
          currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
        }
        measurementMetadataIndexQueue = new ArrayDeque<>();
        seriesIdxForCurrDevice = 0;
      }

      if (seriesIdxForCurrDevice % config.getMaxDegreeOfIndexNode() == 0) {
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
          currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
        }
        if (timeseriesMetadata.getTSDataType() != TSDataType.VECTOR) {
          currentIndexNode.addEntry(
              new MetadataIndexEntry(currentPath.getMeasurement(), out.getPosition()));
        } else {
          currentIndexNode.addEntry(new MetadataIndexEntry("", out.getPosition()));
        }
      }

      prevDevice = currentDevice;
      seriesIdxForCurrDevice++;
      // serialize the timeseries metadata to file
      timeseriesMetadata.serializeTo(out.wrapAsStream());
    }

    addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
    deviceMetadataIndexMap.put(
        prevDevice,
        generateRootNode(
            measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));

    if (indexCount != pathCount) {
      throw new IOException(
          String.format(
              "Expected path count is %d, index path count is %d", pathCount, indexCount));
    }

    MetadataIndexNode metadataIndex = checkAndBuildLevelIndex(deviceMetadataIndexMap, out);

    TsFileMetadata tsFileMetadata = new TsFileMetadata();
    tsFileMetadata.setMetadataIndex(metadataIndex);
    tsFileMetadata.setMetaOffset(metaOffset);

    int size = tsFileMetadata.serializeTo(out.wrapAsStream());
    size += tsFileMetadata.serializeBloomFilter(out.wrapAsStream(), filter);

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());
  }

  /**
   * Flush TsFileMetadata, including ChunkMetadataList and TimeseriesMetaData
   *
   * @param chunkMetadataListMap chunkMetadata that Path.mask == 0
   * @return MetadataIndexEntry list in TsFileMetadata
   */
  private MetadataIndexNode flushMetadataIndex(Map<Path, List<IChunkMetadata>> chunkMetadataListMap)
      throws IOException {

    // convert ChunkMetadataList to this field
    deviceTimeseriesMetadataMap = new LinkedHashMap<>();
    // create device -> TimeseriesMetaDataList Map
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      // for ordinary path
      TimeseriesMetadata timeseriesMetadata =
          constructOneTimeseriesMetadata(entry.getKey(), entry.getValue());
      deviceTimeseriesMetadataMap
          .computeIfAbsent(entry.getKey().getDevice(), k -> new ArrayList<>())
          .add(timeseriesMetadata);
    }

    // construct TsFileMetadata and return
    return MetadataIndexConstructor.constructMetadataIndex(deviceTimeseriesMetadataMap, out);
  }

  /**
   * Flush one chunkMetadata
   *
   * @param path Path of chunk
   * @param chunkMetadataList List of chunkMetadata about path(previous param)
   * @return the constructed TimeseriesMetadata
   */
  protected TimeseriesMetadata constructOneTimeseriesMetadata(
      Path path, List<IChunkMetadata> chunkMetadataList) throws IOException {
    // create TimeseriesMetaData
    PublicBAOS publicBAOS = new PublicBAOS();
    TSDataType dataType = chunkMetadataList.get(chunkMetadataList.size() - 1).getDataType();
    Statistics seriesStatistics = Statistics.getStatsByType(dataType);

    int chunkMetadataListLength = 0;
    boolean serializeStatistic = (chunkMetadataList.size() > 1);
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.getDataType().equals(dataType)) {
        continue;
      }
      chunkMetadataListLength += chunkMetadata.serializeTo(publicBAOS, serializeStatistic);
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    TimeseriesMetadata timeseriesMetadata =
        new TimeseriesMetadata(
            (byte)
                ((serializeStatistic ? (byte) 1 : (byte) 0) | chunkMetadataList.get(0).getMask()),
            chunkMetadataListLength,
            path.getMeasurement(),
            dataType,
            seriesStatistics,
            publicBAOS);
    return timeseriesMetadata;
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException {
    return out.getPosition();
  }

  // device -> ChunkMetadataList
  public Map<String, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<String, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      deviceChunkMetadataMap
          .computeIfAbsent(chunkGroupMetadata.getDevice(), k -> new ArrayList<>())
          .addAll(chunkGroupMetadata.getChunkMetadataList());
    }
    return deviceChunkMetadataMap;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public void mark() throws IOException {
    markedPosition = getPos();
  }

  public void reset() throws IOException {
    out.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    out.close();
    if (tempOutput != null) {
      this.tempOutput.close();
    }
  }

  void writeSeparatorMaskForTest() throws IOException {
    out.write(new byte[] {MetaMarker.SEPARATOR});
  }

  void writeChunkGroupMarkerForTest() throws IOException {
    out.write(new byte[] {MetaMarker.CHUNK_GROUP_HEADER});
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  /** Remove such ChunkMetadata that its startTime is not in chunkStartTimes */
  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    Iterator<ChunkGroupMetadata> chunkGroupMetaDataIterator = chunkGroupMetadataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetadata chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDevice();
      int chunkNum = chunkGroupMetaData.getChunkMetadataList().size();
      Iterator<ChunkMetadata> chunkMetaDataIterator =
          chunkGroupMetaData.getChunkMetadataList().iterator();
      while (chunkMetaDataIterator.hasNext()) {
        IChunkMetadata chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);

        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid =
            startTimeIdx < pathChunkStartTimes.size()
                && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetaDataIterator.remove();
          chunkNum--;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
      }
    }
  }

  public void writePlanIndices() throws IOException {
    ReadWriteIOUtils.write(MetaMarker.OPERATION_INDEX_RANGE, out.wrapAsStream());
    ReadWriteIOUtils.write(minPlanIndex, out.wrapAsStream());
    ReadWriteIOUtils.write(maxPlanIndex, out.wrapAsStream());
    out.flush();
  }

  public void truncate(long offset) throws IOException {
    out.truncate(offset);
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return out;
  }

  /**
   * this function is for Upgrade Tool and Split Tool.
   *
   * @return DeviceTimeseriesMetadataMap
   */
  public Map<String, List<TimeseriesMetadata>> getDeviceTimeseriesMetadataMap() {
    return deviceTimeseriesMetadataMap;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }

  /**
   * Check if the size of chunk metadata in memory is greater than the given threshold. If so, the
   * chunk metadata will be written to a temp files. <b>Notice! If you are writing a aligned device,
   * you should make sure all data of current writing device has been written before this method is
   * called.</b> For not aligned series, there is no such limitation.
   *
   * @throws IOException
   */
  public void checkMetadataSizeAndMayFlush() throws IOException {
    // This function should be called after all data of an aligned device has been written
    if (enableMemoryControl && currentChunkMetadataSize > maxMetadataSize) {
      try {
        sortAndFlushChunkMetadata();
      } catch (IOException e) {
        logger.error("Meets exception when flushing metadata to temp file for {}", file, e);
        throw e;
      }
    }
  }

  /**
   * Sort the chunk metadata by the lexicographical order and the start time of the chunk, then
   * flush them to a temp file.
   *
   * @throws IOException
   */
  protected void sortAndFlushChunkMetadata() throws IOException {
    // group by series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = groupChunkMetadataListBySeries();
    if (tempOutput == null) {
      tempOutput = new LocalTsFileOutput(new FileOutputStream(chunkMetadataTempFile));
    }
    hasChunkMetadataInDisk = true;
    // the file structure in temp file will be
    // chunkSize | chunkBuffer
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      Path seriesPath = entry.getKey();
      if (!seriesPath.equals(lastSerializePath)) {
        // record the count of path to construct bloom filter later
        pathCount++;
      }
      List<IChunkMetadata> iChunkMetadataList = entry.getValue();
      writeChunkMetadata(iChunkMetadataList, seriesPath, tempOutput);
      lastSerializePath = seriesPath;
    }
    // clear the cache metadata to release the memory
    chunkGroupMetadataList.clear();
    if (chunkMetadataList != null) {
      chunkMetadataList.clear();
    }
  }

  private void writeChunkMetadata(
      List<IChunkMetadata> iChunkMetadataList, Path seriesPath, LocalTsFileOutput output)
      throws IOException {
    if (tempOutput == null) {
      tempOutput =
          new LocalTsFileOutput(
              new FileOutputStream(file.getAbsolutePath() + CHUNK_METADATA_TEMP_FILE_SUFFIX));
    }
    // [DeviceId] measurementId datatype size chunkMetadataBuffer
    if (lastSerializePath == null
        || !seriesPath.getDevice().equals(lastSerializePath.getDevice())) {
      // mark the end position of last device
      endPosInCMTForDevice.add(tempOutput.getPosition());
      // serialize the device
      ReadWriteIOUtils.write(seriesPath.getDevice(), tempOutput.wrapAsStream());
    }
    if (!seriesPath.equals(lastSerializePath) && iChunkMetadataList.size() > 0) {
      // serialize the public info of this measurement
      ReadWriteIOUtils.write(seriesPath.getMeasurement(), tempOutput.wrapAsStream());
      ReadWriteIOUtils.write(iChunkMetadataList.get(0).getDataType(), tempOutput.wrapAsStream());
    }
    PublicBAOS buffer = new PublicBAOS();
    int totalSize = 0;
    for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
      totalSize += chunkMetadata.serializeTo(buffer, true);
    }
    ReadWriteIOUtils.write(totalSize, tempOutput.wrapAsStream());
    tempOutput.write(buffer.getBuf());
  }
}
