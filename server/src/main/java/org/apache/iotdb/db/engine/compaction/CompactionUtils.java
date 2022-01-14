/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This tool can be used to perform inner space or cross space compaction of aligned and non aligned
 * timeseries . Currently, we use {@link
 * org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils} to speed up if it is
 * an inner space compaction.
 */
public class CompactionUtils {
  private static final Logger logger = LoggerFactory.getLogger("CompactionUtils");

  public static void compact(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources,
      String fullStorageGroupName)
      throws IOException, MetadataException, StorageEngineException {
    long queryId = QueryResourceManager.getInstance().assignCompactionQueryId();
    QueryContext queryContext = new QueryContext(queryId);
    QueryDataSource queryDataSource = new QueryDataSource(seqFileResources, unseqFileResources);
    QueryResourceManager.getInstance()
        .getQueryFileManager()
        .addUsedFilesForQuery(queryId, queryDataSource);

    List<TsFileResource> allResources = new ArrayList<>();
    allResources.addAll(seqFileResources);
    allResources.addAll(unseqFileResources);
    try (AbstractCompactionWriter compactionWriter =
            getCompactionWriter(seqFileResources, unseqFileResources, targetFileResources);
        MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(allResources)) {
      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        QueryUtils.fillOrderIndexes(queryDataSource, device, true);

        if (isAligned) {
          compactAlignedSeries(
              device, deviceIterator, compactionWriter, queryContext, queryDataSource);
        } else {
          compactNonAlignedSeries(
              device, deviceIterator, compactionWriter, queryContext, queryDataSource);
        }
      }

      compactionWriter.endFile();
      updateDeviceStartTimeAndEndTime(targetFileResources, compactionWriter);
      updatePlanIndexes(targetFileResources, seqFileResources, unseqFileResources);
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  private static void compactAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws IOException, MetadataException {
    MultiTsFileDeviceIterator.AlignedMeasurmentIterator alignedMeasurmentIterator =
        deviceIterator.iterateAlignedSeries(device);
    List<String> allMeasurments = alignedMeasurmentIterator.getAllMeasurements();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurement : allMeasurments) {
      // TODO: use IDTable
      measurementSchemas.add(
          IoTDB.metaManager.getSeriesSchema(new PartialPath(device, measurement)));
    }

    IBatchReader dataBatchReader =
        constructReader(
            device,
            allMeasurments,
            measurementSchemas,
            new HashSet<>(allMeasurments),
            queryContext,
            queryDataSource,
            true);

    if (dataBatchReader.hasNextBatch()) {
      // chunkgroup is serialized only when at least one timeseries under this device has data
      compactionWriter.startChunkGroup(device, true);
      compactionWriter.startMeasurement(measurementSchemas);
      writeWithReader(compactionWriter, dataBatchReader);
      compactionWriter.endMeasurement();
      compactionWriter.endChunkGroup();
    }
  }

  private static void compactNonAlignedSeries(
      String device,
      MultiTsFileDeviceIterator deviceIterator,
      AbstractCompactionWriter compactionWriter,
      QueryContext queryContext,
      QueryDataSource queryDataSource)
      throws MetadataException, IOException {
    boolean hasStartChunkGroup = false;
    MultiTsFileDeviceIterator.MeasurementIterator measurementIterator =
        deviceIterator.iterateNotAlignedSeries(device, false);
    Set<String> allMeasurements = measurementIterator.getAllMeasurements();
    for (String measurement : allMeasurements) {
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(
          IoTDB.metaManager.getSeriesSchema(new PartialPath(device, measurement)));

      IBatchReader dataBatchReader =
          constructReader(
              device,
              Collections.singletonList(measurement),
              measurementSchemas,
              new HashSet<>(allMeasurements),
              queryContext,
              queryDataSource,
              false);

      if (dataBatchReader.hasNextBatch()) {
        if (!hasStartChunkGroup) {
          // chunkgroup is serialized only when at least one timeseries under this device has
          // data
          compactionWriter.startChunkGroup(device, false);
          hasStartChunkGroup = true;
        }
        compactionWriter.startMeasurement(measurementSchemas);
        writeWithReader(compactionWriter, dataBatchReader);
        compactionWriter.endMeasurement();
      }
    }

    if (hasStartChunkGroup) {
      compactionWriter.endChunkGroup();
    }
  }

  private static void writeWithReader(AbstractCompactionWriter writer, IBatchReader reader)
      throws IOException {
    while (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        writer.write(batchData.currentTime(), batchData.currentValue());
        batchData.next();
      }
    }
  }

  /**
   * @param measurementIds if device is aligned, then measurementIds contain all measurements. If
   *     device is not aligned, then measurementIds only contain one measurement.
   */
  private static IBatchReader constructReader(
      String deviceId,
      List<String> measurementIds,
      List<IMeasurementSchema> measurementSchemas,
      Set<String> allSensors,
      QueryContext queryContext,
      QueryDataSource queryDataSource,
      boolean isAlign)
      throws IllegalPathException {
    PartialPath seriesPath;
    TSDataType tsDataType;
    if (isAlign) {
      seriesPath = new AlignedPath(deviceId, measurementIds, measurementSchemas);
      tsDataType = TSDataType.VECTOR;
    } else {
      seriesPath = new MeasurementPath(deviceId, measurementIds.get(0), measurementSchemas.get(0));
      tsDataType = measurementSchemas.get(0).getType();
    }
    return new SeriesRawDataBatchReader(
        seriesPath, allSensors, tsDataType, queryContext, queryDataSource, null, null, null, true);
  }

  private static AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      return new CrossSpaceCompactionWriter(targetFileResources, seqFileResources);
    } else {
      // inner space
      return new InnerSpaceCompactionWriter(targetFileResources.get(0));
    }
  }

  private static void updateDeviceStartTimeAndEndTime(
      List<TsFileResource> targetResources, AbstractCompactionWriter compactionWriter) {
    List<TsFileIOWriter> fileIOWriterList = new ArrayList<>();
    if (compactionWriter instanceof InnerSpaceCompactionWriter) {
      fileIOWriterList.add(((InnerSpaceCompactionWriter) compactionWriter).getFileWriter());
    } else {
      fileIOWriterList.addAll(((CrossSpaceCompactionWriter) compactionWriter).getFileWriters());
    }
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource targetResource = targetResources.get(i);
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
          fileIOWriterList.get(i).getDeviceTimeseriesMetadataMap();
      for (Map.Entry<String, List<TimeseriesMetadata>> entry :
          deviceTimeseriesMetadataMap.entrySet()) {
        for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
          targetResource.updateStartTime(
              entry.getKey(), timeseriesMetadata.getStatistics().getStartTime());
          targetResource.updateEndTime(
              entry.getKey(), timeseriesMetadata.getStatistics().getEndTime());
        }
      }
    }
  }

  private static void updatePlanIndexes(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources) {
    // as the new file contains data of other files, track their plan indexes in the new file
    // so that we will be able to compare data across different IoTDBs that share the same index
    // generation policy
    // however, since the data of unseq files are mixed together, we won't be able to know
    // which files are exactly contained in the new file, so we have to record all unseq files
    // in the new file
    for (TsFileResource targetResource : targetResources) {
      for (TsFileResource unseqResource : unseqResources) {
        targetResource.updatePlanIndexes(unseqResource);
      }
      for (TsFileResource seqResource : seqResources) {
        targetResource.updatePlanIndexes(seqResource);
      }
    }
  }

  /**
   * Update the targetResource. Move tmp target file to target file and serialize
   * xxx.tsfile.resource.
   */
  public static void moveToTargetFile(
      List<TsFileResource> targetResources, boolean isInnerSpace, String fullStorageGroupName)
      throws IOException, WriteProcessException {
    String fileSuffix;
    if (isInnerSpace) {
      fileSuffix = IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX;
    } else {
      fileSuffix = IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
    }
    checkAndUpdateTargetFileResources(targetResources, fileSuffix);
    for (TsFileResource targetResource : targetResources) {
      moveOneTargetFile(targetResource, fileSuffix, fullStorageGroupName);
    }
  }

  /**
   * Check whether all target files is end with correct file suffix and remove target resource whose
   * tsfile is deleted from list.
   */
  private static void checkAndUpdateTargetFileResources(
      List<TsFileResource> targetResources, String tmpFileSuffix) throws WriteProcessException {
    for (int i = 0; i < targetResources.size(); i++) {
      TsFileResource targetResource = targetResources.get(i);
      if (!targetResource.getTsFilePath().endsWith(tmpFileSuffix)) {
        throw new WriteProcessException(
            "Tmp target tsfile "
                + targetResource.getTsFilePath()
                + " should be end with "
                + tmpFileSuffix);
      }
      if (!targetResource.getTsFile().exists()) {
        targetResources.remove(i--);
      }
    }
  }

  private static void moveOneTargetFile(
      TsFileResource targetResource, String tmpFileSuffix, String fullStorageGroupName)
      throws IOException {
    // move to target file and delete old tmp target file
    if (!targetResource.getTsFile().exists()) {
      logger.info(
          "{} [Compaction] Tmp target tsfile {} may be deleted after compaction.",
          fullStorageGroupName,
          targetResource.getTsFilePath());
      return;
    }
    File newFile =
        new File(
            targetResource.getTsFilePath().replace(tmpFileSuffix, TsFileConstant.TSFILE_SUFFIX));
    FSFactoryProducer.getFSFactory().moveFile(targetResource.getTsFile(), newFile);

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.close();
  }
}
