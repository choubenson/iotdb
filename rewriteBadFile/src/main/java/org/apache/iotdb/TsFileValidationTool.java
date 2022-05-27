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

package org.apache.iotdb;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * This tool can be used to check the correctness of tsfile and point out errors in specific
 * timeseries or devices. The types of errors include the following:
 *
 * <p>Device overlap between files
 *
 * <p>Timeseries overlap between files
 *
 * <p>Timeseries overlap between chunks
 *
 * <p>Timeseries overlap between pages
 *
 * <p>Timeseries overlap within one page
 */
public class TsFileValidationTool {
  // print detail type of overlap or not
  private static boolean printDetails = false;

  // print to local file or not
  private static boolean printToFile = false;

  private static String outFilePath = "TsFile_validation_view.txt";

  private static PrintWriter pw = null;

  private static final Logger logger = LoggerFactory.getLogger(TsFileValidationTool.class);
  private static final List<File> seqDataDirList = new ArrayList<>();
  private static final List<File> fileList = new ArrayList<>();
  private static int badFileNum = 0;

  /**
   * The form of param is: [path of data dir or tsfile] [-pd = print details or not] [-f = path of
   * outFile]. Eg: xxx/iotdb/data/data1 xxx/xxx.tsfile -pd=true -f=xxx/TsFile_validation_view.txt
   *
   * <p>The first parameter is required, the others are optional.
   */
  public static void main(String[] args) throws IOException {
    if (!checkArgs(args)) {
      System.exit(1);
    }
    if (printToFile) {
      pw = new PrintWriter(new FileWriter(outFilePath));
    }
    printBoth("Start checking seq files ...");

    // check tsfile, which will only check for correctness inside a single tsfile
    for (File f : fileList) {
      findUncorrectFiles(Collections.singletonList(f));
    }

    // check tsfiles in data dir, which will check for correctness inside one single tsfile and
    // between files
    for (File seqDataDir : seqDataDirList) {
      // get sg data dirs
      if (!checkIsDirectory(seqDataDir)) {
        continue;
      }
      File[] sgDirs = seqDataDir.listFiles();
      for (File sgDir : Objects.requireNonNull(sgDirs)) {
        if (!checkIsDirectory(sgDir)) {
          continue;
        }
        printBoth("- Check files in storage group: " + sgDir.getAbsolutePath());
        // get data region dirs
        File[] dataRegionDirs = sgDir.listFiles();
        for (File dataRegionDir : Objects.requireNonNull(dataRegionDirs)) {
          if (!checkIsDirectory(dataRegionDir)) {
            continue;
          }
          // get time partition dirs and sort them
          List<File> timePartitionDirs =
              Arrays.asList(Objects.requireNonNull(dataRegionDir.listFiles()));
          timePartitionDirs.sort(
              (f1, f2) ->
                  Long.compareUnsigned(Long.parseLong(f1.getName()), Long.parseLong(f2.getName())));
          for (File timePartitionDir : timePartitionDirs) {
            if (!checkIsDirectory(timePartitionDir)) {
              continue;
            }
            // get all seq files under the time partition dir
            List<File> tsFiles =
                Arrays.asList(
                    Objects.requireNonNull(
                        timePartitionDir.listFiles(
                            file -> file.getName().endsWith(TSFILE_SUFFIX))));
            // sort the seq files with timestamp
            tsFiles.sort(
                (f1, f2) ->
                    Long.compareUnsigned(
                        Long.parseLong(f1.getName().split("-")[0]),
                        Long.parseLong(f2.getName().split("-")[0])));
            findUncorrectFiles(tsFiles);
          }
        }
      }
    }
    printBoth("Finish checking successfully, totally find " + badFileNum + " bad files.");
    if (printToFile) {
      pw.close();
    }
  }

  private static void findUncorrectFiles(List<File> tsFiles) {
    // measurementID -> [lastTime, endTimeInLastFile]
    Map<String, long[]> measurementLastTime = new HashMap<>();
    // deviceID -> endTime, the endTime of device in the last seq file
    Map<String, Long> deviceEndTime = new HashMap<>();

    for (File tsFile : tsFiles) {
      try {
        TsFileResource resource = new TsFileResource(tsFile);
        if (!new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()) {
          // resource file does not exist, tsfile may not be flushed yet
          logger.warn(
              "{} does not exist ,skip it.",
              tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
          continue;
        } else {
          resource.deserialize();
        }
        boolean isBadFile = false;
        try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
          // deviceID -> has checked overlap or not
          Map<String, Boolean> hasCheckedDeviceOverlap = new HashMap<>();
          reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
          byte marker;
          String deviceID = "";
          Map<String, boolean[]> hasMeasurementPrintedDetails = new HashMap<>();
          // measurementId -> lastChunkEndTime in current file
          Map<String, Long> lashChunkEndTime = new HashMap<>();

          // start reading data points in sequence
          while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
            switch (marker) {
              case MetaMarker.CHUNK_HEADER:
              case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
                ChunkHeader header = reader.readChunkHeader(marker);
                if (header.getDataSize() == 0) {
                  // empty value chunk
                  break;
                }
                long currentChunkEndTime = Long.MIN_VALUE;
                String measurementID = deviceID + "." + header.getMeasurementID();
                hasMeasurementPrintedDetails.computeIfAbsent(measurementID, k -> new boolean[4]);
                measurementLastTime.computeIfAbsent(
                    measurementID,
                    k -> {
                      long[] arr = new long[2];
                      Arrays.fill(arr, Long.MIN_VALUE);
                      return arr;
                    });
                Decoder defaultTimeDecoder =
                    Decoder.getDecoderByType(
                        TSEncoding.valueOf(
                            TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                        TSDataType.INT64);
                Decoder valueDecoder =
                    Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
                int dataSize = header.getDataSize();
                long lastPageEndTime = Long.MIN_VALUE;
                while (dataSize > 0) {
                  valueDecoder.reset();
                  PageHeader pageHeader =
                      reader.readPageHeader(
                          header.getDataType(),
                          (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
                  ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                  long currentPageEndTime = Long.MIN_VALUE;
                  // NonAligned Chunk
                  PageReader pageReader =
                      new PageReader(
                          pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                  BatchData batchData = pageReader.getAllSatisfiedPageData();
                  while (batchData.hasCurrent()) {
                    long timestamp = batchData.currentTime();
                    if (timestamp <= measurementLastTime.get(measurementID)[0]) {
                      // find bad file
                      if (!isBadFile) {
                        printBoth("-- Find the bad file " + tsFile.getAbsolutePath());
                        isBadFile = true;
                        badFileNum++;
                      }
                      if (printDetails) {
                        if (timestamp <= measurementLastTime.get(measurementID)[1]) {
                          if (!hasMeasurementPrintedDetails.get(measurementID)[0]) {
                            printBoth(
                                "-------- Timeseries " + measurementID + " overlap between files");
                            hasMeasurementPrintedDetails.get(measurementID)[0] = true;
                          }
                        } else if (timestamp
                            <= lashChunkEndTime.getOrDefault(measurementID, Long.MIN_VALUE)) {
                          if (!hasMeasurementPrintedDetails.get(measurementID)[1]) {
                            printBoth(
                                "-------- Timeseries " + measurementID + " overlap between chunks");
                            hasMeasurementPrintedDetails.get(measurementID)[1] = true;
                          }
                        } else if (timestamp <= lastPageEndTime) {
                          if (!hasMeasurementPrintedDetails.get(measurementID)[2]) {
                            printBoth(
                                "-------- Timeseries " + measurementID + " overlap between pages");
                            hasMeasurementPrintedDetails.get(measurementID)[2] = true;
                          }
                        } else {
                          if (!hasMeasurementPrintedDetails.get(measurementID)[3]) {
                            printBoth(
                                "-------- Timeseries "
                                    + measurementID
                                    + " overlap within one page");
                            hasMeasurementPrintedDetails.get(measurementID)[3] = true;
                          }
                        }
                      }
                    } else {
                      measurementLastTime.get(measurementID)[0] = timestamp;
                      currentPageEndTime = timestamp;
                      currentChunkEndTime = timestamp;
                    }
                    batchData.next();
                  }
                  dataSize -= pageHeader.getSerializedPageSize();
                  lastPageEndTime = Math.max(lastPageEndTime, currentPageEndTime);
                }
                lashChunkEndTime.put(
                    measurementID,
                    Math.max(
                        lashChunkEndTime.getOrDefault(measurementID, Long.MIN_VALUE),
                        currentChunkEndTime));
                break;
              case MetaMarker.CHUNK_GROUP_HEADER:
                if (!deviceID.equals("")) {
                  // record the end time of last device in current file
                  if (resource.getEndTime(deviceID)
                      > deviceEndTime.getOrDefault(deviceID, Long.MIN_VALUE)) {
                    deviceEndTime.put(deviceID, resource.getEndTime(deviceID));
                  }
                }
                ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
                deviceID = chunkGroupHeader.getDeviceID();
                if (!hasCheckedDeviceOverlap.getOrDefault(deviceID, false)
                    && resource.getStartTime(deviceID)
                        <= deviceEndTime.getOrDefault(deviceID, Long.MIN_VALUE)) {
                  // find bad file
                  if (!isBadFile) {
                    printBoth("-- Find the bad file " + tsFile.getAbsolutePath());
                    isBadFile = true;
                    badFileNum++;
                  }
                  if (printDetails) {
                    printBoth("---- Device " + deviceID + " overlap between files");
                  }
                }
                hasCheckedDeviceOverlap.put(deviceID, true);
                break;
              case MetaMarker.OPERATION_INDEX_RANGE:
                reader.readPlanIndex();
                break;
              default:
                MetaMarker.handleUnexpectedMarker(marker);
            }
          }

          // record the end time of each timeseries in current file
          for (Map.Entry<String, Long> entry : lashChunkEndTime.entrySet()) {
            Long endTime = Math.max(measurementLastTime.get(entry.getKey())[1], entry.getValue());
            measurementLastTime.get(entry.getKey())[1] = endTime;
          }
        }
      } catch (Throwable e) {
        logger.error("Meet errors in reading file {} , skip it.", tsFile.getAbsolutePath(), e);
        printBoth("-- Meet errors in reading file " + tsFile.getAbsolutePath());
      }
    }
  }

  public static boolean checkArgs(String[] args) {
    if (args.length < 1) {
      System.out.println(
          "Please input correct param, which is [path of data dir] [-pd = print details or not] [-f = path of outFile]. Eg: xxx/iotdb/data/data -pd=true -f=xxx/TsFile_validation_view.txt");
      return false;
    } else {
      for (String arg : args) {
        if (arg.startsWith("-pd")) {
          printDetails = Boolean.parseBoolean(arg.split("=")[1]);
        } else if (arg.startsWith("-f")) {
          printToFile = true;
          outFilePath = arg.split("=")[1];
        } else {
          File f = new File(arg);
          if (f.isDirectory()
              && Objects.requireNonNull(
                          f.list(
                              (dir, name) ->
                                  (name.equals("sequence") || name.equals("unsequence"))))
                      .length
                  == 2) {
            File seqDataDir = new File(f, "sequence");
            seqDataDirList.add(seqDataDir);
          } else if (arg.endsWith(TSFILE_SUFFIX) && f.isFile()) {
            fileList.add(f);
          } else {
            System.out.println(arg + " is not a correct data directory or tsfile of IOTDB.");
            return false;
          }
        }
      }
      if (seqDataDirList.size() == 0 && fileList.size() == 0) {
        System.out.println(
            "Please input correct param, which is [path of data dir] [-pd = print details or not] [-f = path of outFile]. Eg: xxx/iotdb/data/data -pd=true -f=xxx/TsFile_validation_view.txt");
        return false;
      }
      return true;
    }
  }

  private static boolean checkIsDirectory(File dir) {
    boolean res = true;
    if (!dir.isDirectory()) {
      logger.error("{} is not a directory or does not exist, skip it.", dir.getAbsolutePath());
      res = false;
    }
    return res;
  }

  private static void printBoth(String msg) {
    System.out.println(msg);
    if (printToFile) {
      pw.println(msg);
    }
  }
}
