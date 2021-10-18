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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TsFileSequenceRead {

  @SuppressWarnings({
    "squid:S3776",
    "squid:S106"
  }) // Suppress high Cognitive Complexity and Standard outputs warning
  public static void main(String[] args) throws IOException {
    String filename = "test.tsfile";
    if (args.length >= 1) {
      filename = args[0];
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      System.out.println(
          "file length: " + FSFactoryProducer.getFSFactory().getFile(filename).length());
      System.out.println("file magic head: " + reader.readHeadMagic());
      System.out.println("file magic tail: " + reader.readTailMagic());
      System.out.println("Level 1 metadata position: " + reader.getFileMetadataPos());//获取该TsFile的IndexOfTimeseriesIndex索引的开始处所在的偏移位置
      System.out.println("Level 1 metadata size: " + reader.getFileMetadataSize());//该TsFile的IndexOfTimeseriesIndex索引内容的大小
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);//将顺序读取器的指针移到MagicString 和 version后面
      System.out.println("[Chunk Group]");
      System.out.println("position: " + reader.position());
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
            System.out.println("\t[Chunk]");
            System.out.println("\tchunk type: " + marker);
            System.out.println("\tposition: " + reader.position());
            ChunkHeader header = reader.readChunkHeader(marker);
            System.out.println("\tMeasurement: " + header.getMeasurementID());
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();  //获取该Chunk的ChunkData数据大小(即PageHeader+PageData)
            while (dataSize > 0) {
              valueDecoder.reset();
              System.out.println("\t\t[Page]\n \t\tPage head position: " + reader.position());
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              System.out.println("\t\tPage data position: " + reader.position());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType()); // 根据pageHeader和压缩类型，把该page的二进制流数据进行解压后的二进制流数据存入ByteBuffer缓存并返回
              System.out.println(
                  "\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
              PageReader reader1 =
                  new PageReader(     //创建该page的pageReader
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData(); // 读取该page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里并返回
              if (header.getChunkType() == MetaMarker.CHUNK_HEADER) { //1代表该Page有1个或多个page,则可以直接从pageHeader的statistics获取数据点的数量；否则pageHeader是没有统计量的
                System.out.println("\t\tpoints in the page: " + pageHeader.getNumOfValues());
              } else {
                System.out.println("\t\tpoints in the page: " + batchData.length());
              }
              while (batchData.hasCurrent()) {
                System.out.println(
                    "\t\t\ttime, value: "
                        + batchData.currentTime()
                        + ", "
                        + batchData.currentValue());
                batchData.next();
              }
              dataSize -= pageHeader.getSerializedPageSize();//ChunkData剩余数据大小-此page（包括pageHeader+pageData）的全部大小
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            System.out.println("Chunk Group Header position: " + reader.position());
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            System.out.println("device: " + chunkGroupHeader.getDeviceID());
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            System.out.println("minPlanIndex: " + reader.getMinPlanIndex());
            System.out.println("maxPlanIndex: " + reader.getMaxPlanIndex());
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      System.out.println("[Metadata]");
      for (String device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
        System.out.printf(
            "\t[Device]Device %s, Number of Measurements %d%n", device, seriesMetaData.size());
        for (Map.Entry<String, List<ChunkMetadata>> serie : seriesMetaData.entrySet()) {
          System.out.println("\t\tMeasurement:" + serie.getKey());
          for (ChunkMetadata chunkMetadata : serie.getValue()) {
            System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
          }
        }
      }
    }
  }
}
