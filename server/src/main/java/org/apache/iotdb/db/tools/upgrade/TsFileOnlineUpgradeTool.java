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
package org.apache.iotdb.db.tools.upgrade;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.v2.read.TsFileSequenceReaderForV2;
import org.apache.iotdb.tsfile.v2.read.reader.page.PageReaderV2;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TsFileOnlineUpgradeTool extends TsFileRewriteTool {  //TsFile在线升级工具类，该类有一个待升级TsFile文件的TsFileResource

  private static final Logger logger = LoggerFactory.getLogger(TsFileOnlineUpgradeTool.class);

  /**
   * Create a file reader of the given file. This reader will read the old file and rewrite it to a
   * new format(v3) file
   *
   * @param resourceToBeUpgraded the old tsfile resource which need to be upgrade
   * @throws IOException If some I/O error occurs
   */
  public TsFileOnlineUpgradeTool(TsFileResource resourceToBeUpgraded) throws IOException {
    super(resourceToBeUpgraded, true);
  }

  /**
   * upgrade a single TsFile.
   *
   * @param resourceToBeUpgraded the old file's resource which need to be upgrade.
   * @param upgradedResources new version tsFiles' resources
   */
  public static void upgradeOneTsFile(  //该方法用于升级一个TsFile文件，第一个参数是旧的待升级文件的TsFileResource对象，第二个参数是升级完毕新的TsFile文件TsFileResource列表。由于升级时可能会对旧的TsFile根据时间分区分成多个TsFile，因此第二个参数是列表
      TsFileResource resourceToBeUpgraded, List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {
    try (TsFileOnlineUpgradeTool updater = new TsFileOnlineUpgradeTool(resourceToBeUpgraded)) {
      updater.upgradeFile(upgradedResources); //升级该类的对应的旧TsFile文件，并把升级后的不同时间分区的TsFile文件的TsFileResource放进列表里
    }
  }

  /** upgrade file resource */
  @SuppressWarnings({"squid:S3776", "deprecation"}) // Suppress high Cognitive Complexity warning
  private void upgradeFile(List<TsFileResource> upgradedResources)    //升级该类的对应的旧TsFile文件，并把升级后的不同时间分区的TsFile文件的TsFileResource放进列表里
      throws IOException, WriteProcessException { //具体做法是：（1）先将旧TsFile文件的数据区内容经计算每个数据点的时间戳所在时间分区写到对应时间分区里的新TsFile对应的TsFileIOWriter的TsFileOutput对象的输出流BufferedOutputStream的缓存数组里，然后再根据数据区的内容往该缓存数组里写入剩余的索引区内容和其他小内容，然后将该缓存数组flush 写到本地对应的新的TsFile文件里。（2）对每个时间分区新建的TsFile创建对应的TsFileResource对象，并把该TsFileResource对象的内容写到本地对应的.resource文件里（3）对每个时间分区的新TsFile文件写其对应的.mods修改文件

    // check if the old TsFile has correct header
    if (!fileCheck()) {
      return;
    }

    int headerLength =        //TsFile文件开头长度（MagicString的字节数+版本号的字节数）
        TSFileConfig.MAGIC_STRING.getBytes().length       //MagicString的字节数，为6个字节
            + TSFileConfig.VERSION_NUMBER_V2.getBytes().length;   //TsFile版本号的字节数，为6个字节
    reader.position(headerLength);    //设定读取指针在文件的偏移量，即将读指针指向第一个Chunk的位置
    byte marker;    //一个marker占用一个字节
    long firstChunkPositionInChunkGroup = headerLength; //某ChunkGroup的第一个Chunk位置
    boolean firstChunkInChunkGroup = true;      //该Chunk是否是所属ChunkGroup里的第一个Chunk
    String deviceId = null;
    boolean skipReadingChunk = true;  //是否能跳过Chunk的读取。用于先读取每个ChunkGroup的ChunkGroupFooter（ChunkGroupHeader），然后回来依次读取该ChunkGroup的每个Chunk
    try {
      //循环读取旧的TsFile的数据区的内容，并把数据区里的数据（不是索引区）写到新的TsFile文件里
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {//从该TSFIle文件读取一个字节的marker，若不为2（即MetaMarker，IndexMarker）（即只读取数据区的内容，若读到了索引区则停止），则
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:   //如果是ChunkHeader的marker，且为1，则若还没读取该Chunk所属ChunkGroup的ChunkGroupFooter，则先将指针后移到该ChunkGroup的ChunkGroupFooter开头处，然后读取对应的ChunkGroupFooter，然后再回到该ChunkGroup的开头（第一个Chunk位置）处遍历每个Chunk，读取每个Chunk的所有Page，以重写每个Chunk。
            if (skipReadingChunk || deviceId == null) { //若能跳过Chunk的读取或者设备为空，则
              ChunkHeader header = ((TsFileSequenceReaderForV2) reader).readChunkHeader();  //read data from current position of the input, and deserialize it to a CHUNK_HEADER
              int dataSize = header.getDataSize();  //获取该Chunk的数据部分字节大小（即所有page的大小总和）
              while (dataSize > 0) {  //下面依次读取该Chunk的所有page，将读指针移到该Chunk最后一个page的结尾
                // a new Page
                PageHeader pageHeader =       //读取该Chunk的某page的pageHeader,//read data from current position of the input, and deserialize it to a PageHeader
                    ((TsFileSequenceReaderForV2) reader).readPageHeader(header.getDataType());
                ((TsFileSequenceReaderForV2) reader).readCompressedPage(pageHeader);//将读指针向后移动到该Page数据pagedata的结尾处。（从当前TsFile文件的读指针位置开始，读取经压缩后page大小的字节的数据到ByteBuffer缓存里。）
                dataSize -=   //该Chunk的datasize-page的大小（header+data）
                    (Integer.BYTES * 2 // the bytes size of uncompressedSize and compressedSize
                        // count, startTime, endTime bytes size in old statistics
                        + 24
                        // statistics bytes size
                        // new boolean StatsSize is 8 bytes larger than old one
                        + (pageHeader.getStatistics().getStatsSize()
                            - (header.getDataType() == TSDataType.BOOLEAN ? 8 : 0))
                        // page data bytes
                        + pageHeader.getCompressedSize());  //page的经压缩后的数据大小
              }
            } else {  //若不能跳过Chunk的读取，并且设备不为空
              ChunkHeader header = ((TsFileSequenceReaderForV2) reader).readChunkHeader();//从当前读指针位置读取Chunk Header
              MeasurementSchema measurementSchema = //创建传感器配置类对象
                  new MeasurementSchema(
                      header.getMeasurementID(),
                      header.getDataType(),
                      header.getEncodingType(),
                      header.getCompressionType());
              TSDataType dataType = header.getDataType();
              TSEncoding encoding = header.getEncodingType();
              //下面用于的一些list用于存放该Chunk所有page的相关信息
              List<PageHeader> pageHeadersInChunk = new ArrayList<>();  //用于存放每个page的pageHeader
              List<ByteBuffer> dataInChunk = new ArrayList<>();   //用于存放每个page的pageData,每个page的pageData存入缓存ByteBuffer中
              List<Boolean> needToDecodeInfo = new ArrayList<>();//用于存放每个page里的数据是否需要解码
              int dataSize = header.getDataSize();  //该Chunk的数据部分字节大小（即所有page的大小总和）
              while (dataSize > 0) {    //依次读取该Chunk的所有page，将读指针移到该Chunk最后一个page的结尾
                // a new Page
                PageHeader pageHeader =
                    ((TsFileSequenceReaderForV2) reader).readPageHeader(dataType);  //从当前读指针位置读取PageHeader
                boolean needToDecode = checkIfNeedToDecode(dataType, encoding, pageHeader); //判断该page的数据是否需要解码（即把数据二进制流反序列化deserialize成数据点对象），若是以下几种数据类型和编码技术之一，或者该page的开始时间戳和结束时间戳不在一个时间分区里（说明该page里的所有数据不在一个时间分区里），则需要把该二进制流的pageData解码反序列化成数据点对象，用于后续对数据点对象进行数据的处理后再编码、压缩存入对应时间分区的TSFile文件里
                needToDecodeInfo.add(needToDecode);
                ByteBuffer pageData =
                    !needToDecode
                        ? ((TsFileSequenceReaderForV2) reader).readCompressedPage(pageHeader)//若该page数据不需要解码，则从当前位置读取经压缩后page大小的字节的数据到ByteBuffer缓存里。读指针向后移动
                        : reader.readPage(pageHeader, header.getCompressionType()); //若需要解码，则需要先把该page的二进制流数据进行解压uncompress，然后才能解码。即则根据pageHeader和压缩类型，把该page的数据进行解压后存入ByteBuffer缓存并返回
                pageHeadersInChunk.add(pageHeader); //加入该page的pageHeader
                dataInChunk.add(pageData);  //加入该page的数据
                dataSize -=   //该Chunk的剩余数据大小-该page的总大小（header+data）
                    (Integer.BYTES * 2 // the bytes size of uncompressedSize and compressedSize
                        // count, startTime, endTime bytes size in old statistics
                        + 24
                        // statistics bytes size
                        // new boolean StatsSize is 8 bytes larger than old one
                        + (pageHeader.getStatistics().getStatsSize()
                            - (dataType == TSDataType.BOOLEAN ? 8 : 0))
                        // page data bytes
                        + pageHeader.getCompressedSize());
              }
              reWriteChunk(//重写该Chunk的数据到对应时间分区的TsFile文件里：1） 先遍历该Chunk的每个page的pageData，（1）若该page需要解码，则将该page的每个数据点写入对应时间分区TsFile的Chunk的ChunkWriterImpl的输出流pageBuffer缓存里 (2)若该page不需要解码，则直接根据给定的pageHeader和pageData，创建该page数据所在时间分区对应的ChunkWriterImpl对象，并用该ChunkWriterImpl对象把该页page数据（pageData+pageHeader）写入该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里 2）遍历每个时间分区TsFile对应的待写入传感器Chunk的ChunkWriterImpl对象,使用该Chunk的ChunkWriterImpl对象往对应TsFile的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的内容（ChunkHeader和page内容（pageHeader+pageData）），若是第一个Chunk则要先往对应的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入ChunkGroupHeader
                  deviceId,     //设备ID
                  firstChunkInChunkGroup,//该Chunk是否是所属ChunkGroup里的第一个Chunk
                  measurementSchema,  //传感器配置类对象
                  pageHeadersInChunk,   //该Chunk的所有page的pageHeader列表
                  dataInChunk,          //该Chunk的所有page的pageData列表
                  needToDecodeInfo);    //该Chunk的所有page里的数据是否需要解码列表
              if (firstChunkInChunkGroup) {
                firstChunkInChunkGroup = false;
              }
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER: //ChunkGroup的marker
            // this is the footer of a ChunkGroup in TsFileV2.
            if (skipReadingChunk) {   //初始为真，读取ChunkGroupHeader，并重置读指针于V2版本TsFile文件中的第一个Chunk位置
              skipReadingChunk = false; //此时设置不能跳过Chunk的读取
              ChunkGroupHeader chunkGroupFooter =   //读取ChunkGroupFooter
                  ((TsFileSequenceReaderForV2) reader).readChunkGroupFooter();//read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER
              deviceId = chunkGroupFooter.getDeviceID();  //获取该ChunkGroup的设备ID
              reader.position(firstChunkPositionInChunkGroup);  //将读指针重置于V2版本TsFile文件中的第一个Chunk位置
            } else {  //读取完该ChunkGroup里的所有Chunk数据后，会到这里，此时要做一些重置工作，用于下次开始读取下一个ChunkGroup
              endChunkGroup();//结束每个时间分区的TsFileIOWriter类对象对应ChunkGroup
              skipReadingChunk = true;  //重置skipReadingChunk，用于读取下一个ChunkGroup时先读取其ChunkGroupFooter
              ((TsFileSequenceReaderForV2) reader).readChunkGroupFooter();  //将读指针往后移到该ChunkGroup的ChunkGroupFooter结尾处
              deviceId = null;//重置设备ID
              firstChunkPositionInChunkGroup = reader.position(); //获取当前读指针位置，将其赋给下一个ChunkGroup的第一个Chunk的位置
              firstChunkInChunkGroup = true;  //结束该ChunkGroup的全部读取，设置下一个Chunk是否是所属ChunkGroup里的第一个Chunk
            }
            break;
          case MetaMarker.VERSION:  //version marker  //每遇到该marker，就获取一条操作记录，当该操作记录的version大于等于该ChunkGroup的version时，就往升级生成的每个新TsFile的mods文件里记录该删除操作，且生效位置为当前TsFile文件长度
            long version = ((TsFileSequenceReaderForV2) reader).readVersion();
            // convert old Modification to new
            if (oldModification != null && modsIterator.hasNext()) { // //如果旧TsFile文件的操作对象列表不为空（说明旧TsFile存在删除操作）
              if (currentMod == null) {
                currentMod = (Deletion) modsIterator.next();  //获取一条修改操作对象，记录为当前操作
              }
              if (currentMod.getFileOffset() <= version) { //0.11版的mods文件是记录version的，若mods文件里该删除操作的version小于等于TsFile中ChunkGroupFooter后的version，说明该删除操作对当前ChunkGroup里的数据不起作用，则 //Todo:问题：为什么？//若该删除操作的生效位置小于等于版本号，则往每个新的TsFile对应的本地.mods文件写入该删除操作记录，而生效位置是对应新TsFile的长度（即该删除操作是对后续写入的数据生效）  //Todo:问题？
                for (Entry<TsFileIOWriter, ModificationFile> entry :
                    fileModificationMap.entrySet()) { //遍历fileModificationMap，它存放了每个新TsFileIOWriter类对象都有自己对应新TsFile文件的新ModificationFile(.mods)修改文件类对象
                  TsFileIOWriter tsFileIOWriter = entry.getKey(); //获取该TsFile的TsFileIOWriter
                  ModificationFile newMods = entry.getValue();  //获取该TsFile的ModificationFile修改文件类对象
                  newMods.write(    //向新的.mods删除文件里写入删除操作
                      new Deletion(
                          currentMod.getPath(), //获取当前修改操作的时间序列路径（即传感器路径）对象
                          tsFileIOWriter.getFile().length(),  //生效位置为当前TsFile文件内容的结尾，即对后续写入的数据生效
                          currentMod.getStartTime(),
                          currentMod.getEndTime()));
                }
                currentMod = null;
              }
            }
            // write plan indices for ending memtable
            for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {//遍历每个时间分区的TSFile文件的TsFileIOWriter类对象
              tsFileIOWriter.writePlanIndices();//往该TsFile对应的TsFileIOWriter类对象的TsFileOutput对象的输出流BufferedOutputStream的缓存数组里写入planIndex，并将该TsFileIOWriter缓存内容flush到本地TsFile文件
            }
            firstChunkPositionInChunkGroup = reader.position(); //获取下一个ChunkGroup的第一个Chunk的位置
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unrecognized marker detected, " + "this file may be corrupted");
        }
      }
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {//遍历每个时间分区的TSFile文件的TsFileIOWriter类对象，并往该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写剩余的索引区等内容，建立本地新的TsFile和对应.resource文件
        upgradedResources.add(endFileAndGenerateResource(tsFileIOWriter));  //把升级完成后的新的TsFile对应的新TsFileResource对象加入upgradedResources列表里。//endFileAndGenerateResource方法会做两件事：（1）在向该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写完数据区的内容并flush到本地后，再往该缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）（2）根据新的TsFile对象创建对应的TsFileResource对象，并把该TsFileResource对象的内容写到本地对应的.resource文件里
      }
      // write the remain modification for new file
      if (oldModification != null) {
        while (currentMod != null || modsIterator.hasNext()) {  //Todo:问题：会执行吗，modsIterator已经到尽头了
          if (currentMod == null) {
            currentMod = (Deletion) modsIterator.next();
          }
          for (Entry<TsFileIOWriter, ModificationFile> entry : fileModificationMap.entrySet()) {
            TsFileIOWriter tsFileIOWriter = entry.getKey();
            ModificationFile newMods = entry.getValue();
            newMods.write(
                new Deletion(
                    currentMod.getPath(),
                    tsFileIOWriter.getFile().length(),
                    currentMod.getStartTime(),
                    currentMod.getEndTime()));
          }
          currentMod = null;
        }
      }
    } catch (Exception e2) {
      throw new IOException(
          "TsFile upgrade process cannot proceed at position "
              + reader.position()
              + "because: "
              + e2.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /** TsFileName is changing like from 1610635230693-1-0.tsfile to 1610635230693-1-0-0.tsfile */
  @Override
  public String upgradeTsFileName(String oldTsFileName) {
    String[] name = oldTsFileName.split(TsFileConstant.TSFILE_SUFFIX);
    return name[0] + "-0" + TsFileConstant.TSFILE_SUFFIX;
  }

  /**
   * Due to TsFile version-3 changed the serialize way of integer in TEXT data and INT32 data with
   * PLAIN encoding, and also add a sum statistic for BOOLEAN data, these types of data need to
   * decode to points and rewrite in new TsFile.
   */
  @Override
  protected boolean checkIfNeedToDecode(  //判断是否需要解码（即把数据二进制流反序列化deserialize成数据点对象），若是以下几种数据类型和编码技术之一，或者该page的开始时间戳和结束时间戳不在一个时间分区里（说明该page里的所有数据不在一个时间分区里），则需要把该二进制流的pageData解码反序列化成数据点对象，用于后续对数据点对象进行数据的处理后再编码、压缩存入对应时间分区的TSFile文件里
      TSDataType dataType, TSEncoding encoding, PageHeader pageHeader) {  //根据数据类型和编码方式等，判断page里的数据是否需要解码
    return dataType == TSDataType.BOOLEAN
        || dataType == TSDataType.TEXT
        || (dataType == TSDataType.INT32 && encoding == TSEncoding.PLAIN)
        || StorageEngine.getTimePartition(pageHeader.getStartTime())  //该page的开始时间戳和结束时间戳不在一个时间分区里，说明该page里的所有数据不在一个时间分区里，则需要把该二进制流的pageData解码反序列化成数据点对象
            != StorageEngine.getTimePartition(pageHeader.getEndTime());
  }

  @Override
  protected void decodeAndWritePage(
      MeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap)
      throws IOException {
    valueDecoder.reset();
    PageReaderV2 pageReader =
        new PageReaderV2(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    rewritePageIntoFiles(batchData, schema, partitionChunkWriterMap);
  }

  /** check if the file to be upgraded has correct magic strings and version number */
  @Override
  protected boolean fileCheck() throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    String versionNumber = ((TsFileSequenceReaderForV2) reader).readVersionNumberV2();
    if (!versionNumber.equals(TSFileConfig.VERSION_NUMBER_V2)) {
      logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }
}
