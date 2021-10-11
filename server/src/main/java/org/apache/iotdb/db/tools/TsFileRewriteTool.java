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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TsFileRewriteTool implements AutoCloseable {   //TsFile重写工具

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewriteTool.class);

  protected TsFileSequenceReader reader;    //旧TsFile的顺序读取器
  protected File oldTsFile;     //旧的待重写TsFile
  protected List<Modification> oldModification; //旧TsFIle文件对应的修改操作对象列表
  protected Iterator<Modification> modsIterator;

  /** new tsFile writer -> list of new modification */
  protected Map<TsFileIOWriter, ModificationFile> fileModificationMap;//当旧TsFile有删除记录时，则该列表存放着升级后对应每个新TsFileIOWriter类对象都有自己对应新TsFile文件的新ModificationFile(.mods)修改文件类对象；否则该列表为空

  protected Deletion currentMod;
  protected Decoder defaultTimeDecoder =    //默认的时间解码器
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
  protected Decoder valueDecoder;   //数据解码器

  /** PartitionId -> TsFileIOWriter */
  protected Map<Long, TsFileIOWriter> partitionWriterMap;  //存放了每个时间分区的新TSFile文件的TsFileIOWriter类对象

  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the real data and rewrite to some
   * new tsFiles.
   *
   * @throws IOException If some I/O error occurs
   */
  public TsFileRewriteTool(TsFileResource resourceToBeRewritten) throws IOException {
    oldTsFile = resourceToBeRewritten.getTsFile();
    String file = oldTsFile.getAbsolutePath();
    reader = new TsFileSequenceReader(file);
    partitionWriterMap = new HashMap<>();
    if (FSFactoryProducer.getFSFactory().getFile(file + ModificationFile.FILE_SUFFIX).exists()) { //若本地存在该旧TsFile对应的.mods删除文件
      oldModification = (List<Modification>) resourceToBeRewritten.getModFile().getModifications(); //将旧TsFile对应的一条条删除记录操作放入该修改列表里
      modsIterator = oldModification.iterator();
      fileModificationMap = new HashMap<>();
    }
  }

  public TsFileRewriteTool(TsFileResource resourceToBeRewritten, boolean needReaderForV2)
      throws IOException {
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
      fileModificationMap = new HashMap<>();
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
  public void parseAndRewriteFile(List<TsFileResource> rewrittenResources)    //会过滤掉mods文件的数据
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
            endChunkGroup();//用于结束上一个ChunkGroup，并把TsFileIOWriter里的TsFileOutput缓存给flush写到本地文件上
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
                      ? reader.readCompressedPage(pageHeader) //若该page数据不需要解码，则从当前位置直接读取经压缩后page大小的字节的数据到ByteBuffer缓存里。读指针向后移动
                      : reader.readPage(pageHeader, header.getCompressionType()); //若需要解码，则需要先把该page的二进制流数据进行解压uncompress，然后才能解码。即则根据pageHeader和压缩类型，把该page的数据进行解压后存入ByteBuffer缓存并返回
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
              tsFileIOWriter.writePlanIndices();//往该TsFile对应的TsFileIOWriter类对象的TsFileOutput对象的输出流BufferedOutputStream的缓存数组里写入planIndex，并将该TsFileIOWriter类对象的TsFileOutput对象的输出流BufferedOutputStream的缓存数组内容flush到本地TsFile文件
            }
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      endChunkGroup();//用于结束该TsFile的最后一个ChunkGroup，并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {//对每个TsFileIOWriter，根据数据区的内容生成对应的索引区内容并写到新的TsFile，至此，本地新的TsFile文件已经生成并写入了整理后的新数据，本地对应的resource文件也已经生成并写入新内容
        rewrittenResources.add(endFileAndGenerateResource(tsFileIOWriter));//往本地新TsFile写入剩余的内容（索引区和末尾MagicString）,然后本地创建对应的TsFileResource文件，并返回该新TsFileResource对象 //该方法会做两件事：（1）往TsFileIOWriter对象的缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）（2）根据新的TsFile对象创建对应的TsFileResource对象，并把该TsFileResource对象的内容写到本地对应的.resource文件里
      }


      /*// write the remain modification for new file
      if (oldModification != null) {
        while (currentMod != null || modsIterator.hasNext()) {
          if (currentMod == null) {
            currentMod = (Deletion) modsIterator.next();
          }
          for (Entry<TsFileIOWriter, ModificationFile> entry : fileModificationMap.entrySet()) {
            TsFileIOWriter tsFileIOWriter = entry.getKey();
            ModificationFile newMods = entry.getValue();
            newMods.write(
                new Deletion(
                    currentMod.getPath(),
                    tsFileIOWriter.getFile().length(),  //Todo:bug问题：生效位置为该文件当前长度
                    currentMod.getStartTime(),
                    currentMod.getEndTime()));
          }
          currentMod = null;
        }
      }*/
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
   * If the page have no statistics or crosses multi partitions, will return true, otherwise return
   * false.
   */
  protected boolean checkIfNeedToDecode(  //如果该Chunk只有一个page或者该page的数据不在一个时间分区里则需要解码
      TSDataType dataType, TSEncoding encoding, PageHeader pageHeader) {
    if (pageHeader.getStatistics() == null) { //若该Chunk只有一个page，则该page的pageHeader没有statistics信息，这样就不能知道该page数据的起使时间和结束时间，进而无法判断该page数据所属的时间分区
      return true;
    }
    if(oldModification!=null){//若该page的数据存在待删除的部分则需要反序列化解码decode,否则则不需要（可以直接以二进制流的形式写入新TsFile里）
      modsIterator=oldModification.iterator();
      Deletion currentDeletion=null;
      while(modsIterator.hasNext()){
        currentDeletion=(Deletion)modsIterator.next();
        if(pageHeader.getStartTime()<=currentDeletion.getEndTime()&&pageHeader.getEndTime()>=currentDeletion.getStartTime()){
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
  protected void reWriteChunk(  //重写该Chunk的数据到对应时间分区的TsFile文件里：(1) 先遍历该Chunk的每个page的pageData，将该page的每个数据点写入对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里 (2)遍历每个时间分区TsFile对应的待写入传感器Chunk的ChunkWriterImpl对象,使用该Chunk的ChunkWriterImpl对象往对应TsFile的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的内容（ChunkHeader和page内容（pageHeader+pageData）），若是第一个Chunk则要先往对应的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入ChunkGroupHeader
      String deviceId,
      boolean firstChunkInChunkGroup,//该Chunk是否是所属ChunkGroup里的第一个Chunk
      MeasurementSchema schema,//传感器配置类对象
      List<PageHeader> pageHeadersInChunk,//该Chunk的所有page的pageHeader列表
      List<ByteBuffer> pageDataInChunk,//该Chunk的所有page的pageData列表，存的是二进制格式
      List<Boolean> needToDecodeInfoInChunk)//该Chunk的所有page里的数据是否需要解码列表
      throws IOException, PageException { //此方法是以Chunk里的page为单位进行重写（可以自己另创方法考虑以chunk为单位）
    valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());  //根据该Chunk传感器的编码方式和数据类型获得对应的解码器对象
    Map<Long, ChunkWriterImpl> partitionChunkWriterMap = new HashMap<>(); //存放每个时间分区TsFile的某一传感器对应的ChunkWriterImpl类对象
    for (int i = 0; i < pageDataInChunk.size(); i++) {  //遍历每个page，将该page的每个数据点写入其所属时间分区TsFile的对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
      if (Boolean.TRUE.equals(needToDecodeInfoInChunk.get(i))) {  //若该page数据需要解码，则说明需要把二进制流形式的pageData反序列化成数据点对象类，即放入BatchData类对象里
        decodeAndWritePage(schema, pageDataInChunk.get(i), partitionChunkWriterMap); //将给定存放在二进制缓存里的pageData进行解码成BatchData类对象，并对里面的每个数据点重写进TsFile里。即将每个数据点所属的时间分区和其所属的传感器的schema配置对象创建每个时间分区TsFile的该传感器对应的ChunkWriterImpl对象，用对应的ChunkWriterImpl把每个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page
      } else {            //若该page数据不需要解码，则直接把二进制流形式的pageData写进其所属Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
        writePage(   //此处直接根据给定的pageHeader和pageData，根据该page数据所在时间分区和该page所属的传感器创建对应的ChunkWriterImpl对象，并用该ChunkWriterImpl对象把该页page数据（pageData+pageHeader）写入该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
            schema, pageHeadersInChunk.get(i), pageDataInChunk.get(i), partitionChunkWriterMap);  //该page数据是在一个时间分区里的
      }
    }
    for (Entry<Long, ChunkWriterImpl> entry : partitionChunkWriterMap.entrySet()) { //遍历每个时间分区TsFile对应的待写入传感器Chunk的ChunkWriterImpl对象,使用该Chunk的ChunkWriterImpl对象往对应TsFile的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的内容（ChunkHeader和page内容（pageHeader+pageData）），若是第一个Chunk则要先往对应的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入ChunkGroupHeader
      long partitionId = entry.getKey();//获取时间分区ID
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);//根据时间分区ID获取对应TSFile文件的TsFileIOWriter类对象
      if (firstChunkInChunkGroup) { //如果是第一个Chunk，则写入ChunkGroupHeader
        tsFileIOWriter.startChunkGroup(deviceId); //开启一个新的设备ChunkGroup，即把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
      }
      // write chunks to their own upgraded tsFiles
      IChunkWriter chunkWriter = entry.getValue();//该时间分区的TsFile文件有了该设备的ChunkGroup后，就获取ChunkWriterImpl对象
      chunkWriter.writeToFileWriter(tsFileIOWriter);//使用ChunkWriterImpl对象，首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
    }
  }

  /**
   * Benson
   * This method is for rewriting the Chunk which data is in one time partition.
   * @param deviceId
   * @param firstChunkInChunkGroup
   * @param schema
   * @param pageHeadersInChunk
   * @param pageDataInChunk
   * @param needToDecodeInfoInChunk
   */   //将该Chunk的Chunkheader和经过滤后的数据（ChunkWriterImpl里的pageBuffer）写入新TsFile对应的TsFileIOWriter的缓存里，若是第一个Chunk还要写入ChunkGroupHeader
  protected void reWriteChunkInOnePartition(//将该Chunk经mods文件过滤后的数据重写到对应时间分区的新TsFile文件对应的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里：(1) 先遍历该Chunk的每个page的pageData，对需要解码的page则将该page的数据根据.mods文件过滤解码成BatchData，并写入对应时间分区的新TsFile的该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里 ；若该page不需要解码则直接把该page的二进制数据写进新TsFile的该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里 (2)使用该Chunk的ChunkWriterImpl对象往新TsFile的对应TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的内容（ChunkHeader和page内容（pageHeader+pageData）），若是第一个Chunk则要先往对应的TsFileIOWriter的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入ChunkGroupHeader
      String deviceId,
      boolean firstChunkInChunkGroup,//该Chunk是否是所属ChunkGroup里的第一个Chunk
      MeasurementSchema schema,//传感器配置类对象
      List<PageHeader> pageHeadersInChunk,//该Chunk的所有page的pageHeader列表
      List<ByteBuffer> pageDataInChunk,//该Chunk的所有page的pageData列表，存的是二进制格式
      List<Boolean> needToDecodeInfoInChunk) throws IOException, PageException {//该Chunk的所有page里的数据是否需要解码列表
    valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());  //根据该Chunk传感器的编码方式和数据类型获得对应的解码器对象
    ChunkWriterImpl chunkWriter=new ChunkWriterImpl(schema);
    for (int i = 0; i < pageDataInChunk.size(); i++) {  //遍历每个page，将该page的每个数据点写入其所属时间分区TsFile的对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
      if (Boolean.TRUE.equals(needToDecodeInfoInChunk.get(i))) {  //若该page数据需要解码，则说明需要把二进制流形式的pageData反序列化成数据点对象类，即放入BatchData类对象里
        decodeAndWritePageInOnePartition(schema, pageDataInChunk.get(i), chunkWriter); //将给定存放在二进制缓存里的pageData进行解码过滤掉mods文件里删除的数据，解码成BatchData类对象，并创建该page时间分区TsFile的该传感器对应的ChunkWriterImpl对象，用对应的ChunkWriterImpl把每个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page
      } else {            //若该page数据不需要解码，则直接把二进制流形式的pageData写进其所属Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
        writePageInOnePartition(   //此处直接根据给定的pageHeader和pageData，根据该page数据所在时间分区和该page所属的传感器创建对应的ChunkWriterImpl对象，并用该ChunkWriterImpl对象把该页page数据（pageData+pageHeader）写入该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
            schema, pageHeadersInChunk.get(i), pageDataInChunk.get(i), chunkWriter);  //该page数据是在一个时间分区里的
      }
    }
    long partitionId = StorageEngine.getTimePartition(pageHeadersInChunk.get(0).getStartTime());
    TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);
    if (firstChunkInChunkGroup) { //如果是第一个Chunk，则写入ChunkGroupHeader
      tsFileIOWriter.startChunkGroup(deviceId); //开启一个新的设备ChunkGroup，即把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
    }
    chunkWriter.writeToFileWriter(tsFileIOWriter);//使用ChunkWriterImpl对象，首先封口当前page(即把当前Chunk的pageWriter输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里,最后重置该pageWriter)，然后往TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里写入该Chunk的ChunkHeader,最后再写入当前Chunk的所有page数据（pageBuffer输出流的缓存数组内容）
  }

  protected void endChunkGroup() throws IOException {//结束每个时间分区的TsFileIOWriter类对象对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空）
    for (TsFileIOWriter tsFileIoWriter : partitionWriterMap.values()) {   //遍历获取每个时间分区的TsFileIOWriter类对象
      tsFileIoWriter.endChunkGroup(); //结束该TsFileIOWriter类对象对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
    }
  }

  public String upgradeTsFileName(String oldTsFileName) {
    return oldTsFileName;
  }

  //此处是在原先时间分区目录下又重复创建了临时的指定时间分区partition的目录，然后该临时时间分区目录下再新建新TsFile
  protected TsFileIOWriter getOrDefaultTsFileIOWriter(File oldTsFile, long partition) {//获取指定时间分区partition对应的TsFileIOWriter类对象，若没有则：（1）在本地新建一个该时间分区目录下的TsFile文件（2）然后用该新TsFile文件创建对应新的TsFileIOWriter类对象（3）若原先旧TsFile存在删除操作，则创建该新TsFile对应的修改文件ModificationFile类对象，并往fileModificationMap放入该TsFile的TsFileIOWriter和对应的新修改文件ModificationFile对象
    return partitionWriterMap.computeIfAbsent(    //获取指定时间分区partition对应的TsFileIOWriter类对象，若没有则在本地新建一个该时间分区目录下的TsFile文件，然后用该新TsFile文件创建对应新的TsFileIOWriter类对象
        partition,
        k -> {    //若不存在此时间分区对应的sFileIOWriter类对象，则
          File partitionDir =     //创建时间分区目录文件对象
              FSFactoryProducer.getFSFactory()
                  .getFile(oldTsFile.getParent() + File.separator + partition);
          if (!partitionDir.exists()) { //若本地不存在此文件，则创建该目录
            partitionDir.mkdirs();
          }
          File newFile =      //根据路径创建新的TsFile文件对象（并非创建本地文件）
              FSFactoryProducer.getFSFactory()
                  .getFile(partitionDir + File.separator + upgradeTsFileName(oldTsFile.getName()));
          try {
            if (newFile.exists()) {//若本地存在该新的TsFile文件，则删除它，因为该文件可能是上次重写过程中因为中断而残留的文件
              logger.debug("delete uncomplated file {}", newFile);
              Files.delete(newFile.toPath());
            }
            if (!newFile.createNewFile()) { //在本地新建该新的TsFile文件
              logger.error("Create new TsFile {} failed because it exists", newFile);
            }
            TsFileIOWriter writer = new TsFileIOWriter(newFile);  //使用该新的TsFile创建对应的TsFileIOWriter类对象
            if (oldModification != null) {  //若旧TsFIle文件对应的修改操作对象列表不为空，则      ；若旧TsFile没有.mods文件，即没有删除记录，则无需创建。
              fileModificationMap.put(    //创建该新TsFile对应的修改文件ModificationFile类对象，并往fileModificationMap放入该TsFile的TsFileIOWriter和对应的新修改文件ModificationFile对象
                  writer, new ModificationFile(newFile + ModificationFile.FILE_SUFFIX));
            }
            return writer;
          } catch (IOException e) {
            logger.error("Create new TsFile {} failed ", newFile, e);
            return null;
          }
        });
  }

  /**
   * Benson
   * @param schema
   * @param pageHeader
   * @param pageData
   * @param chunkWriter
   * @throws PageException
   */ //writePage方法可以删掉，只用该方法
  protected void writePageInOnePartition(//此处直接根据给定的pageHeader和pageData，根据该page数据所在时间分区和该page所属的传感器创建对应的ChunkWriterImpl对象，并用该ChunkWriterImpl对象把该页page数据（pageData+pageHeader）写入该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
      MeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      ChunkWriterImpl chunkWriter) throws PageException {
    long partitionId = StorageEngine.getTimePartition(pageHeader.getStartTime()); //获取此page开始时间所属的时间分区
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);//获取指定时间分区partition对应的TsFileIOWriter类对象，若没有则：（1）在本地新建一个该时间分区目录下的TsFile文件（2）然后用该新TsFile文件创建对应新的TsFileIOWriter类对象（3）创建该新TsFile对应的修改文件ModificationFile类对象，并往fileModificationMap放入该TsFile的TsFileIOWriter和对应的新修改文件ModificationFile对象
    if(chunkWriter==null){
      chunkWriter=new ChunkWriterImpl(schema);
    }
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader);//往当前Chunk的ChunkWriterImpl对象的输出流pageBuffer里写入page数据（pageData+pageHeader，此处pageData是存放在二进制缓存ByteBuffer对象里）
  }

  protected void writePage( //此处直接根据给定的pageHeader和pageData，根据该page数据所在时间分区和该page所属的传感器创建对应的ChunkWriterImpl对象，并用该ChunkWriterImpl对象把该页page数据（pageData+pageHeader）写入该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
      MeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap)
      throws PageException {
    long partitionId = StorageEngine.getTimePartition(pageHeader.getStartTime()); //获取此page开始时间所属的时间分区
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);//获取指定时间分区partition对应的TsFileIOWriter类对象，若没有则：（1）在本地新建一个该时间分区目录下的TsFile文件（2）然后用该新TsFile文件创建对应新的TsFileIOWriter类对象（3）创建该新TsFile对应的修改文件ModificationFile类对象，并往fileModificationMap放入该TsFile的TsFileIOWriter和对应的新修改文件ModificationFile对象
    ChunkWriterImpl chunkWriter =
        partitionChunkWriterMap.computeIfAbsent(partitionId, v -> new ChunkWriterImpl(schema));//获取该page经更新分割后所在时间分区TsFile里的Chunk传感器的ChunkWriterImpl
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader);//往当前Chunk的ChunkWriterImpl对象的输出流pageBuffer里写入page数据（pageData+pageHeader，此处pageData是存放在二进制缓存ByteBuffer对象里）
  }

  protected void decodeAndWritePage( //将给定存放在二进制缓存里的pageData进行解码成BatchData类对象，并对里面的每个数据点重写进TsFile里。即将每个数据点所属的时间分区和其所属的传感器的schema配置对象创建每个时间分区TsFile的该传感器对应的ChunkWriterImpl对象，用对应的ChunkWriterImpl把每个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page
      MeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap)
      throws IOException {
    valueDecoder.reset(); //重置解码器
    PageReader pageReader =     //根据二进制流的pageData和数据类型，以及时间、数值解码器去创建Page读取类对象
        new PageReader(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    //读取.mods文件里的删除时间范围
    List<TimeRange> deleteIntervalList = new ArrayList<>();
    if(oldModification!=null){
      modsIterator=oldModification.iterator();  //可以不需要？
      Deletion currentDeletion=null;
      while(modsIterator.hasNext()){
        currentDeletion=(Deletion)modsIterator.next();
        deleteIntervalList.add(new TimeRange(currentDeletion.getStartTime(),currentDeletion.getEndTime()));
      }
    }
    //设置pageReader的删除时间范围对象
    pageReader.setDeleteIntervalList(deleteIntervalList);
    BatchData batchData = pageReader.getAllSatisfiedPageData();//通过pageReader对象的时间解码器和数值解码器去读取该page所有满足条件（不在被删除时间范围内以及符合过滤器，此处删除时间范围是null）的时间戳和对应的数据，放入BatchData类对象里
    rewritePageIntoFiles(batchData, schema, partitionChunkWriterMap);//该方法是将传来的page数据进行重写到TsFile里，根据传来batchData里每个数据点所属的时间分区和其所属的传感器的schema配置对象创建每个时间分区TsFile的该传感器对应的ChunkWriterImpl对象，用对应的ChunkWriterImpl把每个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page
  }

  /**
   * Benson
   */
  protected void decodeAndWritePageInOnePartition(
      MeasurementSchema schema,
      ByteBuffer pageData,
      ChunkWriterImpl chunkWriter) throws IOException {
    valueDecoder.reset(); //重置解码器
    PageReader pageReader =     //根据二进制流的pageData和数据类型，以及时间、数值解码器去创建Page读取类对象
        new PageReader(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    //设置pageReader的删除时间范围对象
    //读取.mods文件里的删除时间范围
    List<TimeRange> deleteIntervalList = new ArrayList<>();
    if(oldModification!=null){
      modsIterator=oldModification.iterator();  //可以不需要？
      Deletion currentDeletion=null;
      while(modsIterator.hasNext()){
        currentDeletion=(Deletion)modsIterator.next();
        deleteIntervalList.add(new TimeRange(currentDeletion.getStartTime(),currentDeletion.getEndTime()));
      }
    }
    pageReader.setDeleteIntervalList(deleteIntervalList);
    BatchData batchData = pageReader.getAllSatisfiedPageData();//通过pageReader对象的时间解码器和数值解码器去读取该page所有满足条件（不在被删除时间范围内以及符合过滤器）的时间戳和对应的数据，放入BatchData类对象里
    rewritePageInOnePartitionIntoFiles(batchData,schema,chunkWriter);//此处只是把该page数据顺序写到对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
  }

  /**
   * Benson
   * @param batchData
   * @param schema
   * @param chunkWriter
   */     //此处只是把该page数据顺序写到对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
  protected void rewritePageInOnePartitionIntoFiles(//该方法是将传来的page数据进行重写到TsFile里，根据传来batchData里的数据创建对应时间分区TsFile的该传感器对应的ChunkWriterImpl对象，用对应的ChunkWriterImpl把每个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则要把当前Chunk的pageWriter里两个输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里。结束当前page的写操作后需要把当前pageWriter缓存里的数据flush写到所属Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
      BatchData batchData,  //存放某page里经mods文件过滤后的data
      MeasurementSchema schema, //该page所属Chunk传感器的配置类对象
      ChunkWriterImpl chunkWriter) {   //该page所属Chunk的ChunkWriterImpl
    long partitionId=StorageEngine.getTimePartition(batchData.currentTime()); //获取该page所属时间分区
    while(batchData.hasCurrent()){
      long time=batchData.currentTime();
      Object value=batchData.currentValue();
      getOrDefaultTsFileIOWriter(oldTsFile, partitionId); //获取指定时间分区partition对应的TsFileIOWriter类对象，若没有则：（1）在本地新建一个该时间分区目录下的TsFile文件（2）然后用该新TsFile文件创建对应新的TsFileIOWriter类对象（3）创建该新TsFile对应的修改文件ModificationFile类对象，并往该类对象的fileModificationMap属性放入该新TsFile的TsFileIOWriter和对应的新修改文件ModificationFile对象
      switch (schema.getType()) {
        case INT32:
          chunkWriter.write(time, (int) value, false);//将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page
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
    //强制把当前page那些还存留在pageWriter缓存的数据给强制flush写到对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
    chunkWriter.sealCurrentPage();
  }

  protected void rewritePageIntoFiles(  //该方法是将传来的page数据进行重写到TsFile里，根据传来batchData里每个数据点所属的时间分区和其所属的传感器的schema配置对象创建每个时间分区TsFile的该传感器对应的ChunkWriterImpl对象，用对应的ChunkWriterImpl把每个数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page，若要开启新的page则要把当前Chunk的pageWriter里两个输出流timeOut和valueOut的缓存数据写到该Chunk的ChunkWriterImpl的输出流pageBuffer缓存里。结束当前page的写操作后需要把当前pageWriter缓存里的数据flush写到所属Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
      BatchData batchData,      //存放某page里经mods文件过滤后的data
      MeasurementSchema schema, //该page所属Chunk传感器的配置类对象
      Map<Long, ChunkWriterImpl> partitionChunkWriterMap) {//旧的page数据经update更新可能被分割成多个时间分区的数据。比如旧的page所在的Chunk对应的传感器ID为root.demo.s1.height，这个时候进行更新操作，该旧page数据被分割存储在时间分区0和1的两个TsFile里各自的root.demo.s1.height传感器对应Chunk里。即旧page数据可能被分割存储在不同时间分区TsFile的各自传感器Chunk里，于是经分割后不同时间分区的数据需要有自己的ChunkWriterImpl进行写入到对应时间分区的TsFile文件的传感器Chunk里
   while (batchData.hasCurrent()) {  //循环遍历指定page里的batchData的数据点，根据每个数据点时间戳所在的时间分区获取对应的ChunkWriterImpl类对象，
      long time = batchData.currentTime();//获取时间戳
      Object value = batchData.currentValue();//获取该时间戳对应的数据
      long partitionId = StorageEngine.getTimePartition(time);//根据时间戳获取其所属时间分区ID（此处是更新操作，将旧数据按照时间戳划分所属时间分区以决定后续写入哪个TsFile文件里）

      ChunkWriterImpl chunkWriter =       //从partitionChunkWriterMap中拿取该时间分区对应的ChunkWriterImpl类对象，若不存在则用传感器配置类对象创建一个
          partitionChunkWriterMap.computeIfAbsent(partitionId, v -> new ChunkWriterImpl(schema));//经分割后不同时间分区的数据需要有自己的ChunkWriterImpl进行写入到对应时间分区的TsFile文件的传感器Chunk里
      getOrDefaultTsFileIOWriter(oldTsFile, partitionId); //获取指定时间分区partition对应的TsFileIOWriter类对象，若没有则：（1）在本地新建一个该时间分区目录下的TsFile文件（2）然后用该新TsFile文件创建对应新的TsFileIOWriter类对象（3）创建该新TsFile对应的修改文件ModificationFile类对象，并往fileModificationMap放入该TsFile的TsFileIOWriter和对应的新修改文件ModificationFile对象
      switch (schema.getType()) {
        case INT32:
          chunkWriter.write(time, (int) value, false);//将给定的数据点交由该Chunk的pageWriter写入到其对应的两个输出流timeOut和valueOut的缓存中，并检查该Chunk的pageWriter的数据点or占用内存的大小情况，判断是否要开启一个新的page
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
    partitionChunkWriterMap.values().forEach(writer -> {
      writer.sealCurrentPage();   //强制把每个ChunkWriter的当前page那些还存留在pageWriter缓存的数据给强制flush写到对应Chunk的ChunkWriterImpl的输出流pageBuffer缓存里
    });
  }

  /** check if the file has correct magic strings and version number */
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
      throws IOException {//该方法会做两件事：（1）在向该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写完数据区的内容并flush到本地后，再往该缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）（2）根据新的TsFile对象创建对应的TsFileResource对象，并把该TsFileResource对象的内容写到本地对应的.resource文件里
    tsFileIOWriter.endFile();//在向该TsFileIOWriter的TsFileOutput的缓存输出流BufferedOutputStream的数组里写完数据区的内容并flush到本地后，再往该缓存里写对应索引区的内容，以及剩余的小内容（如TsFile结尾的Magic String等），最后关闭该TsFileIOWriter的TsFileOutput的两个输出流BufferedOutputStream和FileOutputStream，会往对应的本地TsFile写入该TsFileIOWriter缓存里的数据（即索引区和小内容等，数据区之前已经flush到本地文件了）
    TsFileResource tsFileResource = new TsFileResource(tsFileIOWriter.getFile());//根据新建的TsFile文件对象创建对应的TsFileResource对象
    Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        tsFileIOWriter.getDeviceTimeseriesMetadataMap();//(设备路径，该设备下所有传感器的TimeseriesMetadata对象列表)
    //下面是对该新TsFile对应的新TsFileResource的属性进行初始化赋值
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {//遍历每个设备和对应设备下所有传感器的TimeseriesMetadata对象列表
      String device = entry.getKey();//获取设备路径
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {//遍历该设备下所有传感器的TimeseriesMetadata对象列表
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());  //更新TsFileResource的startTime
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());//更新TsFileResource的endTime
      }
    }
    tsFileResource.setMinPlanIndex(minPlanIndex);
    tsFileResource.setMaxPlanIndex(maxPlanIndex);
    tsFileResource.setClosed(true);
    tsFileResource.serialize(); //将该TsFile文件对应的TsFileResource对象里的内容写到本地的.resource文件里
    return tsFileResource;
  }
}
