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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** used in query. */
public class Chunk {

  private ChunkHeader chunkHeader;
  private Statistics chunkStatistic;
  private ByteBuffer chunkData;
  private boolean isFromOldFile = false;
  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private long ramSize;

  public Chunk(
      ChunkHeader header,
      ByteBuffer buffer,
      List<TimeRange> deleteIntervalList,
      Statistics chunkStatistic) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deleteIntervalList = deleteIntervalList;
    this.chunkStatistic = chunkStatistic;
  }

  public ChunkHeader getHeader() {
    return chunkHeader;
  }

  public ByteBuffer getData() {
    return chunkData;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  //将参数的chunk的内容合并到当前chunk对象，即把待合并的参数chunk的chunkData部分追加到当前chunk的chunkData后，并更新该chunk的ChunkHeader。要注意的是，若参数chunk或者当前chunk只有一个page，则需要为其补上自己的pageStatistics，因为合并后的当前新chunk一定会至少有两个page（若当前Chunk只有一个page，则合并是把新的chunk的page追加当作新的page追加到当前chunk的原有page后）
  //具体做法是：
  // （1）分别判断参数chunk和当前chunk是有一个或大于1个page，并用offset进行标记（-1代表该Chunk有多个page，否则代表该Chunk唯一一个page的pageData的起始处），并记录新的DataSize
  // （2）更新当前Chunk合并后的ChunkHeader:ChunkType和dataSize
  // （3）创建新的newChunkData(大小为dataSize个字节),代表当前Chunk的合并后新的ChunkData，往里依次写入当前Chunk的chunkData和参数chunk的chunkData。要注意的是：若当前Chunk原先只有一个page，则要往newChunkData里重写入该page的pageHeader(添加statistics)和pageData当作第一个page,然后再写入参数chunk的其他page,因此合并后的当前chunk最少也有两个page（即原先当前chunk和参数chunk各只有一个page）
  public void mergeChunk(Chunk chunk) throws IOException {
    //新合并后的Chunk的dataSize
    int dataSize = 0;
    // from where the page data of the merged chunk starts, if -1, it means the merged chunk has
    // more than one page
    int offset1 = -1;
    // if the merged chunk has only one page, after merge with current chunk ,it will have more than
    // page
    // so we should add page statistics for it
    //将dataSize加上参数chunk的大小，并初始化offset1（若为-1则说明他有多个page，否则为该Chunk仅有的一个page的pageData的起始处）
    if (((byte) (chunk.chunkHeader.getChunkType() & 0x3F))
        == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      // read the uncompressedSize and compressedSize of this page
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      // record the position from which we can reuse
      //offset1为当前pageData的起始处（pageHeader后）
      offset1 = chunk.chunkData.position();
      chunk.chunkData.flip(); //切换到读模式，position转为0
      // the actual size should add another page statistics size
      dataSize += (chunk.chunkData.array().length + chunk.chunkStatistic.getSerializedSize());
    } else {
      // if the merge chunk already has more than one page, we can reuse all the part of its data
      // the dataSize is equal to the before
      dataSize += chunk.chunkData.array().length;
    }
    // from where the page data of the current chunk starts, if -1, it means the current chunk has
    // more than one page
    int offset2 = -1;
    // if the current chunk has only one page, after merge with the merged chunk ,it will have more
    // than page
    // so we should add page statistics for it
    //将dataSize加上当前chunk的大小，并初始化offset2（若为-1则说明他有多个page，否则为该Chunk仅有的一个page的pageData的起始处,且若当前Chunk只有一个page，则他合并后一定有大于1个page,要修改ChunkTypq）
    if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      // change the chunk type,更新当前Chunk的ChunkHeader的ChunkType：因为参数Chunk要合并到当前Chunk（this）对象，因此当前对象的page数量一定会大于1
      chunkHeader.setChunkType(MetaMarker.CHUNK_HEADER);
      // read the uncompressedSize and compressedSize of this page
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      // record the position from which we can reuse
      //offset2为当前pageData的起始处（pageHeader后）
      offset2 = chunkData.position();
      chunkData.flip();
      // the actual size should add another page statistics size
      dataSize += (chunkData.array().length + chunkStatistic.getSerializedSize());
    } else {
      // if the current chunk already has more than one page, we can reuse all the part of its data
      // the dataSize is equal to the before
      dataSize += chunkData.array().length;
    }
    //更新当前Chunk的ChunkHeader的dataSize
    chunkHeader.setDataSize(dataSize);
    //为当前Chunk的新的ChunkData
    ByteBuffer newChunkData = ByteBuffer.allocate(dataSize);
    // the current chunk has more than one page, we can use its data part directly without any
    // changes
    //若该Chunk有多个page
    if (offset2 == -1) {
      //直接先把当前Chunk的chunkData放入newChunkData,其中应该包含当前Chunk的多个page(pageHeader+pageData)
      newChunkData.put(chunkData.array());
    } else { //该Chunk只有一个page the current chunk has only one page, we need to add one page statistics for it
      byte[] b = chunkData.array();
      //put the uncompressedSize and compressedSize of this page
      //首先放入当前Chunk的第一个page的pageHeader
      newChunkData.put(b, 0, offset2);
      //然后放入当前Chunk第一个page的statistics    add page statistics
      PublicBAOS a = new PublicBAOS();
      chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      //最后放入当前Chunk的第一个page的pageData put the remaining page data
      newChunkData.put(b, offset2, b.length - offset2);
    }
    // the merged chunk has more than one page, we can use its data part directly without any
    // changes
    if (offset1 == -1) {
      newChunkData.put(chunk.chunkData.array());
    } else {
      // put the uncompressedSize and compressedSize of this page
      byte[] b = chunk.chunkData.array();
      newChunkData.put(b, 0, offset1);
      // add page statistics
      PublicBAOS a = new PublicBAOS();
      chunk.chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      // put the remaining page data
      newChunkData.put(b, offset1, b.length - offset1);
    }
    chunkData = newChunkData;
  }

  public Statistics getChunkStatistic() {
    return chunkStatistic;
  }

  public boolean isFromOldFile() {
    return isFromOldFile;
  }

  public void setFromOldFile(boolean isFromOldFile) {
    this.isFromOldFile = isFromOldFile;
  }
}
