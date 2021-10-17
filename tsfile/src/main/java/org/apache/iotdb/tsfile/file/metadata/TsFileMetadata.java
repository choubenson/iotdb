/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

/** TSFileMetaData collects all metadata info and saves in its data structure. */
public class TsFileMetadata { //该类其实就是IndexOfTimeseriesIndex，可以理解为整个TsFile的元数据类对象

  // bloom filter
  private BloomFilter bloomFilter;  //布隆过滤器

  // List of <name, offset, childMetadataIndexType>
  private MetadataIndexNode metadataIndex;  //该TsFile的IndexOfTimeseriesIndexs索引的第一个节点类对象,它一定是设备中间节点类型

  // offset of MetaMarker.SEPARATOR
  private long metaOffset; // 数值为2的metaMarker在该TsFile文件的偏移量，即该TsFile文件的索引区开始的位置,即TimeseriesIndex前面的marker的偏移量

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetadata deserializeFrom(ByteBuffer buffer) { //参数buffer里装着该TsFile的IndexOfTimeseriesIndex区的二进制数据，该方法从buffer里进行读取反序列化成TsFileMetadata对象并返回
    TsFileMetadata fileMetaData = new TsFileMetadata();

    // metadataIndex
    fileMetaData.metadataIndex = MetadataIndexNode.deserializeFrom(buffer);//使用从buffer反序列化读取的节点条目（即子节点索引项）和结束偏移和节点类型创建一个索引节点对象

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);  //从buffer里读取metaOffset数据，该数据即索引区的marker在该TsFile的偏移量
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {//如果buffer里还存在尚未被读取的数据，则
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);//从buffer里读取一个int型数据，代表字节长度byteLength，并创建长度为byteLength的字节数组bytes，从buffer里接着读取长度为byteLength的数据到bytes数组里后返回该数组
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);  //从buffer里读取filterSize变量数据
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);  //从buffer里读取hashFunctionSize变量数据
      fileMetaData.bloomFilter = BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize); //创建BloomFilter对象
    }

    return fileMetaData;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    // metadataIndex
    if (metadataIndex != null) {
      byteLen += metadataIndex.serializeTo(outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }

    // metaOffset
    byteLen += ReadWriteIOUtils.write(metaOffset, outputStream);

    return byteLen;
  }

  /**
   * use the given outputStream to serialize bloom filter.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   */
  public int serializeBloomFilter(OutputStream outputStream, Set<Path> paths) throws IOException {
    int byteLen = 0;
    BloomFilter filter = buildBloomFilter(paths);

    byte[] bytes = filter.serialize();
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    outputStream.write(bytes);
    byteLen += bytes.length;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getSize(), outputStream);
    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getHashFunctionSize(), outputStream);
    return byteLen;
  }

  /**
   * build bloom filter
   *
   * @return bloom filter
   */
  private BloomFilter buildBloomFilter(Set<Path> paths) {
    BloomFilter filter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), paths.size());
    for (Path path : paths) {
      filter.add(path.toString());
    }
    return filter;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public void setMetaOffset(long metaOffset) {
    this.metaOffset = metaOffset;
  }

  public MetadataIndexNode getMetadataIndex() { //获取该TsFile的IndexOfTimeseriesIndex索引的第一个节点类对象
    return metadataIndex;
  }

  public void setMetadataIndex(MetadataIndexNode metadataIndex) {
    this.metadataIndex = metadataIndex;
  }
}
