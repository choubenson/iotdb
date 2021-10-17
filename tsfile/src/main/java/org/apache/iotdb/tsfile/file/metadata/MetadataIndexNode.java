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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MetadataIndexNode {  //IndexOfTimeseriesIndex索引节点类，分为设备节点和传感器节点，它们又各自分为中间节点和叶子节点。若是设备节点，则该节点里的索引条目的内容都是设备相关的；若是传感器节点，则该节点里的索引条目的内容都是传感器相关的

  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private final List<MetadataIndexEntry> children;  //当前节点的一条条的条目内容，代表了该索引节点有哪些子节点以及各自的偏移量是多少。一个索引节点里的所有条目是根据每个条目的name按照字典排序的
  private long endOffset; //当前节点的最底层的最后一个子节点的末尾在TsFile里的偏移量

  /** type of the child node at offset */
  private final MetadataIndexNodeType nodeType; //当前索引节点的类型，共有四种类型

  public MetadataIndexNode(MetadataIndexNodeType nodeType) {
    children = new ArrayList<>();
    endOffset = -1L;
    this.nodeType = nodeType;
  }

  public MetadataIndexNode(
      List<MetadataIndexEntry> children, long endOffset, MetadataIndexNodeType nodeType) {
    this.children = children;
    this.endOffset = endOffset;
    this.nodeType = nodeType;
  }

  public List<MetadataIndexEntry> getChildren() {
    return children;
  }

  public long getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(long endOffset) {
    this.endOffset = endOffset;
  }

  public MetadataIndexNodeType getNodeType() {
    return nodeType;
  }

  public void addEntry(MetadataIndexEntry metadataIndexEntry) {
    this.children.add(metadataIndexEntry);
  }

  boolean isFull() {
    return children.size() >= config.getMaxDegreeOfIndexNode();
  }

  MetadataIndexEntry peek() {
    if (children.isEmpty()) {
      return null;
    }
    return children.get(0);
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(children.size(), outputStream);
    for (MetadataIndexEntry metadataIndexEntry : children) {
      byteLen += metadataIndexEntry.serializeTo(outputStream);
    }
    byteLen += ReadWriteIOUtils.write(endOffset, outputStream);
    byteLen += ReadWriteIOUtils.write(nodeType.serialize(), outputStream);
    return byteLen;
  }

  public static MetadataIndexNode deserializeFrom(ByteBuffer buffer) {//将buffer的内容反序列化成索引节点MetadataIndexNode对象,即使用从buffer反序列化读取的节点条目（即子节点索引项）和结束偏移和节点类型创建一个索引节点对象
    List<MetadataIndexEntry> children = new ArrayList<>();
    int size = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);  //从buffer里读取当前索引节点条目的数量,即当前节点的子节点数量，是一个int型
    for (int i = 0; i < size; i++) {
      children.add(MetadataIndexEntry.deserializeFrom(buffer));//从buffer里反序列化读取名称和偏移量，并用此创建节点条目对象加入该节点的子节点列表children变量里
    }
    long offset = ReadWriteIOUtils.readLong(buffer);//从buffer里读取当前节点结束的偏移量，是一个Long型
    MetadataIndexNodeType nodeType =
        MetadataIndexNodeType.deserialize(ReadWriteIOUtils.readByte(buffer)); //从buffer里读取一个byte，并获得对应的当前索引节点的类型
    return new MetadataIndexNode(children, offset, nodeType); //使用读取的节点条目和结束偏移和节点类型创建一个索引节点对象
  }

  public Pair<MetadataIndexEntry, Long> getChildIndexEntry(String key, boolean exactSearch) {//该方法其实就是用来查找名为key的索引条目它所指向的孩子节点的开始偏移位置和结束偏移位置。具体做法是从当前索引节点查找名字为key的索引条目：1. 若找到了，则把该条目对应条目对象和该条目指向的子节点的结束位置放入pair对象里，即<名为key的索引条目对象，该条目指向的子节点的结束偏移位置> 2. 若没有找到，则若（1）exactSearch为true说明要精确查找，则返回null（2）exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目对象和对应指向子节点的结束位置
    int index = binarySearchInChildren(key, exactSearch); //使用二分法搜索名称为key的子条目是当前节点的第几个条目，即搜索名称为key的条目的索引位置。当该节点不存在名为key的条目时，若exactSearch为true说明要精确查找，则返回-1；若exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目的位置（注意：一个节点里的所有条目是根据每个条目的name按照字典排序的）
    if (index == -1) {  //若exactSearch为true且不存在名为key的条目
      return null;
    }
    long childEndOffset;//要查找的名为key的索引条目所指向的子节点的结束偏移位置
    if (index != children.size() - 1) { //若不是最后一个条目
      childEndOffset = children.get(index + 1).getOffset();//待查找的子节点的结束偏移位置为下个子节点的开始位置
    } else {  //若是最后一个条目，则
      childEndOffset = this.endOffset;//该条目所指向的孩子节点的结束偏移位置为当前节点的最后一个子节点的结束位置
    }
    return new Pair<>(children.get(index), childEndOffset);//把该条目对应条目对象和该条目指向的子节点的结束位置放入pair对象里并返回
  }

  int binarySearchInChildren(String key, boolean exactSearch) { //使用二分法搜索名称为key的子条目是当前节点的第几个条目，即搜索名称为key的条目的索引位置。当该节点不存在名为key的条目时，若exactSearch为true说明要精确查找，则返回-1；若exactSearch为false说明可以模糊查找，则返回该节点里离名称为key的目标索引条目最近的上一个条目的位置（注意：一个节点里的所有条目是根据每个条目的name按照字典排序的）
    int low = 0;
    int high = children.size() - 1;//当前节点的孩子节点数量-1

    while (low <= high) {
      int mid = (low + high) >>> 1;//获取当前节点的第中间的子节点的索引位置。无符号右移一位，无符号即高位补0
      MetadataIndexEntry midVal = children.get(mid);//获取正中间的子节点
      int cmp = midVal.getName().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    // key not found
    if (exactSearch) {
      return -1;
    } else {
      return low == 0 ? low : low - 1;//
    }
  }
}
