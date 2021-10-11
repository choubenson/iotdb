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
package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.PartialPath;

public class HashVirtualPartitioner implements VirtualPartitioner { // 虚拟存储组分割器接口的Hash分割法实现类

  /** total number of virtual storage groups */
  public static int STORAGE_GROUP_NUM = // 虚拟存储组的数量
      IoTDBDescriptor.getInstance().getConfig().getVirtualStorageGroupNum();

  private HashVirtualPartitioner() {}

  public static HashVirtualPartitioner getInstance() {
    return HashVirtualPartitionerHolder.INSTANCE;
  }

  @Override
  public int deviceToVirtualStorageGroupId(PartialPath deviceId) { // 根据DeviceId去Hash得到哈希值当作虚拟存储组ID
    return toStorageGroupId(deviceId);
  }

  @Override
  public int getPartitionCount() {
    return STORAGE_GROUP_NUM;
  }

  private int toStorageGroupId(PartialPath deviceId) { // 根据DeviceId去Hash得到哈希值当作虚拟存储组ID
    return Math.abs(deviceId.hashCode() % STORAGE_GROUP_NUM); // 取绝对值
  }

  private static class HashVirtualPartitionerHolder {

    private static final HashVirtualPartitioner INSTANCE = new HashVirtualPartitioner();

    private HashVirtualPartitionerHolder() {
      // allowed to do nothing
    }
  }
}
