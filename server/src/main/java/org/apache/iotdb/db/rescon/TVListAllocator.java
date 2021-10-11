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

package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class TVListAllocator implements TVListAllocatorMBean, IService {

  private Map<TSDataType, Queue<TVList>> tvListCache = new EnumMap<>(TSDataType.class); //为每个数据类型分配了可用的TVList，因此数据结构是（数据类型，可用TVList队列）
  private String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  public static TVListAllocator getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private static final TVListAllocator INSTANCE = new TVListAllocator();

    private InstanceHolder() {}
  }

  public synchronized TVList allocate(TSDataType dataType) {  //根据数据类型返回一个新的TVList对象
    Queue<TVList> tvLists = tvListCache.computeIfAbsent(dataType, k -> new ArrayDeque<>());   //根据数据类型从tvListCache中获取对应的可用TVList队列
    TVList list = tvLists.poll(); //获取可用TVList队列中的第一个TVList,并把其从可用TVList队列中移除
    return list != null ? list : TVList.newList(dataType);
  }

  public synchronized TVList allocate(List<TSDataType> dataTypes) {
    return TVList.newVectorList(dataTypes);
  }

  /** For non-vector types. */
  public synchronized void release(TSDataType dataType, TVList list) {
    list.clear();
    if (dataType != TSDataType.VECTOR) {
      tvListCache.get(list.getDataType()).add(list);
    }
  }

  /** For VECTOR type only. */
  public synchronized void release(TVList list) {
    list.clear();
    if (list.getDataType() != TSDataType.VECTOR) {
      tvListCache.get(list.getDataType()).add(list);
    }
  }

  @Override
  public int getNumberOfTVLists() {
    int number = 0;
    for (Queue<TVList> queue : tvListCache.values()) {
      number += queue.size();
    }
    return number;
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(InstanceHolder.INSTANCE, mbeanName);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
    for (Queue<TVList> queue : tvListCache.values()) {
      queue.clear();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TVLIST_ALLOCATOR_SERVICE;
  }
}
