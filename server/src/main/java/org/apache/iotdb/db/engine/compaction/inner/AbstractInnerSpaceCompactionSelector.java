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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

public abstract class AbstractInnerSpaceCompactionSelector
    extends AbstractCompactionSelector { // 空间内文件合并选择器
  protected String logicalStorageGroupName; // 存储组名
  protected String virtualStorageGroupName; // 虚拟存储组名，如0
  protected long timePartition; // 时间分区
  protected TsFileResourceList tsFileResources; // 该存储组下的该分区里的所有顺序或乱序文件Resource列表
  protected boolean sequence; // 顺序or乱序
  protected InnerSpaceCompactionTaskFactory taskFactory;
  protected TsFileManager tsFileManager; // 该虚拟存储组的文件管理器,Todo:没有用到！

  public AbstractInnerSpaceCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.timePartition = timePartition;
    this.tsFileResources = tsFileResources;
    this.tsFileManager = tsFileManager;
    this.sequence = sequence;
    this.taskFactory = taskFactory;
  }

  @Override
  public abstract boolean selectAndSubmit();
}
