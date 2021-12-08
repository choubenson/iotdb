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

package org.apache.iotdb.db.engine.compaction.cross.inplace.selector;

import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.List;
import java.util.NoSuchElementException;

public class NaivePathSelector implements IMergePathSelector {

  //某设备下的所有时间序列路径
  private List<PartialPath> paths;
  private int idx;
  //跨空间合并，并行合并序列允许的最大数量
  private int maxSeriesNum;

  public NaivePathSelector(List<PartialPath> paths, int maxSeriesNum) {
    this.paths = paths;
    this.maxSeriesNum = maxSeriesNum;
  }

  //判断是否还有下个时间序列
  @Override
  public boolean hasNext() {
    return idx < paths.size();
  }

  //获取下批时间序列路径，数量为maxSeriesNum个
  @Override
  public List<PartialPath> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    List<PartialPath> ret =
        idx + maxSeriesNum <= paths.size()
            ? paths.subList(idx, idx + maxSeriesNum)
            : paths.subList(idx, paths.size());
    idx += maxSeriesNum;
    return ret;
  }
}
