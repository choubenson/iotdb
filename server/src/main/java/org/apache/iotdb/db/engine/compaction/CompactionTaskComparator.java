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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.Comparator;
import java.util.List;

public class CompactionTaskComparator implements Comparator<AbstractCompactionTask> {
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override //若结果是负数，则说明第一个参数任务o1的优先级高，否则是第二个参数任务o2的优先级高
  public int compare(AbstractCompactionTask o1, AbstractCompactionTask o2) {
    if (((o1 instanceof AbstractInnerSpaceCompactionTask)
            ^ (o2 instanceof AbstractInnerSpaceCompactionTask)) //异或运算：一个true一个false，则结果为true，否则为false
        && config.getCompactionPriority() != CompactionPriority.BALANCE) {
      // the two task is different type, and the compaction priority is not balance
      if (config.getCompactionPriority() == CompactionPriority.INNER_CROSS) {
        return o1 instanceof AbstractInnerSpaceCompactionTask ? -1 : 1;
      } else {
        return o1 instanceof AbstractCrossSpaceCompactionTask ? -1 : 1;
      }
    }
    //若两个都是空间内合并任务
    if (o1 instanceof AbstractInnerSpaceCompactionTask) {
      return compareInnerSpaceCompactionTask(
          (AbstractInnerSpaceCompactionTask) o1, (AbstractInnerSpaceCompactionTask) o2);
    } else {  //若两个都是跨空间合并任务
      return compareCrossSpaceCompactionTask(
          (AbstractCrossSpaceCompactionTask) o1, (AbstractCrossSpaceCompactionTask) o2);
    }
  }

  //空间内合并任务的比较
  private int compareInnerSpaceCompactionTask(
      AbstractInnerSpaceCompactionTask o1, AbstractInnerSpaceCompactionTask o2) {
    //若两个任务一个是顺序空间、一个是乱序空间，则顺序空间内的合并任务优先级高
    if (o1.isSequence() ^ o2.isSequence()) {
      // prioritize sequence file compaction
      return o1.isSequence() ? -1 : 1;
    }

    // if the sum of compaction count of the selected files are different
    // we prefer to execute task with smaller compaction count
    // this can reduce write amplification
    //若两个合并任务的每个文件的平均空间内合并次数不相等，则每个文件平均合并次数低的任务的优先级高
    if (((double) o1.getSumOfCompactionCount()) / o1.getSelectedTsFileResourceList().size()
        != ((double) o2.getSumOfCompactionCount()) / o2.getSelectedTsFileResourceList().size()) {
      return o1.getSumOfCompactionCount() / o1.getSelectedTsFileResourceList().size()
          - o2.getSumOfCompactionCount() / o2.getSelectedTsFileResourceList().size();
    }

    List<TsFileResource> selectedFilesOfO1 = o1.getSelectedTsFileResourceList();
    List<TsFileResource> selectedFilesOfO2 = o2.getSelectedTsFileResourceList();

    // if the number of selected files are different
    // we prefer to execute task with more files
    //若两个任务的选中待合并文件的数量不同，则文件数量多的任务的优先级高
    if (selectedFilesOfO1.size() != selectedFilesOfO2.size()) {
      return selectedFilesOfO2.size() - selectedFilesOfO1.size();
    }

    // if the size of selected files are different
    // we prefer to execute task with smaller file size
    // because small files can be compacted quickly
    //若两个任务的待合并文件的总大小不同，则待合并文件总大小较小的任务的优先级高
    if (o1.getSelectedFileSize() != o2.getSelectedFileSize()) {
      return (int) (o1.getSelectedFileSize() - o2.getSelectedFileSize());
    }

    // if the max file version of o1 and o2 are different
    // we prefer to execute task with greater file version
    // because we want to compact newly written files
    //若两个任务的待合并文件的最大version不同，则较大version的任务的优先级高
    if (o1.getMaxFileVersion() != o2.getMaxFileVersion()) {
      return o2.getMaxFileVersion() > o1.getMaxFileVersion() ? 1 : -1;
    }

    return 0;
  }

  //跨空间合并任务的比较
  private int compareCrossSpaceCompactionTask(
      AbstractCrossSpaceCompactionTask o1, AbstractCrossSpaceCompactionTask o2) {
    //若两个任务的顺序文件的数量不一样，则顺序文件数量较少的任务的优先级高
    if (o1.getSelectedSequenceFiles().size() != o2.getSelectedSequenceFiles().size()) {
      // we prefer the task with fewer sequence files
      // because this type of tasks consume fewer memory during execution
      return o1.getSelectedSequenceFiles().size() - o2.getSelectedSequenceFiles().size();
    }
    // we prefer the task with more unsequence files
    // because this type of tasks reduce more unsequence files
    return o1.getSelectedUnsequenceFiles().size() - o2.getSelectedUnsequenceFiles().size();
  }
}
