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
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * SizeTieredCompactionSelector selects files to be compacted based on the size of files. The
 * selector traverses the file list from old to new. If the size of selected files or the number of
 * select files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager. In CompactionTaskManager, tasks are ordered by {@link
 * org.apache.iotdb.db.engine.compaction.CompactionTaskComparator}. To maximize compaction
 * efficiency, selector searches compaction task from 0 compaction files(that is, file that never
 * been compacted, named level 0 file) to higher level files. If a compaction task is found in some
 * level, selector will not search higher level anymore.
 */
public class SizeTieredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTieredCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileManager,
        tsFileResources,
        sequence,
        taskFactory);
  }

  // 从该虚拟存储组下该分区的所有顺序或者乱序文件里从0层（空间内合并的层数）开始至最高层依次寻找所有文件，当文件数量或者大小到达系统预设值则放入任务队列里，然后为这些队列里的每个文件列表创建一个合并任务线程并放入合并任务管理队列里
  @Override
  public boolean selectAndSubmit() {
    LOGGER.debug(
        "{} [Compaction] SizeTiredCompactionSelector start to select, target file size is {}, "
            + "target file num is {}, current task num is {}, total task num is {}, "
            + "max task num is {}",
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize(),
        IoTDBDescriptor.getInstance().getConfig().getMaxCompactionCandidateFileNum(),
        CompactionTaskManager.currentTaskNum.get(),
        CompactionTaskManager.getInstance().getExecutingTaskCount(),
        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread());
    tsFileResources.readLock();
    PriorityQueue<Pair<List<TsFileResource>, Long>>
        taskPriorityQueue = // 合并队列，每个装该任务的所有文件组和对应文件的总大小
        new PriorityQueue<>(new SizeTieredCompactionTaskComparator());
    try {
      int maxLevel = searchMaxFileLevel(); // 获取该空间内合并文件选择器里该存储组下的所有顺序或者乱序文件的空间内合并的最大层数
      for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
        // 根据指定的层数level，获取该虚拟存储组下的该分区里的空间内合并在该层数上的所有TsFile，每当文件数量或者总文件大小到达系统预设值，则把该批文件和对应的总大小放入taskPriorityQueue待合并任务队列里。若该level层数上找到了待合并文件，则返回false，代表停止向高层level文件搜索
        if (!selectLevelTask(currentLevel, taskPriorityQueue)) { // 从第0层开始每层去寻找待合并的文件组，若找到了则不再往上层寻找
          break;
        }
      }
      while (taskPriorityQueue.size() > 0) { // 把任务队列所有的任务文件列表创建各自的合并任务并提交到队列里
        createAndSubmitTask(taskPriorityQueue.poll().left); // 根据指定文件用taskFactory创建合并任务线程，并加入合并等待队列里
      }
    } catch (Exception e) {
      LOGGER.error("Exception occurs while selecting files", e);
    } finally {
      tsFileResources.readUnlock();
    }
    return true;
  }

  // 根据指定的层数level，获取该虚拟存储组下的该分区里的空间内合并在该层数上的所有TsFile，每当文件数量或者总文件大小到达系统预设值，则把该批文件和对应的总大小放入taskPriorityQueue待合并任务队列里。若该level层数上找到了待合并文件，则返回false，代表停止向高层level文件搜索
  // 搜索第level层上的所有文件，若该层上有连续的文件满足系统预设的条件（数量超过10个或者总文件大小超过2G）则为该批文件创建一个合并任务放进taskPriorityQueue队列里，并继续搜索下一批。若在该层上搜索到至少一批以上待合并文件，则返回false（示意不再向高层搜索），否则返回true。
  private boolean selectLevelTask( // taskPriorityQueue是空间内合并任务队列，存放了每个合并任务里要合并的TsFile列表和对应的总文件大小
      int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
      throws IOException {
    boolean shouldContinueToSearch = true;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();

    for (TsFileResource currentFile : tsFileResources) {
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      // 若当前遍历的文件内空间合并次数不等于level层数，则把selectedFileList等清空，遍历下个文件
      if (currentName.getInnerCompactionCnt() != level) {
        selectedFileList.clear(); // 若该虚拟存储组下存在文件的空间内合并层数不等于level，则就把selectedFileList清空，重新计算
        selectedFileSize = 0L;
        continue;
      }
      LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
      selectedFileList.add(currentFile);
      selectedFileSize += currentFile.getTsFileSize();
      LOGGER.debug(
          "Add tsfile {}, current select file num is {}, size is {}",
          currentFile,
          selectedFileList.size(),
          selectedFileSize);
      // if the file size or file num reach threshold
      if (selectedFileSize >= targetCompactionFileSize // 若被选中的文件
          || selectedFileList.size() >= config.getMaxCompactionCandidateFileNum()) {
        // submit the task
        if (selectedFileList.size() > 1) {
          taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
        }
        selectedFileList = new ArrayList<>();
        selectedFileSize = 0L;
        shouldContinueToSearch = false;
      }
    }
    return shouldContinueToSearch;
  }

  private int searchMaxFileLevel() throws IOException { // 获取该空间内合并文件选择器里该存储组下的所有顺序或者乱序文件的空间内合并的最大层数
    int maxLevel = -1;
    Iterator<TsFileResource> iterator = tsFileResources.iterator();
    while (iterator.hasNext()) {
      TsFileResource currentFile = iterator.next();
      TsFileNameGenerator.TsFileName currentName =
          TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
      if (currentName.getInnerCompactionCnt() > maxLevel) {
        maxLevel = currentName.getInnerCompactionCnt();
      }
    }
    return maxLevel;
  }

  private boolean createAndSubmitTask(
      List<TsFileResource> selectedFileList) { // 根据指定文件用taskFactory创建合并任务线程，并加入合并等待队列里
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileManager,
            tsFileResources,
            selectedFileList,
            sequence);
    return CompactionTaskManager.getInstance().addTaskToWaitingQueue(compactionTask);
  }

  private class SizeTieredCompactionTaskComparator
      implements Comparator<Pair<List<TsFileResource>, Long>> {

    @Override
    public int compare(Pair<List<TsFileResource>, Long> o1, Pair<List<TsFileResource>, Long> o2) {
      TsFileResource resourceOfO1 = o1.left.get(0);
      TsFileResource resourceOfO2 = o2.left.get(0);
      try {
        TsFileNameGenerator.TsFileName fileNameOfO1 =
            TsFileNameGenerator.getTsFileName(resourceOfO1.getTsFile().getName());
        TsFileNameGenerator.TsFileName fileNameOfO2 =
            TsFileNameGenerator.getTsFileName(resourceOfO2.getTsFile().getName());
        //第一个待合并文件的空间内合并次数大的任务的优先级高
        if (fileNameOfO1.getInnerCompactionCnt() != fileNameOfO2.getInnerCompactionCnt()) {
          return fileNameOfO2.getInnerCompactionCnt() - fileNameOfO1.getInnerCompactionCnt();
        }
      } catch (IOException e) {
        return 0;
      }
      //选中待合并文件数量少的任务的优先级高
      if (o1.left.size() != o2.left.size()) {
        return o1.left.size() - o2.left.size();
      } else {
        //选中的待合并的文件总大小较大的任务的优先级高
        return ((int) (o2.right - o1.right));
      }
    }
  }
}
