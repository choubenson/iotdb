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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * MaxFileMergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget. It always assume the number of timeseries being
 * queried at the same time is 1 to maximize the number of file merged.
 */
public class MaxFileMergeFileSelector implements ICrossSpaceMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(MaxFileMergeFileSelector.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  CrossSpaceMergeResource resource;

  long totalCost;
  private long memoryBudget;
  private long maxSeqFileCost;

  // the number of timeseries being queried at the same time
  int concurrentMergeNum = 1;

  /** Total metadata size of each file. */
  private Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();
  /** Maximum memory cost of querying a timeseries in each file. */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  List<TsFileResource> selectedUnseqFiles; // 选中的待被合并的乱序文件
  List<TsFileResource> selectedSeqFiles; // 选中的待被合并的顺序文件

  // 用来临时存放CrossSpaceMergeResource跨空间合并资源管理器里第几个顺序文件是与某一乱序文件有overlap的
  private Collection<Integer> tmpSelectedSeqFiles;
  private long tempMaxSeqFileCost;

  private boolean[] seqSelected; // 用来标记CrossSpaceMergeResource跨空间合并资源管理器里的每个顺序文件是否被选中
  private int seqSelectedNum;

  public MaxFileMergeFileSelector(CrossSpaceMergeResource resource, long memoryBudget) {
    this.resource = resource;
    this.memoryBudget = memoryBudget;
  }

  /**
   * Select merge candidates from seqFiles and unseqFiles under the given memoryBudget. This process
   * iteratively adds the next unseqFile from unseqFiles and its overlapping seqFiles as newly-added
   * candidates and computes their estimated memory cost. If the current cost pluses the new cost is
   * still under the budget, accept the unseqFile and the seqFiles as candidates, otherwise go to
   * the next iteration. The memory cost of a file is calculated in two ways: The rough estimation:
   * for a seqFile, the size of its metadata is used for estimation. Since in the worst case, the
   * file only contains one timeseries and all its metadata will be loaded into memory with at most
   * one actual data chunk (which is negligible) and writing the timeseries into a new file generate
   * metadata of the similar size, so the size of all seqFiles' metadata (generated when writing new
   * chunks) pluses the largest one (loaded when reading a timeseries from the seqFiles) is the
   * total estimation of all seqFiles; for an unseqFile, since the merge reader may read all chunks
   * of a series to perform a merge read, the whole file may be loaded into memory, so we use the
   * file's length as the maximum estimation. The tight estimation: based on the rough estimation,
   * we scan the file's metadata to count the number of chunks for each series, find the series
   * which have the most chunks in the file and use its chunk proportion to refine the rough
   * estimation. The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   *
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is selected
   *     unseqFiles or an empty array if there are no proper candidates by the budget.
   */
  // 针对每个乱序文件查找与其Overlap且还未被此次合并任务选中的顺序文件列表，每找到一个乱序文件及其对应的Overlap顺序文件列表后就预估他们进行合并可能增加的额外内存开销，若未超过系统给合并线程预设的内存开销，则把他们放入到此合并任务选中的顺序和乱序文件里。并更新该虚拟存储组下该时间分区的跨空间合并资源管理器里的顺序文件和乱序文件列表，移除其未被选中的文件的SequenceReader,清缓存
  @Override
  public List[] select() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.info(
          "Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(),
          resource.getUnseqFiles().size());
      // 针对每个乱序文件查找与其Overlap且还未被此次合并任务选中的顺序文件列表，每找到一个乱序文件及其对应的Overlap顺序文件列表后就预估他们进行合并可能增加的额外内存开销，若未超过系统给合并线程预设的内存开销，则把他们放入到此合并任务选中的顺序和乱序文件里
      // 优先使用宽松法估计内存原因是速度快，可是估计的偏大，不准确，可能导致估计的内存太大超过系统给合并任务设定的内存预值，因此该合并任务就没有一个被选中待合并的乱序和对应Overlap的顺序文件。若发生这种情况，再使用严格法估计内存来挑选待合并文件，它所估计的内存精确到具体要访问几个序列，较精确，可是要访问文件磁盘上每个序列的具体Chunk数量，速度较慢。
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      // 在确定了此次合并任务的所有被选中待合并的顺序和乱序文件后，更新该虚拟存储组下该时间分区的跨空间合并资源管理器里的顺序文件和乱序文件列表
      resource.setSeqFiles(selectedSeqFiles);
      resource.setUnseqFiles(selectedUnseqFiles);
      // 移除未被选中的文件的SequenceReader,清缓存
      resource.removeOutdatedSeqReaders();
      if (selectedUnseqFiles.isEmpty()) {
        logger.info("No merge candidates are found");
        return new List[0];
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(),
          selectedUnseqFiles.size(),
          totalCost,
          System.currentTimeMillis() - startTime);
    }
    return new List[] {selectedSeqFiles, selectedUnseqFiles};
  }

  // 针对每个乱序文件查找与其Overlap且还未被此次合并任务选中的顺序文件列表，每找到一个乱序文件及其对应的Overlap顺序文件列表后就预估他们进行合并可能增加的额外内存开销，若未超过系统给合并线程预设的内存开销，则把他们放入到此合并任务选中的顺序和乱序文件里
  void select(boolean useTightBound)
      throws IOException { // 参数代表计算并估计每个乱序和对应顺序文件的内存开销的方法，true代表严格估计，false代表轻松估计。
    tmpSelectedSeqFiles = new HashSet<>();
    seqSelected = new boolean[resource.getSeqFiles().size()];
    seqSelectedNum = 0; // 目前已经选中的待合并顺序文件的数量
    selectedSeqFiles = new ArrayList<>();
    selectedUnseqFiles = new ArrayList<>();
    maxSeqFileCost = 0;
    tempMaxSeqFileCost = 0;

    totalCost = 0;

    int unseqIndex = 0; // 乱序文件索引
    long startTime = System.currentTimeMillis();
    long timeConsumption = 0;
    // 系统预设的跨空间的选择文件所能用的最大时间（为30秒）
    long timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    // 若乱序文件索引还未超过乱序文件数量 且 耗时未超过系统预设，则
    while (unseqIndex < resource.getUnseqFiles().size() && timeConsumption < timeLimit) {
      // select next unseq files  ,遍历获取下一个乱序文件
      TsFileResource unseqFile = resource.getUnseqFiles().get(unseqIndex);

      if (seqSelectedNum != resource.getSeqFiles().size()) {
        // 将与该unseqFile乱序文件有Overlap的并且还未被此次合并任务选中的顺序文件的索引放入tmpSelectedSeqFiles列表里。具体判断是否有OVerlap的做法是：依次遍历获取乱序文件的每个设备ChunkGroup，判断所有还未被此次合并任务选中的顺序文件的该设备ChunkGroup是否有与乱序的ChunkGroup重叠，有的话则选中此顺序文件。
        selectOverlappedSeqFiles(unseqFile);
      }
      // 检查判断该乱序文件以及与该乱序文件有Overlap的所有顺序文件是否关闭且不在Merging
      boolean isClosed = checkClosedAndNotMerging(unseqFile);
      // 若该乱序文件或者与其Overlap的某一顺序文件未关闭或正在merging，则放弃他们
      if (!isClosed) {
        tmpSelectedSeqFiles.clear();
        unseqIndex++;
        timeConsumption = System.currentTimeMillis() - startTime;
        continue;
      }

      tempMaxSeqFileCost = maxSeqFileCost;
      // 此乱序文件与对应的Overlap顺序文件列表进行合并预估会增加的内存开销：宽松估计出的大小会偏大（误差大），而严格估计出的大小精确到具体合并的有多少序列，大小较精确
      long newCost =
          useTightBound
              ? calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles, startTime, timeLimit)
              : calculateLooseMemoryCost(unseqFile, tmpSelectedSeqFiles, startTime, timeLimit);
      // 判断此乱序文件对应的Overlap顺序文件进行合并会新增的内存开销加上原有的其他乱序文件进行合并的开销是否超过系统给合并线程预设的内存开销，若不超过，则把选中乱序文件和顺序文件的索引放入全局对象列表里
      if (!updateSelectedFiles(newCost, unseqFile)) {
        // older unseq files must be merged before newer ones
        break;
      }

      tmpSelectedSeqFiles.clear();
      unseqIndex++;
      // 目前已使用的时间
      timeConsumption = System.currentTimeMillis() - startTime;
    }
    // 最后根据待合并的顺序文件索引把对应的顺序文件加入待合并顺序文件列表里
    for (int i = 0; i < seqSelected.length; i++) {
      if (seqSelected[i]) {
        selectedSeqFiles.add(resource.getSeqFiles().get(i));
      }
    }
  }

  // 判断此乱序文件对应的Overlap顺序文件进行合并会新增的内存开销加上原有的其他乱序文件进行合并的开销是否超过系统给合并线程预设的内存开销，若不超过，则把选中乱序文件和顺序文件的索引放入全局对象列表里
  private boolean updateSelectedFiles(long newCost, TsFileResource unseqFile) {
    // 若已有的内存+新增的内存开销<系统给每个合并线程允许的内存开销，则把选中乱序文件和顺序文件的索引放入全局对象列表里
    if (totalCost + newCost < memoryBudget) {
      selectedUnseqFiles.add(unseqFile);
      maxSeqFileCost = tempMaxSeqFileCost;

      for (Integer seqIdx : tmpSelectedSeqFiles) {
        seqSelected[seqIdx] = true;
        seqSelectedNum++;
      }
      totalCost += newCost;
      logger.debug(
          "Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
              + " cost {}",
          unseqFile,
          tmpSelectedSeqFiles,
          newCost,
          totalCost);
      return true;
    }
    return false;
  }

  // 检查判断该乱序文件以及与该乱序文件有Overlap的所有顺序文件是否关闭且不在Merging
  private boolean checkClosedAndNotMerging(TsFileResource unseqFile) {
    boolean isClosedAndNotMerging = unseqFile.isClosed() && !unseqFile.isMerging();
    if (!isClosedAndNotMerging) {
      return false;
    }
    for (Integer seqIdx : tmpSelectedSeqFiles) {
      if (!resource.getSeqFiles().get(seqIdx).isClosed()
          || resource.getSeqFiles().get(seqIdx).isMerging()) {
        isClosedAndNotMerging = false;
        break;
      }
    }
    return isClosedAndNotMerging;
  }

  // 将与该unseqFile乱序文件有Overlap的并且还未被此次合并任务选中的顺序文件的索引放入tmpSelectedSeqFiles列表里。具体判断是否有OVerlap的做法是：依次遍历获取乱序文件的每个设备ChunkGroup，判断所有还未被此次合并任务选中的顺序文件的该设备ChunkGroup是否有与乱序的ChunkGroup重叠，有的话则选中此顺序文件。
  private void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    // 与该unseqFile乱序文件有Overlap的顺序文件的数量
    int tmpSelectedNum = 0;
    // 遍历该乱序文件里的每个设备ID
    for (String deviceId : unseqFile.getDevices()) {
      long unseqStartTime = unseqFile.getStartTime(deviceId); // 该设备ChunkGroup的起始时间
      long unseqEndTime = unseqFile.getEndTime(deviceId); // 该设备ChunkGroup的结束时间

      // 代表该乱序文件的该设备ChunkGroup是否还可能与后续的顺序文件的该ChunkGroup有Overlap
      boolean noMoreOverlap = false;
      for (int i = 0; i < resource.getSeqFiles().size() && !noMoreOverlap; i++) {
        // 循环遍历每个顺序文件
        TsFileResource seqFile = resource.getSeqFiles().get(i);
        // 若该顺序文件已经被选中待合并或者该顺序文件里不包含此乱序文件的设备ID，则遍历下个顺序文件
        if (seqSelected[i] || !seqFile.getDevices().contains(deviceId)) {
          continue;
        }
        // the open file's endTime is Long.MIN_VALUE, this will make the file be filtered below
        // 获取该顺序文件的该设备ID的结束时间。若该顺序文件还未封口，则结束时间为Long型最大值
        long seqEndTime = seqFile.isClosed() ? seqFile.getEndTime(deviceId) : Long.MAX_VALUE;
        if (unseqEndTime <= seqEndTime) {
          // the unseqFile overlaps current seqFile
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
          // the device of the unseqFile can not merge with later seqFiles
          // 由于每个连续的顺序TsFile他们是不Overlap的，因此当此乱序文件的该设备ChunkGroup时间小于当前遍历到的顺序文件的该ChunkGroup（则说明与该顺序文件有Overlap），也一定小于后面的其他顺序文件的该设备ChunkGroup的startTime,因此后面顺序文件与该乱序文件的该设备ChunkGroup不会再有Overlap了
          noMoreOverlap = true;
        } else if (unseqStartTime <= seqEndTime) {
          // 此处是该乱序的该设备结束时间unseqEndTime>seqEndTime，但是unseqStartTime又<=seqEndTime,因此该乱序文件的该设备ChunkGroup与当前顺序文件的该ChunkGroup是有OverLap，与后面的其他顺序文件还可能有Overlap
          // the device of the unseqFile may merge with later seqFiles
          // and the unseqFile overlaps current seqFile
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
        }
      }
      // 若当前与该乱序文件有Overlap的顺序文件数量+已选中待合并的顺序文件数量=总的顺序文件数量，则退出
      if (tmpSelectedNum + seqSelectedNum == resource.getSeqFiles().size()) {
        break;
      }
    }
  }

  // 计算此次合并中可能占用的内存大小：宽松估计出的大小会偏大（误差大），而严格估计出的大小精确到具体合并的有多少序列，大小较精确
  // (1)对于宽松估计：乱序文件占用的大小（即整个乱序文件大小，因为可能会读取出乱序文件里的所有Chunk）+顺序文件占用的大小（由于每次只读取一个顺序文件，因此cost记录顺序文件里最大的metadata大小；写入的时候每个文件都会有metadata,因此cost还要把所有顺序文件的metadata加起来）
  // (2)对于严格估计：计算获取查询该乱序文件里concurrentMergeNum个时间序列共要花费的可能最大内存空间，即（总文件大小*MaxChunkNum/totalChunkNum）；计算获取查询该顺序文件里concurrentMergeNum个时间序列共要花费的可能最大内存空间，即（总索引区大小*MaxChunkNum序列最多含有的Chunk数量/totalChunkNum所有序列Chunk数量和）
  private long calculateMemoryCost(
      TsFileResource tmpSelectedUnseqFile, // 乱序文件
      Collection<Integer> tmpSelectedSeqFiles, // 与乱序文件Overlap的顺序文件索引
      IFileQueryMemMeasurement unseqMeasurement,
      IFileQueryMemMeasurement seqMeasurement,
      long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    // 获取乱序文件的文件长度
    Long fileCost = unseqMeasurement.measure(tmpSelectedUnseqFile);
    cost += fileCost;

    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      // 遍历每个Overlap的顺序文件
      TsFileResource seqFile = resource.getSeqFiles().get(seqFileIdx);
      // 获取该顺序文件的索引区元数据的大小
      fileCost = seqMeasurement.measure(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileCost;
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile); // cost+该顺序文件的索引区元数据的大小 //Todo:??应该只会生成一个新的顺序文件
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    return cost;
  }

  // 计算此次合并中可能占用的内存大小：宽松估计出的大小会偏大（误差大）
  private long calculateLooseMemoryCost(
      TsFileResource tmpSelectedUnseqFile, // 乱序文件
      Collection<Integer> tmpSelectedSeqFiles, // 与乱序文件Overlap的顺序文件索引
      long startTime,
      long timeLimit)
      throws IOException {
    // 计算此次合并中可能占用的内存大小：宽松估计出的大小会偏大（误差大），而严格估计出的大小精确到具体合并的有多少序列，大小较精确
    return calculateMemoryCost(
        tmpSelectedUnseqFile,
        tmpSelectedSeqFiles,
        TsFileResource::getTsFileSize,
        this::calculateMetadataSize,
        startTime,
        timeLimit);
  }

  // 计算此次合并中可能占用的内存大小：严格估计出的大小精确到具体合并的有多少序列，大小较精确
  private long calculateTightMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      long startTime,
      long timeLimit)
      throws IOException {
    return calculateMemoryCost(
        tmpSelectedUnseqFile,
        tmpSelectedSeqFiles,
        this::calculateTightUnseqMemoryCost,
        this::calculateTightSeqMemoryCost,
        startTime,
        timeLimit);
  }

  // 获取该顺序文件的索引区元数据的大小
  private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
    // 获取该顺序文件的索引区元数据的大小
    Long cost = fileMetaSizeMap.get(seqFile);
    if (cost == null) {
      // 获取该文件的索引区的大小
      cost = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
      fileMetaSizeMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  // 计算获取指定顺序文件里查询某一时间序列可能花费的最大内存空间
  private long calculateTightFileMemoryCost(
      TsFileResource seqFile, IFileQueryMemMeasurement measurement) throws IOException {
    // 获取查询此顺序文件里某个时间序列可能花费的最大内存空间（这也只是估计出来的）
    Long cost = maxSeriesQueryCostMap.get(seqFile);
    if (cost == null) {
      // 获取该TsFile里所有时间序列的Chunk的总数量和以及单时间序列的最大Chunk数量
      long[] chunkNums =
          MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
      long totalChunkNum = chunkNums[0]; // 该文件的总Chunk数量
      long maxChunkNum = chunkNums[1]; // 该文件某序列的最大数量
      // 假设该文件里每个时间序列都有maxChunkNum个Chunk，则每个时间序列最大占用的内存（顺序文件则是索引metadata占用内存，而乱序文件则是整个文件大小）大小就是如下cost
      cost = measurement.measure(seqFile) * maxChunkNum / totalChunkNum;
      maxSeriesQueryCostMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion to all series to get a maximum estimation
  // 计算获取查询该顺序文件里concurrentMergeNum个时间序列共要花费的可能最大内存空间
  private long calculateTightSeqMemoryCost(TsFileResource seqFile) throws IOException {
    // 计算获取指定顺序文件里查询某一时间序列可能花费的最大内存空间,即（总索引区大小*MaxChunkNum序列最多含有的Chunk数量/totalChunkNum所有序列Chunk数量和）
    long singleSeriesCost = calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
    // 查询该顺序文件里concurrentMergeNum个时间序列共要花费的可能最大内存空间
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    // 获取该顺序文件的索引区元数据的大小
    long maxCost = calculateMetadataSize(seqFile);
    // 查询的最坏情况是该文件里所有Chunk的索引都要被读出来占用内存，因此选取小的
    return Math.min(multiSeriesCost, maxCost);
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  // 计算获取查询该乱序文件里concurrentMergeNum个时间序列共要花费的可能最大内存空间
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
    // 计算获取指定顺序文件里查询某一时间序列可能花费的最大内存空间，即（总文件大小*MaxChunkNum/totalChunkNum）
    long singleSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getTsFileSize);
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    // 乱序文件的文件大小
    long maxCost = unseqFile.getTsFileSize();
    return Math.min(multiSeriesCost, maxCost);
  }

  @Override
  public int getConcurrentMergeNum() {
    return concurrentMergeNum;
  }
}
