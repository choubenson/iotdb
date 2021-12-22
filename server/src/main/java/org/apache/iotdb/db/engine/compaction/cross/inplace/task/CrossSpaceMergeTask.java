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

package org.apache.iotdb.db.engine.compaction.cross.inplace.task;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeContext;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.InplaceCompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * CrossSpaceMergeTask merges given seqFiles and unseqFiles into new ones, which basically consists
 * of three steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge files 2.
 * move the merged chunks in the temp files back to the seqFiles or move the unmerged chunks in the
 * seqFiles into temp files and replace the seqFiles with the temp files. 3. remove unseqFiles
 */
public class CrossSpaceMergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge";
  private static final Logger logger = LoggerFactory.getLogger(CrossSpaceMergeTask.class);

  // 跨空间合并的资源管理器，里面存放了待被合并的顺序和乱序文件
  CrossSpaceMergeResource resource;
  String storageGroupSysDir;
  //logicalStorageGroupName
  String storageGroupName;
  InplaceCompactionLogger inplaceCompactionLogger;
  //跨空间合并的上下文
  CrossSpaceMergeContext mergeContext = new CrossSpaceMergeContext();
  //跨空间 允许并行合并序列的线程数量
  int concurrentMergeSeriesNum;
  String taskName;
  //是否是fullMerge
  boolean fullMerge;
  //跨空间合并的状态
  States states = States.START;
  MergeMultiChunkTask chunkTask;
  MergeFileTask fileTask;
  private final MergeCallback callback;

  CrossSpaceMergeTask(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      MergeCallback callback,
      String taskName,
      boolean fullMerge,
      String storageGroupName) {
    this.resource = new CrossSpaceMergeResource(seqFiles, unseqFiles);
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.concurrentMergeSeriesNum = 1;
    this.storageGroupName = storageGroupName;
  }

  public CrossSpaceMergeTask(
      CrossSpaceMergeResource mergeResource,
      String storageGroupSysDir,
      MergeCallback callback,
      String taskName,
      boolean fullMerge,
      int concurrentMergeSeriesNum,
      String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      abort();
    }
    return null;
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp(false);
    // call the callback to make sure the StorageGroup exit merging status, but passing 2
    // empty file lists to avoid files being deleted.
    callback.call(
        Collections.emptyList(),
        Collections.emptyList(),
        new File(storageGroupSysDir, InplaceCompactionLogger.MERGE_LOG_NAME));
  }

  //该合并任务线程执行的内容：
  //1. 在该元数据MTree树下根据前缀获取该存储组下的所有传感器的<完整路径,MeasurementSchema>，并将该存储组下的所有序列视为待被合并的序列
  //2. 创建合并Chunk的工具对象，并使用该工具开始执行合并所有待合并序列，依次对一批批待合并序列分别将其在乱序文件里的数据和某个顺序文件里的数据写到一个个顺序文件的对应临时目标文件.tsfile.merge里 （至此，有用的数据存在于顺序文件和对应的临时目标文件里）
      /**
       * 1. 对所有的待合并序列进行按设备分组，依次遍历每个设备的一组序列：
       *    1）对该设备下的一组序列路径 和 允许获取的最大序列数量（目前为1） 去创建序列选择器
       *    2）使用该选择器循环获取该设备的下一批待合并序列（数量为预设的，目前为1），令他们为当前的待合并序列
       *       (1)开始合并当前的待合并序列：
       *    * 1. 往日志里写入当前正准备被合并的所有时间序列路径
       *    * 2. 获取当前每个待合并序列的乱序阅读器，即首先获取每个序列在所有待合并乱序文件里的所有Chunk，并以此创建每个序列的乱序数据点读取器，该读取器有一个数据点优先级队列，它存放了该序列在所有乱序文件里的数据点，每次拿去优先级最高的数据点，时间戳小的Element数据点对应的优先级高；时间戳一样时，越新的乱序文件里的数据点的优先级越高，若在同一个乱序文件里，则offset越大的数据点说明越新，则优先级越高。
       *    * 3. 初始化当前每个待合并序列的当前数据点数组
       *    * 4. 遍历每个待合并顺序文件，执行以下操作：
       *    *      * 1. 获取当前待合并顺序文件以及相关信息（包括当前待合并序列的设备ID）
       *    *      * 2. 初始化每个待合并序列在当前待合并文件里的所有ChunkMetadataList，放入seqChunkMeta数组（搜索该待合并文件里是否存在待合并序列的方法为：依次遍历该顺序文件的该设备的一个个TimeseriesMetadata节点上的每一个currSensor序列，若currSensor为当前待合并序列，则初始化其删除数据和ChunkMetadataList，若currSensor已经超过待合并序列里的最大maxId，则停止该顺序文件，因为泽嵩树上measurementId是按字典序从小到大排序）
       *    *      *    2.1 初始化当前待合并顺序文件下每个传感器ID和对应的ChunkMetadata列表的遍历器，使用该遍历器可以获取该文件的该设备在泽嵩树的下一个TimeseriesMetadata节点上的一批传感器ID和对应的ChunkMetadataList
       *    *      *    2.2 获取待合并序列里最大的传感器measurementId
       *    *      *    2.3 若为第一次循环（即currSensor为null）或者currSensor的id小于待合并序列里最大的传感器measurementId，则继续循环：
       *    *      *        2.3.1 初始化measurementChunkMetadataListMap，使用遍历器往里放入当前待合并顺序文件的该设备在泽嵩树的下一个TimeseriesMetadata节点上的所有一条条TimeseriesMetadata对应传感器measurementId和各自对应的ChunkMetadata列表
       *    *      *        2.3.2 循环遍历当前待合并顺序文件里当前设备在泽嵩树上该TimeseriesMetadata节点上的 一个个“measurementId”和“对应的ChunkMetadataList”：
       *    *      *              2.3.2.1 若当前遍历到的currSensor是在待合并序列里，则将其在该顺序待合并文件里的ChunkMetadataList和modifications放入对应位置的数组里
       *    *      *              2.3.2.2 若该待合并顺序文件的当前TimeseriesMetadata节点上的该currSensor传感器大于所有待合并序列中的maxId，则跳出此循环；若小于等于maxId，则从相应的遍历器移除此传感器
       *    *      *    2.4 更新chunkMetadataListCacheForMerge变量（该变量存放当前待合并顺序文件里当前设备在泽嵩树上某一个TimeseriesMetadata节点上的所有剩余的大于lastSensor的measurementId和对应的ChunkMetadataList）
       *    *      *    2.5 遍历每个待合并序列，判断它们是否存在于当前待合并顺序文件里（若不存在则过滤掉，返回未被过滤的序列的索引列表），若该顺序文件里不存在任何的当前待合并序列，则直接返回。（注意：若seqChunkMeta某个位置为空列表则说明该文件不存在该待合并序列，则该位置不被计入返回值里；若当前是最后一个顺序待合并文件且某不存在某序列，可是待合并乱序文件里存在，则不会过滤掉此序列）
       *    *      *    //注意：可能出现乱序文件里该设备下有一个新的sensor序列，而所有的待合并序列都不存在该设备下的该sensor序列，因此在合并的时候是把该设备的该乱序新序列合并在最后的时候写到临时目标文件的ChunkWriter里，然后flush到目标文件的内容结尾。即乱序新文件是写到目标文件的最后。
       *    *      * 3. 获取或者创建临时目标文件的写入TsFileIOWriter。注意：跨空间合并的临时目标文件是"顺序文件名.tsfile.merge" ，即xxx.tsfile.merge！！
       *    *      * 4. 开启一个新的设备ChunkGroup，并把该新的ChunkGroupHeader的内容（marker（为0）和设备ID）写入该TsFileIOWriter对象的TsFileOutput写入对象的输出流BufferedOutputStream的缓存数组里
       *    *      * 5. 对该待合并顺序文件里存在的待合并序列执行合并：
       *    *      *      * 1，创建系统预设数量（4个）的创建子合并任务线程（一个待合并顺序文件可能对应好几个该子合并线程，每个子线程用于合并不同的待合并序列，即把该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并），将当前所有的待合并序列平均分散地放入到每个合并子任务里的优先级队列里。（若待合并序列数量少于4个，则创建的子合并任务数量为序列数量即可）
       *    *      *      * 2. 该待合并顺序文件的所有合并子任务开始并行执行，即将该待合并顺序文件里的所有数据和在优先级队列的所有时间序列进行合并写入到临时目标文件，具体操作如下：
       *    *      *      *    依优先级从队列里遍历每个待合并序列：
       *    *      *      *    1）遍历该序列在该待合并顺序文件里的每个Chunk:
       *    *      *      *       (1) 获取该序列的顺序chunk的一些相关属性，如对应的chunkMetadata、是否是该文件的最后一个chunk、该序列该所有待合并乱序里是否有数据点与该顺序chunk overlap、该chunk是否数据点太少等
       *    *      *      *       (2) 接着开始对该序列顺序chunk进行合并重写，即把该序列的当前顺序Chunk重写到目标文件的TsFileIOWriter的缓存里，返回还在目标文件该序列的chunkWriter里未被刷盘的数据点数量，分3种情况：
       *    *      *      *           (2.1) 若不是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则不需要进行重写，直接返回0
       *    *      *      *           (2.2) 若是fullMerger && 该序列目标文件不存在unclose Chunk && 当前顺序Chunk足够大 && 当前序列在乱序文件里不存在数据与顺序Chunk有overlap && 当前顺序chunk没有数据被删除，则直接把当前Chunk整个写到目标文件的TsFileIOWriter的缓存里，并返回0
       *    *      *      *           (2.3)
       *    *      *      *                (2.3.1）若当前序列在乱序文件里不存在数据与顺序Chunk有overlap，将该序列的该顺序Chunk的所有满足条件（未被删除）的数据点依次写入临时目标文件的该序列的chunkWriter里，并返回写入的数据点数量
       *    *      *      *                (2.3.2）若当前序列在乱序文件里存在数据与顺序Chunk有overlap，则将该待合并顺序文件该Chunk和该序列在所有乱序文件里的数据，按时间戳递增顺序依次写入目标文件的该序列的ChunkWriter里，最后写入的数据点的时间戳小于等于TimeLimit（该顺序Chunk的最大的结束时间戳），返回写入目标文件的数据点数量。具体：
       *    *      *      *                     （2.3.2.1）遍历该待合并顺序文件该Chunk的每个page的每个数据点，依次把该序列在所有乱序文件中小于等于当前数据点时间戳的所有数据点写入目标文件(该序列在所有乱序文件中是按照优先级读取出每个数据点的)，若乱序文件没有与当前顺序数据点时间戳相同的数据则把当前顺序数据点写入目标文件的chunkWriter里（否则直接写如乱序文件里的该时间戳数据点即可，因为同个时间戳，乱序文件里的数据是比较新的）
       *    *      *      *                     （2.3.2.2）可能出现该顺序文件的该序列的该Chunk的最后一些数据被删除了，因此要往目标文件的该序列的ChunkWriter里写入该序列在所有乱序文件里时间戳小于timeLimit的所有数据点。
       *    *      *      *               (2.2.3) 若该序列的顺序chunk在目标文件的还未刷盘的ChunkWriter里数据点数量大于等于 系统预设chunk最少数据点数量100000，则将该ChunkWriter缓存里的数据刷到目标文件的TsFileIOWriter里的缓存，并返回0；否则返回还在目标文件该序列的ChunkWriter里未被刷的数据点数量
       *    *      *      *       (3) 当已经是最后一个待合并顺序文件且完成该顺序文件里该序列数据的重写合并了（其实此处没有写入），可是该序列仍然存在乱序数据未被写入，说明所有待合并顺序文件都不存在该序列，则把该序列在乱序文件里的所有数据点先写到对应目标文件ChunkWriter里（下一步再追加写入目标文件的TsFileIOWriter里）
       *    *      *      *       (4) flush当前目标文件里该序列对应ChunkWriter的缓存到目标文件的TsFileIOWriter里
       *    *      *      * 3. 等待所有的子合并任务执行完毕，返回是否合并成功，若有该顺序文件有一个chunk被合并，则成功。
       *    *      * 6. 若合并成功，则：
       *    *      *    6.1 结束currentChunkGroupDeviceId对应ChunkGroup，即把其ChunkGroupMeatadata元数据加入该写入类的chunkGroupMetadataList列表缓存里，并把该写入类的相关属性（设备ID和ChunkMetadataList清空），并将该TsFile的TsFileIOWriter对象的输出缓存流TsFileOutput的内容给flush到本地对应TsFile文件
       *    *      *    6.2 往日志里写目标文件目前的文件长度是多少
       *    *      *    6.3 更新当前被合并源顺序文件的该设备的起始时间
       *    * 5. 往合并日志里写入end，代表结束当前这些序列的合并
       *       (2)输出当前的合并进度
       *     3) 清除该设备下的相关内存
       * 2，往日志里写入all ts en。此刻，应该生成了多个临时目标文件，即每个待合并顺序文件会对应一个临时目标文件 xxx.tsfile.merge，每个临时目标文件里存放了 对应的顺序文件里存在的待合并序列的合并重写后的有序数据，而最后一个临时目标文件里还夹杂存放了所有乱序新序列的有序新数据
       */
  //3. 创建合并文件的工具对象，遍历每个已合并完的顺序文件：
  //   （1）若该旧文件已合并的Chunk数量大于等于未被合并的Chunk数量，则将旧的合并完后的顺序文件里尚未被合并的序列的所有Chunk数据写到临时目标文件里（给旧文件里每个未被合并的序列在临时目标文件里新开一个ChunkGroup然后顺序写入该未被合并序列的每个Chunk，然后关闭该ChunkGroup），删除旧的顺序文件，并把临时目标tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
  //   （2）若该旧文件已合并的Chunk数量小于未被合并的Chunk数量，则过滤掉旧顺序文件里已被合并的Chunk，并把新的临时目标文件里的每个设备ChunkGroup下的所有Chunk写到旧文件里（对每个设备新开ChunkGroup），并把旧tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
  //    注意：此方法执行完后，所有的已合并完的旧顺序文件会被删除
  //4. 做一些善后工作：
  //   1）清除内存
  //   2）清除每个顺序文件的临时目标文件
  //   3）把所有的顺序和乱序文件的TsFileResource setMerging(false)
  //   4）若参数为true，则执行回调函数：
  //    （1）删除此次合并任务里所有乱序文件对应的本地tsfile文件、.resource文件和.mods文件
  //    （2）遍历每个待合并顺序文件：（1）删除该顺序文件的临时目标文件（若合并过程出问题，则该顺序文件对应的临时目标文件可能没有被删除）（2）删除该已合并顺序文件的原有mods文件，并先后往合并过程中对该顺序文件和所有乱序文件产生的删除操作写到该已合并顺序文件的新mods文件
  //    （3）删除所有合并完的顺序和乱序文件对应的.compaction.mods文件
  //    （4）删除合并日志
  //   5）若参数为false，则只删除合并日志
  private void doMerge() throws IOException, MetadataException {
    //若待被合并顺序文件数量为0，则
    if (resource.getSeqFiles().isEmpty()) {
      logger.info("{} no sequence file to merge into, so will abort task.", taskName);
      abort();
      return;
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} starts to merge seq files {}, unseq files {}",
          taskName,
          resource.getSeqFiles(),
          resource.getUnseqFiles());
    }
    long startTime = System.currentTimeMillis();
    //获取顺序和乱序文件的总文件大小
    long totalFileSize =
        MergeUtils.collectFileSizes(resource.getSeqFiles(), resource.getUnseqFiles());
    //跨空间合并的日志类，用于后续写入日志
    inplaceCompactionLogger = new InplaceCompactionLogger(storageGroupSysDir);

    //往跨空间合并日志里依次写入待合并顺序和乱序文件的重要属性
    inplaceCompactionLogger.logFiles(resource);

    //在该元数据MTree树下根据前缀获取该存储组下的所有传感器的<完整路径,MeasurementSchema>
    Map<PartialPath, IMeasurementSchema> measurementSchemaMap =
        IoTDB.metaManager.getAllMeasurementSchemaByPrefix(new PartialPath(storageGroupName));
    //该存储组下的所有时间序列完整路径，他们被视为是尚未被合并的序列
    List<PartialPath> unmergedSeries = new ArrayList<>(measurementSchemaMap.keySet());
    resource.setMeasurementSchemaMap(measurementSchemaMap);

    //写入日志，代表开始跨空间合并
    inplaceCompactionLogger.logMergeStart();

    //创建合并多个Chunk的工具类
    chunkTask =
        new MergeMultiChunkTask(
            mergeContext,
            taskName,
            inplaceCompactionLogger,
            resource, //跨空间合并资源管理器
            fullMerge,
            unmergedSeries,
            concurrentMergeSeriesNum,
            storageGroupName);
    //跨空间合并状态
    states = States.MERGE_CHUNKS;
    chunkTask.mergeSeries();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }
    //合并文件工具类
    fileTask =
        new MergeFileTask(
            taskName, mergeContext, inplaceCompactionLogger, resource, resource.getSeqFiles());
    states = States.MERGE_FILES;
    chunkTask = null;
    //遍历每个已合并完的顺序文件：
    //（1）若该旧文件已合并的Chunk数量大于等于未被合并的Chunk数量，则将旧的合并完后的顺序文件里尚未被合并的序列的所有Chunk数据写到临时目标文件里（给旧文件里每个未被合并的序列在临时目标文件里新开一个ChunkGroup然后顺序写入该未被合并序列的每个Chunk，然后关闭该ChunkGroup），删除旧的顺序文件，并把临时目标tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
    //（2）若该旧文件已合并的Chunk数量小于未被合并的Chunk数量，则过滤掉旧顺序文件里已被合并的Chunk，并把新的临时目标文件里的每个设备ChunkGroup下的所有Chunk写到旧文件里（对每个设备新开ChunkGroup），并把旧tsfile文件和对应的.resource文件移动到最终目标文件(文件名里跨空间合并次数比旧文件增加了1)和对应新.resource文件（此处新resource文件更新了每个设备ID对应的起始和结束时间）。
    // 注意：此方法执行完后，所有的已合并完的旧顺序文件会被删除
    fileTask.mergeFiles();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    states = States.CLEAN_UP;
    fileTask = null;
    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double seriesRate = unmergedSeries.size() / elapsedTime;
      double chunkRate = mergeContext.getTotalChunkWritten() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info(
          "{} ends after {}s, byteRate: {}MB/s, seriesRate {}/s, chunkRate: {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName,
          elapsedTime,
          byteRate,
          seriesRate,
          chunkRate,
          fileRate,
          ptRate);
    }
  }

  //1.清除内存
  //2.清除每个顺序文件的临时目标文件
  //3.把所有的顺序和乱序文件的TsFileResource setMerging(false)
  //4.若参数为true，则执行回调函数：
  //（1）删除此次合并任务里所有乱序文件对应的本地tsfile文件、.resource文件和.mods文件
  //（2）遍历每个待合并顺序文件：（1）删除该顺序文件的临时目标文件（若合并过程出问题，则该顺序文件对应的临时目标文件可能没有被删除）（2）删除该已合并顺序文件的原有mods文件，并先后往合并过程中对该顺序文件和所有乱序文件产生的删除操作写到该已合并顺序文件的新mods文件
  //（3）删除所有合并完的顺序和乱序文件对应的.compaction.mods文件
  //（4）删除合并日志
  //5.若参数为false，则只删除合并日志
  void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();
    mergeContext.clear();

    if (inplaceCompactionLogger != null) {
      inplaceCompactionLogger.close();
    }

    //删除每个顺序文件对应的本地临时目标文件.tsfile.merge
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getTsFilePath() + MERGE_SUFFIX);
      mergeFile.delete();
      seqFile.setMerging(false);
    }
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      unseqFile.setMerging(false);
    }

    File logFile = new File(storageGroupSysDir, InplaceCompactionLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile);
    } else {
      logFile.delete();
    }
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public String getProgress() {
    switch (states) {
      case ABORTED:
        return "Aborted";
      case CLEAN_UP:
        return "Cleaning up";
      case MERGE_FILES:
        return "Merging files: " + fileTask.getProgress();
      case MERGE_CHUNKS:
        return "Merging series: " + chunkTask.getProgress();
      case START:
      default:
        return "Just started";
    }
  }

  public String getTaskName() {
    return taskName;
  }

  enum States {
    START,
    MERGE_CHUNKS,
    MERGE_FILES,
    CLEAN_UP,
    ABORTED
  }
}
