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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionMergeTaskPoolManager;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.task.RecoverMergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEngine;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.TEMP_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * For sequence data, a StorageGroupProcessor has some TsFileProcessors, in which there is only one
 * TsFileProcessor in the working status. <br>
 *
 * <p>There are two situations to set the working TsFileProcessor to closing status:<br>
 *
 * <p>(1) when inserting data into the TsFileProcessor, and the TsFileProcessor shouldFlush() (or
 * shouldClose())<br>
 *
 * <p>(2) someone calls syncCloseAllWorkingTsFileProcessors(). (up to now, only flush command from
 * cli will call this method)<br>
 *
 * <p>UnSequence data has the similar process as above.
 *
 * <p>When a sequence TsFileProcessor is submitted to be flushed, the
 * updateLatestFlushTimeCallback() method will be called as a callback.<br>
 *
 * <p>When a TsFileProcessor is closed, the closeUnsealedTsFileProcessorCallBack() method will be
 * called as a callback.
 */
public class StorageGroupProcessor {//每个虚拟存储组对应一个StorageGroupProcessor

  public static final String MERGING_MODIFICATION_FILE_NAME = "merge.mods";
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  /**
   * All newly generated chunks after merge have version number 0, so we set merged Modification
   * file version to 1 to take effect
   */
  private static final int MERGE_MOD_START_VERSION_NUM = 1;

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupProcessor.class);
  /** indicating the file to be loaded already exists locally. */
  private static final int POS_ALREADY_EXIST = -2;
  /** indicating the file to be loaded overlap with some files. */
  private static final int POS_OVERLAP = -3;

  private final boolean enableMemControl = config.isEnableMemControl();
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * partitionLatestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /** closeStorageGroupCondition is used to wait for all currently closing TsFiles to be done. */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /** time partition id in the storage group -> tsFileProcessor for this time partition */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();//是个键值对结构数据，存放了所有工作的顺序TsFileProcessor，存放的是该工作TsFile的所属时间分区和对应TsFileProcessor
  /** time partition id in the storage group -> tsFileProcessor for this time partition */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();//是个键值对结构数据，存放了每个时间分区对应的乱序工作TsFileProcessor
  /** compactionMergeWorking is used to wait for last compaction to be done. */
  private volatile boolean compactionMergeWorking = false;
  // upgrading sequence TsFile resource list
  private List<TsFileResource> upgradeSeqFileList = new LinkedList<>(); //待升级的顺序TSFile文件的TsFileResource类对象列表

  /** sequence tsfile processors which are closing */
  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor =    //存放着那些已经被关闭的顺序TsFileProcessor
      new CopyOnReadLinkedList<>();

  // upgrading unsequence TsFile resource list
  private List<TsFileResource> upgradeUnseqFileList = new LinkedList<>();//待升级的乱序TSFile文件的TsFileResource类对象列表

  /** unsequence tsfile processors which are closing */
  private CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor =   //存放着那些已经被关闭的乱序TsFileProcessor
      new CopyOnReadLinkedList<>();

  private AtomicInteger upgradeFileCount = new AtomicInteger();//当前虚拟存储组目录下待升级的旧的TSFile文件数量（包括顺序和乱序）
  /*
   * time partition id -> map, which contains
   * device -> global latest timestamp of each device latestTimeForEachDevice caches non-flushed
   * changes upon timestamps of each device, and is used to update partitionLatestFlushedTimeForEachDevice
   * when a flush is issued.
   */
  private Map<Long, Map<String, Long>> latestTimeForEachDevice = new HashMap<>();//该存储组下每一个时间分区的每一个设备的写入数据的最大时间戳（包括未flush和已经flush的数据时间戳）
  /**
   * time partition id -> map, which contains device -> largest timestamp of the latest memtable to
   * be submitted to asyncTryToFlush partitionLatestFlushedTimeForEachDevice determines whether a
   * data point should be put into a sequential file or an unsequential file. Data of some device
   * with timestamp less than or equals to the device's latestFlushedTime should go into an
   * unsequential file.
   */
  private Map<Long, Map<String, Long>> partitionLatestFlushedTimeForEachDevice = new HashMap<>();//记录该存储组下每一个时间分区下的每一个设备已刷盘数据的最大时间戳，以此确定该设备后面插入的新数据点是顺序还是乱序数据，当某个设备的待插入数据的时间戳小于等于该设备的lastestFlushedTime最后刷盘数据最大时间戳，则该待插入数据应该被刷盘放入乱序文件里

  /** used to record the latest flush time while upgrading and inserting */
  private Map<Long, Map<String, Long>> newlyFlushedPartitionLatestFlushedTimeForEachDevice =
      new HashMap<>(); //记录升级文件成功后，每一个新TsFile文件所属时间分区的每个设备的最大时间戳
  /**
   * global mapping of device -> largest timestamp of the latest memtable to * be submitted to
   * asyncTryToFlush, globalLatestFlushedTimeForEachDevice is utilized to maintain global
   * latestFlushedTime of devices and will be updated along with
   * partitionLatestFlushedTimeForEachDevice
   */
  private Map<String, Long> globalLatestFlushedTimeForEachDevice = new HashMap<>();

  /** virtual storage group id */
  private String virtualStorageGroupId;

  /** logical storage group name */
  private String logicalStorageGroupName;

  /** storage group system directory */
  private File storageGroupSysDir;

  /** manage seqFileList and unSeqFileList */
  private TsFileManagement tsFileManagement;//每个虚拟存储组有一个TsFile管理器类对象，负责管理该虚拟存储组下所有的顺序和乱序TsFile，他还可以获取该虚拟存储组下的所有TsFile
  /**
   * time partition id -> version controller which assigns a version for each MemTable and
   * deletion/update such that after they are persisted, the order of insertions, deletions and
   * updates can be re-determined. Will be empty if there are not MemTables in memory.
   */
  private HashMap<Long, VersionController> timePartitionIdVersionControllerMap = new HashMap<>(); //（时间分区ID，版本控制对象）
  /**
   * when the data in a storage group is older than dataTTL, it is considered invalid and will be
   * eventually removed.
   */
  private long dataTTL = Long.MAX_VALUE;

  /** file system factory (local or hdfs) */
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  /** file flush policy */
  private TsFileFlushPolicy fileFlushPolicy;

  /**
   * The max file versions in each partition. By recording this, if several IoTDB instances have the
   * same policy of closing file and their ingestion is identical, then files of the same version in
   * different IoTDB instance will have identical data, providing convenience for data comparison
   * across different instances. partition number -> max version number
   */
  private Map<Long, Long> partitionMaxFileVersions = new HashMap<>();

  /** storage group info for mem control */
  private StorageGroupInfo storageGroupInfo = new StorageGroupInfo(this);
  /**
   * Record the device number of the last TsFile in each storage group, which is applied to
   * initialize the array size of DeviceTimeIndex. It is reasonable to assume that the adjacent
   * files should have similar numbers of devices. Default value: INIT_ARRAY_SIZE = 64
   */
  private int deviceNumInLastClosedTsFile = DeviceTimeIndex.INIT_ARRAY_SIZE;

  /** whether it's ready from recovery */
  private boolean isReady = false;

  /** close file listeners */
  private List<CloseFileListener> customCloseFileListeners = Collections.emptyList();

  /** flush listeners */
  private List<FlushListener> customFlushListeners = Collections.emptyList();

  private static final int WAL_BUFFER_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2;

  private final Deque<ByteBuffer> walByteBufferPool = new LinkedList<>();

  private int currentWalPoolSize = 0;

  // this field is used to avoid when one writer release bytebuffer back to pool,
  // and the next writer has already arrived, but the check thread get the lock first, it find the
  // pool
  // is not empty, so it free the memory. When the next writer get the lock, it will apply the
  // memory again.
  // So our free memory strategy is only when the expected size less than the current pool size
  // and the pool is not empty and the time interval since the pool is not empty is larger than
  // DEFAULT_POOL_TRIM_INTERVAL_MILLIS
  private long timeWhenPoolNotEmpty = Long.MAX_VALUE;

  /**
   * record the insertWriteLock in SG is being hold by which method, it will be empty string if on
   * one holds the insertWriteLock
   */
  private String insertWriteLockHolder = "";

  /** get the direct byte buffer from pool, each fetch contains two ByteBuffer */
  public ByteBuffer[] getWalDirectByteBuffer() {
    ByteBuffer[] res = new ByteBuffer[2];
    synchronized (walByteBufferPool) {
      long startTime = System.nanoTime();
      int MAX_WAL_BYTEBUFFER_NUM =
          config.getConcurrentWritingTimePartition()
              * config.getMaxWalBytebufferNumForEachPartition();
      while (walByteBufferPool.isEmpty() && currentWalPoolSize + 2 > MAX_WAL_BYTEBUFFER_NUM) {
        try {
          walByteBufferPool.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error(
              "getDirectByteBuffer occurs error while waiting for DirectByteBuffer" + "group {}-{}",
              logicalStorageGroupName,
              virtualStorageGroupId,
              e);
        }
        logger.info(
            "Waiting {} ms for wal direct byte buffer.",
            (System.nanoTime() - startTime) / 1_000_000);
      }
      // If the queue is not empty, it must have at least two.
      if (!walByteBufferPool.isEmpty()) {
        res[0] = walByteBufferPool.pollFirst();
        res[1] = walByteBufferPool.pollFirst();
      } else {
        // if the queue is empty and current size is less than MAX_BYTEBUFFER_NUM
        // we can construct another two more new byte buffer
        currentWalPoolSize += 2;
        res[0] = ByteBuffer.allocateDirect(WAL_BUFFER_SIZE);
        res[1] = ByteBuffer.allocateDirect(WAL_BUFFER_SIZE);
      }
      // if the pool is empty, set the time back to MAX_VALUE
      if (walByteBufferPool.isEmpty()) {
        timeWhenPoolNotEmpty = Long.MAX_VALUE;
      }
    }
    return res;
  }

  /** put the byteBuffer back to pool */
  public void releaseWalBuffer(ByteBuffer[] byteBuffers) {
    for (ByteBuffer byteBuffer : byteBuffers) {
      byteBuffer.clear();
    }
    synchronized (walByteBufferPool) {
      // if the pool is empty before, update the time
      if (walByteBufferPool.isEmpty()) {
        timeWhenPoolNotEmpty = System.nanoTime();
      }
      walByteBufferPool.addLast(byteBuffers[0]);
      walByteBufferPool.addLast(byteBuffers[1]);
      walByteBufferPool.notifyAll();
    }
  }

  /** trim the size of the pool and release the memory of needless direct byte buffer */
  private void trimTask() {
    synchronized (walByteBufferPool) {
      int expectedSize =
          (workSequenceTsFileProcessors.size() + workUnsequenceTsFileProcessors.size()) * 2;
      // the unit is ms
      long poolNotEmptyIntervalInMS = (System.nanoTime() - timeWhenPoolNotEmpty) / 1_000_000;
      // only when the expected size less than the current pool size
      // and the pool is not empty and the time interval since the pool is not empty is larger than
      // 10s
      // we will trim the size to expectedSize until the pool is empty
      while (expectedSize < currentWalPoolSize
          && !walByteBufferPool.isEmpty()
          && poolNotEmptyIntervalInMS >= config.getWalPoolTrimIntervalInMS()) {
        MmapUtil.clean((MappedByteBuffer) walByteBufferPool.removeLast());
        MmapUtil.clean((MappedByteBuffer) walByteBufferPool.removeLast());
        currentWalPoolSize -= 2;
      }
    }
  }

  /**
   * constrcut a storage group processor
   *
   * @param systemDir system dir path
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param fileFlushPolicy file flush policy
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public StorageGroupProcessor(
      String systemDir,
      String virtualStorageGroupId,
      TsFileFlushPolicy fileFlushPolicy,
      String logicalStorageGroupName)
      throws StorageGroupProcessorException {
    this.virtualStorageGroupId = virtualStorageGroupId;//该存储组所属的虚拟存储组ID
    this.logicalStorageGroupName = logicalStorageGroupName; //该存储组的逻辑名
    this.fileFlushPolicy = fileFlushPolicy; //flush策略

    storageGroupSysDir = SystemFileFactory.INSTANCE.getFile(systemDir, virtualStorageGroupId);  //该虚拟存储组目录文件
    if (storageGroupSysDir.mkdirs()) {  //若本地不存在该存储组的目录则进行创建
      logger.info(
          "Storage Group system Directory {} doesn't exist, create it",
          storageGroupSysDir.getPath());
    } else if (!storageGroupSysDir.exists()) {
      logger.error("create Storage Group system Directory {} failed", storageGroupSysDir.getPath());
    }
    this.tsFileManagement =       //获取该虚拟存储组对应的TSFile管理器对象
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCompactionStrategy()
            .getTsFileManagement(logicalStorageGroupName, storageGroupSysDir.getAbsolutePath());

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleWithFixedDelay(
        this::trimTask,
        config.getWalPoolTrimIntervalInMS(),
        config.getWalPoolTrimIntervalInMS(),
        TimeUnit.MILLISECONDS);
    recover();    //
  }

  public String getLogicalStorageGroupName() {
    return logicalStorageGroupName;
  }

  public boolean isReady() {
    return isReady;
  }

  public void setReady(boolean ready) {
    isReady = ready;
  }

  private Map<Long, List<TsFileResource>> splitResourcesByPartition(  //对给定的TsFileResource按照时间分区进行分组，（时间分区ID，TsFileResource对象列表）
      List<TsFileResource> resources) {
    Map<Long, List<TsFileResource>> ret = new HashMap<>();
    for (TsFileResource resource : resources) {
      ret.computeIfAbsent(resource.getTimePartition(), l -> new ArrayList<>()).add(resource);
    }
    return ret;
  }

  /** recover from file */
  private void recover() throws StorageGroupProcessorException {    //开始根据本地文件夹和文件里存储的数据恢复此虚拟存储组的StorageGroupProcessor的相关属性值，即deserialize
    logger.info(
        String.format(
            "start recovering virtual storage group %s[%s]",
            logicalStorageGroupName, virtualStorageGroupId));

    try {
      // collect candidate TsFiles from sequential and unsequential data directory
      Pair<List<TsFileResource>, List<TsFileResource>> seqTsFilesPair =
          getAllFiles(DirectoryManager.getInstance().getAllSequenceFileFolders());//根据给定的顺序TsFile文件目录路径列表(目前folders列表只有一个元素，eg:"data/data/sequence")，获取该目录下所有时间分区目录下的TSFile对应的TSFileResource文件对象放入第一个List里，并获取该目录下的upgrade目录下的待升级TsFile文件对应的TsFileResource，把他们反序列化后放入第二个列表List里。
      List<TsFileResource> tmpSeqTsFiles = seqTsFilesPair.left;//获取该顺序文件目录下所有时间分区目录下的TSFile对应的TSFileResource文件对象
      List<TsFileResource> oldSeqTsFiles = seqTsFilesPair.right;//获取该顺序文件目录下的upgrade目录下的待升级TsFile文件对应的TsFileResource，把他们反序列化后放入的列表
      upgradeSeqFileList.addAll(oldSeqTsFiles);//把该顺序文件目录下的upgrade目录下的所有待升级TsFile文件对应的经反序列化的TsFileResource加入待升级的顺序TSFile文件的TsFileResource类对象列表
      Pair<List<TsFileResource>, List<TsFileResource>> unseqTsFilesPair =   //对给定的乱序文件目录"data/data/unsequence"，获取其下所有的TSFile的TsFileResource和待升级TSFile的TsFileResource
          getAllFiles(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      List<TsFileResource> tmpUnseqTsFiles = unseqTsFilesPair.left;
      List<TsFileResource> oldUnseqTsFiles = unseqTsFilesPair.right;
      upgradeUnseqFileList.addAll(oldUnseqTsFiles);//把该乱序文件目录下的upgrade目录下的所有待升级TsFile文件对应的经反序列化的TsFileResource加入待升级的乱序TSFile文件的TsFileResource类对象列表

      if (upgradeSeqFileList.size() + upgradeUnseqFileList.size() != 0) { //如果待升级的顺序和乱序的TSFile对应的TsFileResource数量不为0
        upgradeFileCount.set(upgradeSeqFileList.size() + upgradeUnseqFileList.size());//设置待升级的TSFile文件数量
      }

      // split by partition so that we can find the last file of each partition and decide to
      // close it or not
      Map<Long, List<TsFileResource>> partitionTmpSeqTsFiles =  //每个时间分区对应着多个顺序TsFile的TsFileResource
          splitResourcesByPartition(tmpSeqTsFiles);   //将所有顺序TSFile的TsFileResource按照时间分区进行分组，时间分区ID，TsFileResource对象列表）
      Map<Long, List<TsFileResource>> partitionTmpUnseqTsFiles =
          splitResourcesByPartition(tmpUnseqTsFiles);//将所有乱序TSFile的TsFileResource按照时间分区进行分组，时间分区ID，TsFileResource对象列表）
      for (List<TsFileResource> value : partitionTmpSeqTsFiles.values()) {  //循环遍历每个时间分区的顺序TsFileResoure列表
        recoverTsFiles(value, true);  //恢复相关TSFile
      }
      for (List<TsFileResource> value : partitionTmpUnseqTsFiles.values()) {
        recoverTsFiles(value, false);
      }

      String taskName =
          logicalStorageGroupName + "-" + virtualStorageGroupId + "-" + System.currentTimeMillis();
      File mergingMods =
          SystemFileFactory.INSTANCE.getFile(storageGroupSysDir, MERGING_MODIFICATION_FILE_NAME);
      if (mergingMods.exists()) {
        this.tsFileManagement.mergingModification = new ModificationFile(mergingMods.getPath());
      }
      RecoverMergeTask recoverMergeTask =
          new RecoverMergeTask(
              new ArrayList<>(tsFileManagement.getTsFileList(true)),
              tsFileManagement.getTsFileList(false),
              storageGroupSysDir.getPath(),
              tsFileManagement::mergeEndAction,
              taskName,
              IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
              logicalStorageGroupName);
      logger.info(
          "{} - {} a RecoverMergeTask {} starts...",
          logicalStorageGroupName,
          virtualStorageGroupId,
          taskName);
      recoverMergeTask.recoverMerge(
          IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
      if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
        mergingMods.delete();
      }
      recoverCompaction();
      for (TsFileResource resource : tsFileManagement.getTsFileList(true)) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      for (TsFileResource resource : tsFileManagement.getTsFileList(false)) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      for (TsFileResource resource : upgradeSeqFileList) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      for (TsFileResource resource : upgradeUnseqFileList) {
        long partitionNum = resource.getTimePartition();
        updatePartitionFileVersion(partitionNum, resource.getVersion());
      }
      updateLatestFlushedTime();
    } catch (IOException | MetadataException e) {
      throw new StorageGroupProcessorException(e);
    }

    List<TsFileResource> seqTsFileResources = tsFileManagement.getTsFileList(true);
    for (TsFileResource resource : seqTsFileResources) {
      long timePartitionId = resource.getTimePartition();
      Map<String, Long> endTimeMap = new HashMap<>();
      for (String deviceId : resource.getDevices()) {
        long endTime = resource.getEndTime(deviceId);
        endTimeMap.put(deviceId, endTime);
      }
      latestTimeForEachDevice
          .computeIfAbsent(timePartitionId, l -> new HashMap<>())
          .putAll(endTimeMap);
      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(timePartitionId, id -> new HashMap<>())
          .putAll(endTimeMap);
      globalLatestFlushedTimeForEachDevice.putAll(endTimeMap);
    }

    if (IoTDBDescriptor.getInstance().getConfig().isEnableContinuousCompaction()
        && seqTsFileResources.size() > 0) {
      for (long timePartitionId : timePartitionIdVersionControllerMap.keySet()) {
        executeCompaction(
            timePartitionId, IoTDBDescriptor.getInstance().getConfig().isForceFullMerge());
      }
    }

    logger.info(
        String.format(
            "the virtual storage group %s[%s] is recovered successfully",
            logicalStorageGroupName, virtualStorageGroupId));
  }

  private void recoverCompaction() {
    if (!CompactionMergeTaskPoolManager.getInstance().isTerminated()) {
      compactionMergeWorking = true;
      logger.info(
          "{} - {} submit a compaction recover merge task",
          logicalStorageGroupName,
          virtualStorageGroupId);
      try {
        CompactionMergeTaskPoolManager.getInstance()
            .submitTask(
                logicalStorageGroupName,
                tsFileManagement.new CompactionRecoverTask(this::closeCompactionMergeCallBack));
      } catch (RejectedExecutionException e) {
        this.closeCompactionMergeCallBack(false, 0);
        logger.error(
            "{} - {} compaction submit task failed",
            logicalStorageGroupName,
            virtualStorageGroupId,
            e);
      }
    } else {
      logger.error(
          "{} compaction pool not started ,recover failed",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
    }
  }

  private void updatePartitionFileVersion(long partitionNum, long fileVersion) {
    long oldVersion = partitionMaxFileVersions.getOrDefault(partitionNum, 0L);
    if (fileVersion > oldVersion) {
      partitionMaxFileVersions.put(partitionNum, fileVersion);
    }
  }

  /**
   * use old seq file to update latestTimeForEachDevice, globalLatestFlushedTimeForEachDevice,
   * partitionLatestFlushedTimeForEachDevice and timePartitionIdVersionControllerMap
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void updateLatestFlushedTime() throws IOException {

    VersionController versionController =
        new SimpleFileVersionController(storageGroupSysDir.getPath());
    long currentVersion = versionController.currVersion();
    for (TsFileResource resource : upgradeSeqFileList) {
      for (String deviceId : resource.getDevices()) {
        long endTime = resource.getEndTime(deviceId);
        long endTimePartitionId = StorageEngine.getTimePartition(endTime);
        latestTimeForEachDevice
            .computeIfAbsent(endTimePartitionId, l -> new HashMap<>())
            .put(deviceId, endTime);
        globalLatestFlushedTimeForEachDevice.put(deviceId, endTime);

        // set all the covered partition's LatestFlushedTime
        long partitionId = StorageEngine.getTimePartition(resource.getStartTime(deviceId));
        while (partitionId <= endTimePartitionId) {
          partitionLatestFlushedTimeForEachDevice
              .computeIfAbsent(partitionId, l -> new HashMap<>())
              .put(deviceId, endTime);
          if (!timePartitionIdVersionControllerMap.containsKey(partitionId)) {
            File directory =
                SystemFileFactory.INSTANCE.getFile(storageGroupSysDir, String.valueOf(partitionId));
            if (!directory.exists()) {
              directory.mkdirs();
            }
            File versionFile =
                SystemFileFactory.INSTANCE.getFile(
                    directory, SimpleFileVersionController.FILE_PREFIX + currentVersion);
            if (!versionFile.createNewFile()) {
              logger.warn("Version file {} has already been created ", versionFile);
            }
            timePartitionIdVersionControllerMap.put(
                partitionId,
                new SimpleFileVersionController(storageGroupSysDir.getPath(), partitionId));
          }
          partitionId++;
        }
      }
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private Pair<List<TsFileResource>, List<TsFileResource>> getAllFiles(List<String> folders)
      throws IOException, StorageGroupProcessorException {//根据给定的目录路径列表(目前folders列表只有一个元素，eg:"data/data/sequence")，获取该目录下所有时间分区目录下的TSFile对应的TSFileResource文件对象放入第一个List里，并获取该目录下的upgrade目录下的待升级TsFile文件对应的TsFileResource，把他们反序列化后放入第二个列表List里。
    List<File> tsFiles = new ArrayList<>();   //文件类列表，用来存放TsFile文件对象
    List<File> upgradeFiles = new ArrayList<>();  //文件类列表，用来存放待升级的TSFile文件对象
    for (String baseDir : folders) {  //遍历给定目录列表(目前folders列表只有一个元素，eg:"data/data/sequence")，找出其下的所有虚拟存储组目录的时间分区目录和升级目录下是否存在TSFile文件，分别一起放入TSFile文件列表tsFiles和待升级TSFile文件列表upgradeFiles
      File fileFolder = //获取该存储组的该虚拟存储组的目录文件File对象。即此处获取到的File对象其实是一个虚拟存储组目录
          fsFactory.getFile(
              baseDir + File.separator + logicalStorageGroupName, virtualStorageGroupId);
      if (!fileFolder.exists()) {
        continue;
      }

      // old version
      // some TsFileResource may be being persisted when the system crashed, try recovering such
      // resources
      continueFailedRenames(fileFolder, TEMP_SUFFIX); //判断该fileFolder虚拟存储组目录下是否存在tmp后缀的文件（如example.tmp），若存在则：(1) 若存在不含后缀的同名文件example，则把example.tmp本地文件删掉（2）否则把example.tmp文件重命名为example文件

      // some TsFiles were going to be replaced by the merged files when the system crashed and
      // the process was interrupted before the merged files could be named
      continueFailedRenames(fileFolder, MERGE_SUFFIX);  //判断该fileFolder虚拟存储组目录下是否存在merge后缀的文件（如example.merge），若存在则：(1) 若存在不含后缀的同名文件example，则把example.merge本地文件删掉（2）否则把example.merge文件重命名为example文件

      File[] subFiles = fileFolder.listFiles(); //获取该虚拟存储组目录下的子目录或子文件
      if (subFiles != null) {
        for (File partitionFolder : subFiles) { //遍历该虚拟存储组目录下的子文件
          if (!partitionFolder.isDirectory()) { //如果不是个目录
            logger.warn("{} is not a directory.", partitionFolder.getAbsolutePath());
          } else if (!partitionFolder.getName().equals(IoTDBConstant.UPGRADE_FOLDER_NAME)) {  //如果该该虚拟存储组目录下的该文件是个目录并且名不是为"upgrade"，此时一般是时间分区ID目录
            // some TsFileResource may be being persisted when the system crashed, try recovering
            // such
            // resources
            continueFailedRenames(partitionFolder, TEMP_SUFFIX);//判断该时间分区目录下是否存在tmp后缀的文件（如example.tmp），若存在则：(1) 若存在不含后缀的同名文件example，则把example.tmp本地文件删掉（2）否则把example.tmp文件重命名为example文件

            // some TsFiles were going to be replaced by the merged files when the system crashed
            // and
            // the process was interrupted before the merged files could be named
            continueFailedRenames(partitionFolder, MERGE_SUFFIX);//判断该时间分区目录下是否存在merge后缀的文件（如example.merge），若存在则：(1) 若存在不含后缀的同名文件example，则把example.merge本地文件删掉（2）否则把example.merge文件重命名为example文件

            Collections.addAll(
                tsFiles,
                fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), TSFILE_SUFFIX));//获取该时间分区目录下的所有.tsfile文件对象，并把把这些文件对象放入TSFile文件列表tsFiles里
          } else {  //若该虚拟存储组目录下的该文件是个目录并且名为"upgrade"，即该虚拟存储组目录下存在升级目录
            // collect old TsFiles for upgrading
            Collections.addAll(
                upgradeFiles,
                fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), TSFILE_SUFFIX)); //获取该虚拟存储组下的升级目录下的所有.tsfile文件对象，并把把这些文件对象放入待升级的TsFile文件列表upgradeFiles里
          }
        }
      }
    }

    tsFiles.sort(this::compareFileName);  //对TSFile文件按照名称里的时间进行从小到大排序
    if (!tsFiles.isEmpty()) { //若TSFile文件列表不空，则
      checkTsFileTime(tsFiles.get(tsFiles.size() - 1));//用于判断最后一个（即时间最大的）TSFile文件的时间是否小于系统当前时间，若大于系统时间则说明存储时出了问题，会抛出异常
    }
    //若TsFile的时间没有抛出异常，则继续下面的操作
    List<TsFileResource> ret = new ArrayList<>();
    tsFiles.forEach(f -> ret.add(new TsFileResource(f))); //为该虚拟存储组下所有时间分区目录的TsFile创建对应的TsFileResource对象并放入ret列表里

    upgradeFiles.sort(this::compareFileName);//对待升级文件列表里的TSFile文件按照文件名里的时间进行从小到大排序
    if (!upgradeFiles.isEmpty()) {
      checkTsFileTime(upgradeFiles.get(upgradeFiles.size() - 1));//用于判断最后一个（即时间最大的）TSFile文件的时间是否小于系统当前时间，若大于系统时间则说明存储时出了问题，会抛出异常
    }
    List<TsFileResource> upgradeRet = new ArrayList<>();  //用于存放该虚拟存储组下的待升级upgrade目录下的TsFile对应的TsFileResource对象
    for (File f : upgradeFiles) { //循环遍历带升级TSFile
      TsFileResource fileResource = new TsFileResource(f); //创建其对应的TsFileResource
      fileResource.setClosed(true); //把该TsFileResource设为封口，说明该TsFile是关闭封口的
      // make sure the flush command is called before IoTDB is down.
      fileResource.deserializeFromOldFile();  //反序列化
      upgradeRet.add(fileResource);//把该待升级的TSFile的TsFileResource对象加入列表
    }
    return new Pair<>(ret, upgradeRet);
  }

  private void continueFailedRenames(File fileFolder, String suffix) {  //判断该fileFolder目录下是否存在suffix后缀的文件（如example.suffix），若存在则：(1) 若存在不含后缀的同名文件example，则把example.suffix本地文件删掉（2）否则把example.suffix文件重命名为example文件
    File[] files = fsFactory.listFilesBySuffix(fileFolder.getAbsolutePath(), suffix);//获取该fileFolder目录下所有以suffix后缀（如.tmp）结尾的子文件
    if (files != null) {//若该目录下存在此后缀名的文件，则
      for (File tempResource : files) {//循环遍历该fileFolder目录下所有以suffix后缀（如.tmp）结尾的这些子文件
        File originResource = fsFactory.getFile(tempResource.getPath().replace(suffix, ""));//获取同路径名的不包含后缀名的文件，比如该fileFolder目录下存在tempResource所代表的example.tmp文件，则此originResource存放的就是不含后缀的example文件。
        if (originResource.exists()) {//若该目录下也存在example文件，则把包含后缀名的example.tmp文件给删除掉；
          tempResource.delete();
        } else {    //否则把example.tmp文件重命名为example文件
          tempResource.renameTo(originResource);
        }
      }
    }
  }

  /** check if the tsfile's time is smaller than system current time */
  private void checkTsFileTime(File tsFile) throws StorageGroupProcessorException { //用于判断TSFile文件的时间是否小于系统当前时间，若大于系统时间则说明存储时出了问题，会抛出异常
    String[] items = tsFile.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long fileTime = Long.parseLong(items[0]);
    long currentTime = System.currentTimeMillis();
    if (fileTime > currentTime) {
      throw new StorageGroupProcessorException(
          String.format(
              "virtual storage group %s[%s] is down, because the time of tsfile %s is larger than system current time, "
                  + "file time is %d while system current time is %d, please check it.",
              logicalStorageGroupName,
              virtualStorageGroupId,
              tsFile.getAbsolutePath(),
              fileTime,
              currentTime));
    }
  }

  private void recoverTsFiles(List<TsFileResource> tsFiles, boolean isSeq) {  //恢复TsFile
    for (int i = 0; i < tsFiles.size(); i++) {  //循环遍历给定的TsFileResource
      TsFileResource tsFileResource = tsFiles.get(i);
      long timePartitionId = tsFileResource.getTimePartition(); //获取时间分区ID

      TsFileRecoverPerformer recoverPerformer =
          new TsFileRecoverPerformer(
              logicalStorageGroupName
                  + File.separator
                  + virtualStorageGroupId
                  + FILE_NAME_SEPARATOR,
              tsFileResource,
              isSeq,
              i == tsFiles.size() - 1);

      RestorableTsFileIOWriter writer;
      try {
        // this tsfile is not zero level, no need to perform redo wal
        if (LevelCompactionTsFileManagement.getMergeLevel(tsFileResource.getTsFile()) > 0) {
          writer =
              recoverPerformer.recover(false, this::getWalDirectByteBuffer, this::releaseWalBuffer);
          if (writer.hasCrashed()) {
            tsFileManagement.addRecover(tsFileResource, isSeq);
          } else {
            tsFileResource.setClosed(true);
            tsFileManagement.add(tsFileResource, isSeq);
          }
          continue;
        } else {
          writer =
              recoverPerformer.recover(true, this::getWalDirectByteBuffer, this::releaseWalBuffer);
        }
      } catch (StorageGroupProcessorException e) {
        logger.warn(
            "Skip TsFile: {} because of error in recover: ", tsFileResource.getTsFilePath(), e);
        continue;
      }

      if (i != tsFiles.size() - 1 || !writer.canWrite()) {
        // not the last file or cannot write, just close it
        tsFileResource.setClosed(true);
      } else if (writer.canWrite()) {
        // the last file is not closed, continue writing to in
        TsFileProcessor tsFileProcessor;
        if (isSeq) {
          tsFileProcessor =
              new TsFileProcessor(
                  virtualStorageGroupId,
                  storageGroupInfo,
                  tsFileResource,
                  this::closeUnsealedTsFileProcessorCallBack,
                  this::updateLatestFlushTimeCallback,
                  true,
                  writer);
          if (enableMemControl) {
            TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
            tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
            this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
          }
          workSequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
        } else {
          tsFileProcessor =
              new TsFileProcessor(
                  virtualStorageGroupId,
                  storageGroupInfo,
                  tsFileResource,
                  this::closeUnsealedTsFileProcessorCallBack,
                  this::unsequenceFlushCallback,
                  false,
                  writer);
          if (enableMemControl) {
            TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
            tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
            this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
          }
          workUnsequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
        }
        tsFileResource.setProcessor(tsFileProcessor);
        tsFileResource.removeResourceFile();
        tsFileProcessor.setTimeRangeId(timePartitionId);
        writer.makeMetadataVisible();
        if (enableMemControl) {
          // get chunkMetadata size
          long chunkMetadataSize = 0;
          for (Map<String, List<ChunkMetadata>> metaMap : writer.getMetadatasForQuery().values()) {
            for (List<ChunkMetadata> metadatas : metaMap.values()) {
              for (ChunkMetadata chunkMetadata : metadatas) {
                chunkMetadataSize += chunkMetadata.calculateRamSize();
              }
            }
          }
          tsFileProcessor.getTsFileProcessorInfo().addTSPMemCost(chunkMetadataSize);
        }
      }
      tsFileManagement.add(tsFileResource, isSeq);
    }
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  private int compareFileName(File o1, File o2) { //比较两个TsFile文件名是否相同，相同则返回0
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }

  /**
   * insert one row of data
   *
   * @param insertRowPlan one row of data
   */
  public void insert(InsertRowPlan insertRowPlan)
      throws WriteProcessException, TriggerExecutionException {
    // reject insertions that are out of ttl
    if (!isAlive(insertRowPlan.getTime())) {  //当待插入的数据仍在其所属存储组设定的TTL生存时间内才允许插入，否则拒绝并抛出异常
      throw new OutOfTTLException(insertRowPlan.getTime(), (System.currentTimeMillis() - dataTTL));
    }
    writeLock("InsertRow"); //获取写锁
    try {
      // init map
      long timePartitionId = StorageEngine.getTimePartition(insertRowPlan.getTime()); //根据当前插入数据的时间获取其所在的时间分区

      //获取该存储组下指定时间分区下的每一个设备已刷盘数据的最大时间戳（时间分区,(设备名称,已刷盘数据的最大时间戳)）//当partitionLatestFlushedTimeForEachDevice在该时间分区key下的value为空或者不存在该key，则初始化一个（timePartitionId,new HashMap()）键值对
      partitionLatestFlushedTimeForEachDevice.computeIfAbsent(    //该方法是jdk8中对map类新增的方法，第一个参数是key，第二个参数是一个方法，即"参数->方法名()"等同于"方法名(参数)"
          timePartitionId, id -> new HashMap<>());  // 此处是根据timePartitionId这个key来获取value值，当map中不存在此键值对，则把第二个参数方法的返回值当作value，把这个键值对存入map中，其中第二个参数里的id就等于第一个参数timePartitionId的值，id用作第二个参数方法的参数传入

      boolean isSequence =    //判断当前待插入数据是否是顺序，根据对应时间分区的partitionLatestFlushedTimeForEachDevice进行比较，若大于partitionLatestFlushedTimeForEachDevice则说明是顺序的。
          insertRowPlan.getTime()
              > partitionLatestFlushedTimeForEachDevice
                  .get(timePartitionId)
                  .getOrDefault(insertRowPlan.getPrefixPath().getFullPath(), Long.MIN_VALUE);

      // is unsequence and user set config to discard out of order data
      if (!isSequence
          && IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) { //当是乱序的数据并且系统配置是无视、丢弃乱序数据，则抛弃该乱序数据不进行插入数据库，返回。
        return;
      }
      //获取该存储组下指定时间分区的每一个设备的写入数据的最大时间戳（包括未flush和已经flush的数据时间戳）
      latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>()); //当该latestTimeForEachDevice时间分区key下的value为空或者不存在该key，则初始化一个(timePartitionId,new HashMap())键值对

      // fire trigger before insertion
      TriggerEngine.fire(TriggerEvent.BEFORE_INSERT, insertRowPlan);
      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertRowPlan, isSequence, timePartitionId);  //交给对应时间分区的TsFileProcessor进行处理
      // fire trigger after insertion
      TriggerEngine.fire(TriggerEvent.AFTER_INSERT, insertRowPlan);
    } finally {
      writeUnlock();  //StorageGroupProcessor释放写锁
    }
  }

  /**
   * Insert a tablet (rows belonging to the same devices) into this storage group.
   *
   * @throws BatchProcessException if some of the rows failed to be inserted
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void insertTablet(InsertTabletPlan insertTabletPlan)
      throws BatchProcessException, TriggerExecutionException {

    writeLock("insertTablet");
    try {
      TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
      Arrays.fill(results, RpcUtils.SUCCESS_STATUS);
      boolean noFailure = true;

      /*
       * assume that batch has been sorted by client
       */
      int loc = 0;
      while (loc < insertTabletPlan.getRowCount()) {
        long currTime = insertTabletPlan.getTimes()[loc];
        // skip points that do not satisfy TTL
        if (!isAlive(currTime)) {
          results[loc] =
              RpcUtils.getStatus(
                  TSStatusCode.OUT_OF_TTL_ERROR,
                  "time " + currTime + " in current line is out of TTL: " + dataTTL);
          loc++;
          noFailure = false;
        } else {
          break;
        }
      }
      // loc pointing at first legal position
      if (loc == insertTabletPlan.getRowCount()) {
        throw new BatchProcessException(results);
      }

      // fire trigger before insertion
      final int firePosition = loc;
      TriggerEngine.fire(TriggerEvent.BEFORE_INSERT, insertTabletPlan, firePosition);

      // before is first start point
      int before = loc;
      // before time partition
      long beforeTimePartition =
          StorageEngine.getTimePartition(insertTabletPlan.getTimes()[before]);
      // init map
      long lastFlushTime =
          partitionLatestFlushedTimeForEachDevice
              .computeIfAbsent(beforeTimePartition, id -> new HashMap<>())
              .computeIfAbsent(
                  insertTabletPlan.getPrefixPath().getFullPath(), id -> Long.MIN_VALUE);
      // if is sequence
      boolean isSequence = false;
      while (loc < insertTabletPlan.getRowCount()) {
        long time = insertTabletPlan.getTimes()[loc];
        long curTimePartition = StorageEngine.getTimePartition(time);
        // start next partition
        if (curTimePartition != beforeTimePartition) {
          // insert last time partition
          if (isSequence
              || !IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
            noFailure =
                insertTabletToTsFileProcessor(
                        insertTabletPlan, before, loc, isSequence, results, beforeTimePartition)
                    && noFailure;
          }
          // re initialize
          before = loc;
          beforeTimePartition = curTimePartition;
          lastFlushTime =
              partitionLatestFlushedTimeForEachDevice
                  .computeIfAbsent(beforeTimePartition, id -> new HashMap<>())
                  .computeIfAbsent(
                      insertTabletPlan.getPrefixPath().getFullPath(), id -> Long.MIN_VALUE);
          isSequence = false;
        }
        // still in this partition
        else {
          // judge if we should insert sequence
          if (!isSequence && time > lastFlushTime) {
            // insert into unsequence and then start sequence
            if (!IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
              noFailure =
                  insertTabletToTsFileProcessor(
                          insertTabletPlan, before, loc, false, results, beforeTimePartition)
                      && noFailure;
            }
            before = loc;
            isSequence = true;
          }
          loc++;
        }
      }

      // do not forget last part
      if (before < loc
          && (isSequence
              || !IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData())) {
        noFailure =
            insertTabletToTsFileProcessor(
                    insertTabletPlan, before, loc, isSequence, results, beforeTimePartition)
                && noFailure;
      }
      long globalLatestFlushedTime =
          globalLatestFlushedTimeForEachDevice.getOrDefault(
              insertTabletPlan.getPrefixPath().getFullPath(), Long.MIN_VALUE);
      tryToUpdateBatchInsertLastCache(insertTabletPlan, globalLatestFlushedTime);

      if (!noFailure) {
        throw new BatchProcessException(results);
      }

      // fire trigger after insertion
      TriggerEngine.fire(TriggerEvent.AFTER_INSERT, insertTabletPlan, firePosition);
    } finally {
      writeUnlock();
    }
  }

  /** @return whether the given time falls in ttl */
  private boolean isAlive(long time) {  //当待插入的数据仍在其所属存储组设定的TTL生存时间内才允许插入，否则拒绝
    return dataTTL == Long.MAX_VALUE || (System.currentTimeMillis() - time) <= dataTTL;
  }

  /**
   * insert batch to tsfile processor thread-safety that the caller need to guarantee The rows to be
   * inserted are in the range [start, end)
   *
   * @param insertTabletPlan insert a tablet of a device
   * @param sequence whether is sequence
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   * @param timePartitionId time partition id
   * @return false if any failure occurs when inserting the tablet, true otherwise
   */
  private boolean insertTabletToTsFileProcessor(
      InsertTabletPlan insertTabletPlan,
      int start,
      int end,
      boolean sequence,
      TSStatus[] results,
      long timePartitionId) {
    // return when start >= end
    if (start >= end) {
      return true;
    }

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      for (int i = start; i < end; i++) {
        results[i] =
            RpcUtils.getStatus(
                TSStatusCode.INTERNAL_SERVER_ERROR,
                "can not create TsFileProcessor, timePartitionId: " + timePartitionId);
      }
      return false;
    }

    try {
      tsFileProcessor.insertTablet(insertTabletPlan, start, end, results);
    } catch (WriteProcessRejectException e) {
      logger.warn("insert to TsFileProcessor rejected, {}", e.getMessage());
      return false;
    } catch (WriteProcessException e) {
      logger.error("insert to TsFileProcessor error ", e);
      return false;
    }

    latestTimeForEachDevice.computeIfAbsent(timePartitionId, t -> new HashMap<>());
    // try to update the latest time of the device of this tsRecord
    if (sequence
        && latestTimeForEachDevice
                .get(timePartitionId)
                .getOrDefault(insertTabletPlan.getPrefixPath().getFullPath(), Long.MIN_VALUE)
            < insertTabletPlan.getTimes()[end - 1]) {
      latestTimeForEachDevice
          .get(timePartitionId)
          .put(
              insertTabletPlan.getPrefixPath().getFullPath(), insertTabletPlan.getTimes()[end - 1]);
    }

    // check memtable size and may async try to flush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
    return true;
  }

  private void tryToUpdateBatchInsertLastCache(InsertTabletPlan plan, Long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    IMeasurementMNode[] mNodes = plan.getMeasurementMNodes();
    int columnIndex = 0;
    for (int i = 0; i < mNodes.length; i++) {
      // Don't update cached last value for vector type
      if (mNodes[i] != null && plan.isAligned()) {
        columnIndex += mNodes[i].getSchema().getValueMeasurementIdList().size();
      } else {
        if (plan.getColumns()[i] == null) {
          columnIndex++;
          continue;
        }
        // Update cached last value with high priority
        if (mNodes[i] != null) {
          // in stand alone version, the seriesPath is not needed, just use measurementMNodes[i] to
          // update last cache
          IoTDB.metaManager.updateLastCache(
              null, plan.composeLastTimeValuePair(columnIndex), true, latestFlushedTime, mNodes[i]);
        } else {
          // measurementMNodes[i] is null, use the path to update remote cache
          IoTDB.metaManager.updateLastCache(
              plan.getPrefixPath().concatNode(plan.getMeasurements()[columnIndex]),
              plan.composeLastTimeValuePair(columnIndex),
              true,
              latestFlushedTime,
              null);
        }
        columnIndex++;
      }
    }
  }

  private void insertToTsFileProcessor(    //把插入计划交给对应时间分区的TsFileProcessor进行处理：(1) 获取（若没有则创建）当前TsFile对应的TsFileProcessor （2）往当前TsFileProcessor的workMemTable里插入数据
      InsertRowPlan insertRowPlan, boolean sequence, long timePartitionId)
      throws WriteProcessException {
    //根据时间分区和是否乱序获取对应的TsFileProcessor，若该存储组下的该时间分区里不存在TsFileProcessor,则说明插入数据的时间很新，即时间分区是新的，需要新建对应乱序or顺序的TsFileProcessor（同时也会创建对应的TsFile文件和TsFileResource对象）并返回
    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);//根据时间分区和是否乱序获取对应的TsFileProcessor，若不存在则创建新的一个对应的TsFileProcessor
    if (tsFileProcessor == null) {
      return;
    }

    tsFileProcessor.insert(insertRowPlan);    //往对应TsFileProcessor的workMemTable里插入数据，该方法做的事：（1）若当前TsFileProcessor的workMemTable是空，则创建一个（2）统计当前写入计划新增的内存占用，增加至TspInfo和SgInfo中（3）若系统配置是允许写前日志，则记录写前日志（4）写入workMemTable，即遍历该插入计划中每个待插入传感器的数值，往该传感器对应的memtable里的TVList写入待插入的数值,首先判断是否要对此TVList的values和timestamps列表，然后往该TVList的values和timestamps列表的某一数组里里插入对应的时间戳和数值（5）在插入操作后，要更新该TsFile对应TsFileResource对象里该设备数据的最大、最小时间戳

    // try to update the latest time of the device of this tsRecord
    if (latestTimeForEachDevice
            .get(timePartitionId)
            .getOrDefault(insertRowPlan.getPrefixPath().getFullPath(), Long.MIN_VALUE)
        < insertRowPlan.getTime()) {  //如果该存储组下该时间分区里该设备的最后写入数据的最大时间戳小于此次插入数据的时间戳，则需要更新该存储组下该分区的该设备的最后写入数据的时间戳
      latestTimeForEachDevice
          .get(timePartitionId)
          .put(insertRowPlan.getPrefixPath().getFullPath(), insertRowPlan.getTime());
    }

    long globalLatestFlushTime =  //获取此次该设备在全局、跨时间分区的最后刷盘数据的最大时间戳
        globalLatestFlushedTimeForEachDevice.getOrDefault(
            insertRowPlan.getPrefixPath().getFullPath(), Long.MIN_VALUE);

    tryToUpdateInsertLastCache(insertRowPlan, globalLatestFlushTime); //为此次插入行为的设备下的每个传感器更新对应的上个时间戳数据点缓存，即记录的是每个传感器在此插入行为后最大的时间戳和对应的数值

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {  //判断该TsFileProcessor的workMemTable是否需要被Flush
      fileFlushPolicy.apply(this, tsFileProcessor, sequence); //执行flush此TSFile的workMemTable
    }
  }

  private void tryToUpdateInsertLastCache(InsertRowPlan plan, Long latestFlushedTime) { //为此次插入行为的设备下的每个传感器更新对应的上个时间戳数据点缓存，即记录的是每个传感器在此插入行为后最大的时间戳和对应的数值//第二个参数是该设备在全局、跨时间分区的最后刷盘数据的最大时间戳
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    IMeasurementMNode[] mNodes = plan.getMeasurementMNodes();
    int columnIndex = 0;  //用作临时的传感器索引，供下方遍历使用
    for (IMeasurementMNode mNode : mNodes) {  //循环遍历每个传感器节点
      // Don't update cached last value for vector type
      if (!plan.isAligned()) {  //如果是不对齐的
        if (plan.getValues()[columnIndex] == null) {
          columnIndex++;
          continue;
        }
        // Update cached last value with high priority
        if (mNode != null) {  //如果传感器节点不为空
          // in stand alone version, the seriesPath is not needed, just use measurementMNodes[i] to
          // update last cache
          IoTDB.metaManager.updateLastCache(  //更新此传感器上个时间戳数据点缓存（必须是顺序的，即记录的是每个传感器在上次插入行为后最晚的时间戳和对应的数值）
              null, plan.composeTimeValuePair(columnIndex), true, latestFlushedTime, mNode);
        } else {  //如果传感器节点为空
          IoTDB.metaManager.updateLastCache(
              plan.getPrefixPath().concatNode(plan.getMeasurements()[columnIndex]), //要自己获取对应的传感器节点
              plan.composeTimeValuePair(columnIndex),
              true,
              latestFlushedTime,
              null);
        }
        columnIndex++;
      }
    }
  }

  /**
   * mem control module use this method to flush memtable
   *
   * @param tsFileProcessor tsfile processor in which memtable to be flushed
   */
  public void submitAFlushTaskWhenShouldFlush(TsFileProcessor tsFileProcessor) {
    writeLock("submitAFlushTaskWhenShouldFlush");
    try {
      // check memtable size and may asyncTryToFlush the work memtable
      if (tsFileProcessor.shouldFlush()) {
        fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
      }
    } finally {
      writeUnlock();
    }
  }

  private TsFileProcessor getOrCreateTsFileProcessor(long timeRangeId, boolean sequence) {//根据时间分区和是否乱序获取对应的TsFileProcessor，若不存在则创建新的一个对应的TsFileProcessor
    TsFileProcessor tsFileProcessor = null;
    try {
      if (sequence) { //如果是顺序的
        tsFileProcessor =
            getOrCreateTsFileProcessorIntern(timeRangeId, workSequenceTsFileProcessors, true);//workSequenceTsFileProcessors存放了每个时间分区下对应的顺序TsFileProcessor,即（时间分区ID,TsFileProcessor对象）
      } else {  //如果是乱序的
        tsFileProcessor =
            getOrCreateTsFileProcessorIntern(timeRangeId, workUnsequenceTsFileProcessors, false);
      }
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "disk space is insufficient when creating TsFile processor, change system mode to read-only",
          e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    } catch (IOException e) {
      logger.error(
          "meet IOException when creating TsFileProcessor, change system mode to read-only", e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    }
    return tsFileProcessor;
  }

  /**
   * get processor from hashmap, flush oldest processor if necessary
   *
   * @param timeRangeId time partition range
   * @param tsFileProcessorTreeMap tsFileProcessorTreeMap
   * @param sequence whether is sequence or not
   */
  private TsFileProcessor getOrCreateTsFileProcessorIntern( //返回或创建新的TsFileProcessor（若创建新的TsFileProcessor则需要先创建对应的TsFile文件）
      long timeRangeId, TreeMap<Long, TsFileProcessor> tsFileProcessorTreeMap, boolean sequence)
      throws IOException, DiskSpaceInsufficientException {

    TsFileProcessor res = tsFileProcessorTreeMap.get(timeRangeId);  //根据时间分区id从workSequenceTsFileProcessors/workUnSequenceTsFileProcessors中获取TsFileProcessor

    if (null == res) {  //当workSequenceTsFileProcessors里不包含该时间分区的TsFileProcessor，说明该时间分区是新的，则需要创建该新时间分区的TsFile文件，也需要创建一个对应新的TsFileProcessor存入workSequenceTsFileProcessors中，这个时候就需要注意内存的占用情况
      // we have to remove oldest processor to control the num of the memtables
      // TODO: use a method to control the number of memtables
      if (tsFileProcessorTreeMap.size()
          >= IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition()) {//当workSequenceTsFileProcessors里存放TsFileProcessor的数量超过了系统的指定数量，则需要把最早的，即第一个TsFileProcessor给删除掉
        Map.Entry<Long, TsFileProcessor> processorEntry = tsFileProcessorTreeMap.firstEntry();//获取最早加入的、第一个TsFileProcessor
        logger.info(
            "will close a {} TsFile because too many active partitions ({} > {}) in the storage group {},",
            sequence,
            tsFileProcessorTreeMap.size(),
            IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition(),
            logicalStorageGroupName);
        asyncCloseOneTsFileProcessor(sequence, processorEntry.getValue());  //异步关闭最早的TsFileProcessor
      }

      // build new processor
      res = newTsFileProcessor(sequence, timeRangeId);  //根据是否顺序，创建该存储组下该时间分区对应新的TsFileProcessor
      tsFileProcessorTreeMap.put(timeRangeId, res); //把新建的该时间分区的TsFileProcessor放入工作的活跃workSequenceTsFileProcessors/workUnSequenceTsFileProcessors队列中
      tsFileManagement.add(res.getTsFileResource(), sequence);  //由于会新建TsFile，所以要把该文件相应的TsFileResource信息存入TsFileManagement管理类里。
    }

    return res;
  }

  private TsFileProcessor newTsFileProcessor(boolean sequence, long timePartitionId)
      throws IOException, DiskSpaceInsufficientException {    //根据时间分区和是否顺序创建对应的TsFile文件和对应的TsFileProcessor
    DirectoryManager directoryManager = DirectoryManager.getInstance();
    String baseDir =    //获取根目录
        sequence
            ? directoryManager.getNextFolderForSequenceFile()
            : directoryManager.getNextFolderForUnSequenceFile();
    fsFactory   //创建TsFile文件
        .getFile(baseDir + File.separator + logicalStorageGroupName, virtualStorageGroupId)
        .mkdirs();

    String filePath =
        baseDir
            + File.separator
            + logicalStorageGroupName
            + File.separator
            + virtualStorageGroupId
            + File.separator
            + timePartitionId
            + File.separator
            + getNewTsFileName(timePartitionId);

    return getTsFileProcessor(sequence, filePath, timePartitionId);
  }

  private TsFileProcessor getTsFileProcessor( //新建TsFileProcessor
      boolean sequence, String filePath, long timePartitionId) throws IOException {
    TsFileProcessor tsFileProcessor;
    if (sequence) { //如果是顺序
      tsFileProcessor =
          new TsFileProcessor(
              logicalStorageGroupName + File.separator + virtualStorageGroupId,
              fsFactory.getFileWithParent(filePath),
              storageGroupInfo,
              this::closeUnsealedTsFileProcessorCallBack,
              this::updateLatestFlushTimeCallback,
              true);
    } else {    //如果是乱序
      tsFileProcessor =
          new TsFileProcessor(
              logicalStorageGroupName + File.separator + virtualStorageGroupId,
              fsFactory.getFileWithParent(filePath),
              storageGroupInfo,
              this::closeUnsealedTsFileProcessorCallBack,
              this::unsequenceFlushCallback,
              false);
    }

    if (enableMemControl) { //如果系统开启了内存控制，则创建TsFileProcessor对应的内存控制类TsFileProcessorInfo对象
      TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
      tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);  //将控制类配置给此TsFileProcessor
      this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
    }

    tsFileProcessor.addCloseFileListeners(customCloseFileListeners);  //增加CloseFileListener
    tsFileProcessor.addFlushListeners(customFlushListeners);  //增加FlushListener
    tsFileProcessor.setTimeRangeId(timePartitionId);  //设置时间分区ID

    return tsFileProcessor;
  }

  /**
   * Create a new tsfile name
   *
   * @return file name
   */
  private String getNewTsFileName(long timePartitionId) {
    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, 0L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    return getNewTsFileName(System.currentTimeMillis(), version, 0, 0);
  }

  private String getNewTsFileName(long time, long version, int mergeCnt, int unSeqMergeCnt) {
    return TsFileResource.getNewTsFileName(time, version, mergeCnt, unSeqMergeCnt);
  }

  /**
   * close one tsfile processor
   *
   * @param sequence whether this tsfile processor is sequence or not
   * @param tsFileProcessor tsfile processor
   */
  public void syncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {
    synchronized (closeStorageGroupCondition) {
      try {
        asyncCloseOneTsFileProcessor(sequence, tsFileProcessor);
        long startTime = System.currentTimeMillis();
        while (closingSequenceTsFileProcessor.contains(tsFileProcessor)
            || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn(
                "{} has spent {}s to wait for closing one tsfile.",
                logicalStorageGroupName + "-" + this.virtualStorageGroupId,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error(
            "syncCloseOneTsFileProcessor error occurs while waiting for closing the storage "
                + "group {}",
            logicalStorageGroupName + "-" + virtualStorageGroupId,
            e);
      }
    }
  }

  /**
   * close one tsfile processor, thread-safety should be ensured by caller
   *
   * @param sequence whether this tsfile processor is sequence or not
   * @param tsFileProcessor tsfile processor
   */
  public void asyncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {   //异步关闭指定的TsFileProcessor，以保证内存大小的可使用，或者是当该TsFile文件过大时就需要关闭此文件(具体做法是将其加入关闭TsFileProcessor队列中，并更新对应的TsFileResource每个设备的最后写入数据的时间戳，然后异步关闭该TsFileProcessor，然后从活跃队列中移除，并判断是否关闭对应分区的版本控制)
    // for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    // for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)
        || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)
        || tsFileProcessor.alreadyMarkedClosing()) {    //如果该TsFileProcessor已经被关闭了，则返回
      return;
    }
    logger.info(
        "Async close tsfile: {}",
        tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
    if (sequence) { //如果待关闭的TsFileProcessor是顺序的
      closingSequenceTsFileProcessor.add(tsFileProcessor);  //把该TsFileProcessor加入到关闭的顺序队列closingSequenceTsFileProcessor中
      updateEndTimeMap(tsFileProcessor);  //更新该TsFile对应TsFileResource的每个设备的最后写入的数据时间戳
      tsFileProcessor.asyncClose();   //将此TsFileProcessor进行异步关闭

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());  //从活跃顺序workSequenceTsFileProcessors队列中移除此TsFileProcessor
      // if unsequence files don't contain this time range id, we should remove it's version
      // controller
      //当活跃的乱序TsFileProcessor队列workUnsequenceTsFileProcessors不包含此已关闭的顺序TsFileProcessor所在时间分区对应的乱序TsFileProcessor，即该存储组的该时间分区里已经不存在任何活跃的顺序、乱序TsFileProcessor，该时间分区必不会占用任何内存memtable，所以要从timePartitionIdVersionControllerMap移除该分区的版本控制。
      if (!workUnsequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
      logger.info(
          "close a sequence tsfile processor {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
    } else {    //如果待关闭的TsFileProcessor是乱序的
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);//把该TsFileProcessor加入到关闭的乱序队列closingUnSequenceTsFileProcessor中
      tsFileProcessor.asyncClose();//将此TsFileProcessor进行异步关闭

      workUnsequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());//从活跃乱序workSequenceTsFileProcessors队列中移除此TsFileProcessor
      // if sequence files don't contain this time range id, we should remove it's version
      // controller
      //当该时间分区里也不存在活跃的顺序TsFileProcessor时，即该时间分区不存在任何乱序、顺序TsFileProcessor，必不占用任何内存memtable，因此要从timePartitionIdVersionControllerMap移除该分区的版本控制。
      if (!workSequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
    }
  }

  /**
   * delete the storageGroup's own folder in folder data/system/storage_groups
   *
   * @param systemDir system dir
   */
  public void deleteFolder(String systemDir) {
    logger.info(
        "{} will close all files for deleting data folder {}",
        logicalStorageGroupName + "-" + virtualStorageGroupId,
        systemDir);
    writeLock("deleteFolder");
    try {
      syncCloseAllWorkingTsFileProcessors();
      File storageGroupFolder =
          SystemFileFactory.INSTANCE.getFile(systemDir, virtualStorageGroupId);
      if (storageGroupFolder.exists()) {
        org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
      }
    } finally {
      writeUnlock();
    }
  }

  /** close all tsfile resource */
  public void closeAllResources() {
    for (TsFileResource tsFileResource : tsFileManagement.getTsFileList(false)) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
    for (TsFileResource tsFileResource : tsFileManagement.getTsFileList(true)) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
  }

  /** release wal buffer */
  public void releaseWalDirectByteBufferPool() {
    synchronized (walByteBufferPool) {
      while (!walByteBufferPool.isEmpty()) {
        MmapUtil.clean((MappedByteBuffer) walByteBufferPool.removeFirst());
        currentWalPoolSize--;
      }
    }
  }

  /** delete tsfile */
  public void syncDeleteDataFiles() {
    logger.info(
        "{} will close all files for deleting data files",
        logicalStorageGroupName + "-" + virtualStorageGroupId);
    writeLock("syncDeleteDataFiles");
    try {

      syncCloseAllWorkingTsFileProcessors();
      // normally, mergingModification is just need to be closed by after a merge task is finished.
      // we close it here just for IT test.
      if (this.tsFileManagement.mergingModification != null) {
        this.tsFileManagement.mergingModification.close();
      }

      closeAllResources();
      List<String> folder = DirectoryManager.getInstance().getAllSequenceFileFolders();
      folder.addAll(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      deleteAllSGFolders(folder);

      this.workSequenceTsFileProcessors.clear();
      this.workUnsequenceTsFileProcessors.clear();
      this.tsFileManagement.clear();
      this.partitionLatestFlushedTimeForEachDevice.clear();
      this.globalLatestFlushedTimeForEachDevice.clear();
      this.latestTimeForEachDevice.clear();
    } catch (IOException e) {
      logger.error(
          "Cannot close the mergingMod file {}",
          this.tsFileManagement.mergingModification.getFilePath(),
          e);
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File storageGroupFolder =
          fsFactory.getFile(
              tsfilePath, logicalStorageGroupName + File.separator + virtualStorageGroupId);
      if (storageGroupFolder.exists()) {
        org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
      }
    }
  }

  /** Iterate each TsFile and try to lock and remove those out of TTL. */
  public synchronized void checkFilesTTL() {   //检查该虚拟存储组下所有顺序和乱序的TsFile的数据是否超过TTL,若有则删除本地相关的文件(TsFile和mods文件和resource文件)
    if (dataTTL == Long.MAX_VALUE) {
      logger.debug(
          "{}: TTL not set, ignore the check",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
      return;
    }
    long ttlLowerBound = System.currentTimeMillis() - dataTTL;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: TTL removing files before {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId,
          new Date(ttlLowerBound));
    }

    // copy to avoid concurrent modification of deletion
    List<TsFileResource> seqFiles = new ArrayList<>(tsFileManagement.getTsFileList(true));
    List<TsFileResource> unseqFiles = new ArrayList<>(tsFileManagement.getTsFileList(false));

    for (TsFileResource tsFileResource : seqFiles) {
      checkFileTTL(tsFileResource, ttlLowerBound, true);//判断给定的TsFile里是否存在数据超过TTL，若有则删除本地相关的文件(TsFile和mods文件和resource文件)
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      checkFileTTL(tsFileResource, ttlLowerBound, false);//判断给定的TsFile里是否存在数据超过TTL，若有则删除本地相关的文件(TsFile和mods文件和resource文件)
    }
  }

  private void checkFileTTL(TsFileResource resource, long ttlLowerBound, boolean isSeq) { //判断给定的TsFile里是否存在数据超过TTL，若有则删除本地相关的文件(TsFile和mods文件和resource文件)
    if (resource.isMerging()  //若文件正在合并中or文件未封口or（文件未被删除并且文件里的所有数据还活着，未超出TTL），则直接返回
        || !resource.isClosed()
        || !resource.isDeleted() && resource.stillLives(ttlLowerBound)) {
      return;
    }

    //若该TsFile的存在数据超过了TTL，则把该TsFile本地文件和相关文件给删除
    writeLock("checkFileTTL");  //对该虚拟存储组加写锁，防止用户对该虚拟存储组下的文件有其他操作
    try {
      // prevent new merges and queries from choosing this file
      resource.setDeleted(true);  //设置删除状态，防止后续系统对此被删除的TsFile进行merge合并
      // the file may be chosen for merge after the last check and before writeLock()
      // double check to ensure the file is not used by a merge
      if (resource.isMerging()) {
        return;
      }

      // ensure that the file is not used by any queries
      if (resource.tryWriteLock()) {  //对该TsFileResource加写锁
        try {
          // physical removal
          resource.remove();  //删除该TsFile对应的本地文件和.mods文件和.resource文件
          if (logger.isInfoEnabled()) {
            logger.info(
                "Removed a file {} before {} by ttl ({}ms)",
                resource.getTsFilePath(),
                new Date(ttlLowerBound),
                dataTTL);
          }
          tsFileManagement.remove(resource, isSeq);
        } finally {
          resource.writeUnlock(); //释放该TsfileResource的写锁
        }
      }
    } finally {
      writeUnlock();  //释放该虚拟存储组的写锁
    }
  }

  public void timedFlushMemTable() {
    writeLock("timedFlushMemTable");
    try {
      // only check unsequence tsfiles' memtables
      List<TsFileProcessor> tsFileProcessors =
          new ArrayList<>(workUnsequenceTsFileProcessors.values());
      long timestampBaseline = System.currentTimeMillis() - config.getUnseqMemtableFlushInterval();
      for (TsFileProcessor tsFileProcessor : tsFileProcessors) {
        if (tsFileProcessor.getWorkMemTableCreatedTime() < timestampBaseline) {
          logger.info(
              "Exceed flush interval, so flush work memtable of time partition {} in storage group {}[{}]",
              tsFileProcessor.getTimeRangeId(),
              logicalStorageGroupName,
              virtualStorageGroupId);
          fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /** This method will be blocked until all tsfile processors are closed. */
  public void syncCloseAllWorkingTsFileProcessors() {
    synchronized (closeStorageGroupCondition) {
      try {
        asyncCloseAllWorkingTsFileProcessors();
        long startTime = System.currentTimeMillis();
        while (!closingSequenceTsFileProcessor.isEmpty()
            || !closingUnSequenceTsFileProcessor.isEmpty()) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn(
                "{} has spent {}s to wait for closing all TsFiles.",
                logicalStorageGroupName + "-" + this.virtualStorageGroupId,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "CloseFileNodeCondition error occurs while waiting for closing the storage "
                + "group {}",
            logicalStorageGroupName + "-" + virtualStorageGroupId,
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** close all working tsfile processors */
  public void asyncCloseAllWorkingTsFileProcessors() {
    writeLock("asyncCloseAllWorkingTsFileProcessors");
    try {
      logger.info(
          "async force close all files in storage group: {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        asyncCloseOneTsFileProcessor(true, tsFileProcessor);
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        asyncCloseOneTsFileProcessor(false, tsFileProcessor);
      }
    } finally {
      writeUnlock();
    }
  }

  /** force close all working tsfile processors */
  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    writeLock("forceCloseAllWorkingTsFileProcessors");
    try {
      logger.info(
          "force close all processors in storage group: {}",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workSequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor :
          new ArrayList<>(workUnsequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
      }
    } finally {
      writeUnlock();
    }
  }

  // TODO need a read lock, please consider the concurrency with flush manager threads.
  /**
   * build query data source by searching all tsfile which fit in query filter
   *
   * @param fullPath data path
   * @param context query context
   * @param timeFilter time filter
   * @return query data source
   */
  public QueryDataSource query(//获取此次查询需要用到的所有顺序or乱序TsFileResource,并把它们往添加入查询文件管理类里，即添加此次查询ID对应需要用到的顺序和乱序TsFileResource,并创建返回QueryDataSource对象，该类对象存放了一次查询里对一条时间序列涉及到的所有顺序TsFileResource和乱序TsFileResource和数据TTL
      PartialPath fullPath, //此次查询的此单一时间序列的全路径
      QueryContext context, //此次查询的查询环境类对象
      QueryFileManager filePathsManager,//查询文件管理类对象，该类存放了每个查询ID对应需要的已封口和为封口的TsFileResource
      Filter timeFilter)  //此次查询的时间过滤器，可能是一元或二元的过滤器
      throws QueryProcessException {
    readLock();   //查询操作前，先对虚拟存储组粒度加读锁
    try {
      List<TsFileResource> seqResources =
          getFileResourceListForQuery(//获取该虚拟存储组下的所有满足此次查询要求（即该TsFile里存在待查询的设备，且设备下的所有数据都存活，且存在满足时间过滤器的数据点）的顺序的TsFileResource（包含已封口和为封口的）和待升级的TsFileResource，将它们放入此次的待查寻TsFileResource列表中
              tsFileManagement.getTsFileList(true), //获取此虚拟存储组下所有顺序的TsFileResource
              upgradeSeqFileList, //待升级的顺序TsFileResource列表
              fullPath, //此次查询的此单一时间序列路径
              context,  //查询环境
              timeFilter, //此查询的时间过滤器
              true);
      List<TsFileResource> unseqResources =//获取该虚拟存储组下的所有满足此次查询要求（即该TsFile里存在待查询的设备，且设备下的所有数据都存活，且存在满足时间过滤器的数据点）的乱序的TsFileResource（包含已封口和为封口的）和待升级的TsFileResource，将它们放入此次的待查寻TsFileResource列表中
          getFileResourceListForQuery(
              tsFileManagement.getTsFileList(false),
              upgradeUnseqFileList,
              fullPath,
              context,
              timeFilter,
              false);
      QueryDataSource dataSource = new QueryDataSource(seqResources, unseqResources);//创建QueryDataSource对象，该类对象存放了一次查询里对一条时间序列涉及到的所有顺序TsFileResource和乱序TsFileResource
      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      // is null only in tests
      if (filePathsManager != null) {
        filePathsManager.addUsedFilesForQuery(context.getQueryId(), dataSource);//往查询文件管理类里添加此次查询ID对应需要用到的顺序和乱序TsFileResource
      }
      dataSource.setDataTTL(dataTTL); //对QueryDataSource对象设置TTL数据存活时间
      return dataSource;
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    } finally {
      readUnlock();
    }
  }

  /** lock the read lock of the insert lock */
  public void readLock() {
    // apply read lock for SG insert lock to prevent inconsistent with concurrently writing memtable
    insertLock.readLock().lock();
    // apply read lock for TsFileResource list
    tsFileManagement.readLock();
  }

  /** unlock the read lock of insert lock */
  public void readUnlock() {
    tsFileManagement.readUnLock();
    insertLock.readLock().unlock();
  }

  /** lock the write lock of the insert lock */
  public void writeLock(String holder) {
    insertLock.writeLock().lock();
    insertWriteLockHolder = holder;
  }

  /** unlock the write lock of the insert lock */
  public void writeUnlock() {
    insertWriteLockHolder = "";
    insertLock.writeLock().unlock();
  }

  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResource> getFileResourceListForQuery( //获取该虚拟存储组下的所有满足此次查询要求（即该TsFile里存在待查询的设备，且设备下的所有数据都存活，且存在满足时间过滤器的数据点）的顺序or乱序的TsFileResource（包含已封口和为封口的）和待升级的TsFileResource，将它们放入此次的待查寻TsFileResource列表中
      Collection<TsFileResource> tsFileResources,//此虚拟存储组下所有顺序or乱序的TsFileResource
      List<TsFileResource> upgradeTsFileResources,//待升级的顺序or乱序TsFileResource列表
      PartialPath fullPath,//此次查询的此单一时间序列路径
      QueryContext context,//查询环境类对象
      Filter timeFilter,    //时间过滤器
      boolean isSeq)  //是否顺序
      throws MetadataException {
    String deviceId = fullPath.getDevice(); //根据查询的时间序列路径获取设备路径ID

    if (context.isDebug()) {
      DEBUG_LOGGER.info(
          "Path: {}.{}, get tsfile list: {} isSeq: {} timefilter: {}",
          deviceId,
          fullPath.getMeasurement(),
          tsFileResources,
          isSeq,
          (timeFilter == null ? "null" : timeFilter));
    }

    IMeasurementSchema schema = IoTDB.metaManager.getSeriesSchema(fullPath);  //根据时间序列全路径获取该路径上对应的传感器的配置类对象。

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>(); //存放了此次查询需要用到的TsFile对应的TsFileResource
    long ttlLowerBound = //该属性表示在当下对于给定的TTL，数据点允许的最小时间戳，即时间戳小于ttlLowerBound的数据就是超过了TTL存活时间了，只有时间戳大于等于ttlLowerBound的数据才处于存活状态    //对于某一TsFile，ttlLowerBound必须小于等于该TsFile里每个设备的最大数据时间戳endTime，即ttlLowerBound=currentTime-dataTTL<=endTime，则说明该TsFile仍存活，未超出TTL
        dataTTL != Long.MAX_VALUE ? System.currentTimeMillis() - dataTTL : Long.MIN_VALUE;
    context.setQueryTimeLowerBound(ttlLowerBound);  //

    // for upgrade files and old files must be closed
    for (TsFileResource tsFileResource : upgradeTsFileResources) {  //遍历待升级or正在升级中的顺序or乱序TsFileResource
      if (!tsFileResource.isSatisfied(deviceId, timeFilter, isSeq, dataTTL, context.isDebug())) { //根据给定的时间过滤器和ttl等参数，判断该TsFile里的该设备下的所有数据是否满足要求（即该设备下的所有数据都存活，且存在满足时间过滤器的数据点）。若不满足要求，则遍历下一个待升级的TsFileResource
        continue;
      }
      closeQueryLock.readLock().lock();//加读锁
      try {
        tsfileResourcesForQuery.add(tsFileResource);//将此待升级or正在升级中的TsFileResource加入tsfileResourcesForQuery待查寻列表中
      } finally {
        closeQueryLock.readLock().unlock(); //释放读锁
      }
    }

    for (TsFileResource tsFileResource : tsFileResources) { //循环遍历给定的该虚拟存储组下的顺序or乱序TsFileResource列表
      if (!tsFileResource.isSatisfied(  //若该TsFileResource的该指定设备下的数据不满足要求（要求即未超过TTL且存在数据点满足给定的时间过滤器），则遍历下一个
          fullPath.getDevice(), timeFilter, isSeq, dataTTL, context.isDebug())) {
        continue;
      }
      //若该TsFile的指定设备下的所有数据都满足要求，则
      closeQueryLock.readLock().lock();//加读锁
      try {
        if (tsFileResource.isClosed()) {  //若该TsFile已封口，则把其TsFileResource加入待查询列表tsfileResourcesForQuery中
          tsfileResourcesForQuery.add(tsFileResource);
        } else {  //否则获取该TsFile对应的未封口的TsFileProcessor进行查询操作
          tsFileResource
              .getUnsealedFileProcessor()
              .query(deviceId, fullPath.getMeasurement(), schema, context, tsfileResourcesForQuery); //此方法根据给定的设备和传感器等参数，创建一个只读的TsFileResource对象，并把它放入tsfileResourcesForQuery列表里，用作查询
        }
      } catch (IOException e) {
        throw new MetadataException(e);
      } finally {
        closeQueryLock.readLock().unlock();//释放读锁
      }
    }
    return tsfileResourcesForQuery;
  }

  /**
   * Delete data whose timestamp <= 'timestamp' and belongs to the time series
   * deviceId.measurementId.
   *
   * @param path the timeseries path of the to be deleted.
   * @param startTime the startTime of delete range.
   * @param endTime the endTime of delete range.
   */
  public void delete(PartialPath path, long startTime, long endTime, long planIndex)
      throws IOException {    //注意：时间序列路径的存储组是确定的，但设备路径还不确定，可能包含通配符*，因此可能有多个设备。eg:root.ln.*.*.*
    // If there are still some old version tsfiles, the delete won't succeeded.
    if (upgradeFileCount.get() != 0) {  //当仍然存在旧的待升级的TsFile，说明这些旧的TsFile正在升级中，此时不能执行删除操作。其实，只要判断若该删除操作是对旧的升级中的TsFile里的数据进行删除，则才不允许此删除操作执行，因为删除操作会修改对应旧TsFile的mods文件，而升级过程会对这些旧TsFile的mods文件进行删除。
     throw new IOException(
          "Delete failed. " + "Please do not delete until the old files upgraded.");
    }
    // TODO: how to avoid partial deletion?
    // FIXME: notice that if we may remove a SGProcessor out of memory, we need to close all opened
    // mod files in mergingModification, sequenceFileList, and unsequenceFileList
    writeLock("delete");  //加锁

    // record files which are updated so that we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      Set<PartialPath> devicePaths = IoTDB.metaManager.getDevices(path.getDevicePath());  //根据时间路径获取其上的设备路径，此处由于只确定了存储组，因此可能有一个或多个设备
      for (PartialPath device : devicePaths) {//循环遍历根据该确定存储组的路径搜索到的所有设备路径对象
        Long lastUpdateTime = null; //用于记录该存储组下待删除的所有设备中写入数据的最大时间戳
        for (Map<String, Long> latestTimeMap : latestTimeForEachDevice.values()) {//循环遍历该存储组下（设备名称 ,最后写入时间戳），即每个设备的写入数据的最大时间戳（包括未flush和已经flush的数据时间戳）
          Long curTime = latestTimeMap.get(device.getFullPath()); //获取该存储组下的该设备的写入数据的最大时间戳
          if (curTime != null && (lastUpdateTime == null || lastUpdateTime < curTime)) {//若（该设备的写入数据的最大时间戳不为空）并且（（是第一次循环）或者（最大时间戳小于此设备写入数据的最大时间戳））
            lastUpdateTime = curTime;       //记最大时间戳为此设备写入数据的最大时间戳
          }
        }
        // delete Last cache record if necessary
        tryToDeleteLastCache(device, path, startTime, endTime);//在删除操作时，根据给定的设备判断其下的每个传感器节点是否需要重置清空上个最大时间戳数据点缓存（若该缓存在此删除操作的时间范围里，则清空）
      }

      // write log to impacted working TsFileProcessors//对于每一个 Memtable，都会记录一个 WAL 文件，当 Memtable 被 flush 完成时，WAL 会被删掉。WAL只针对正在工作的TSFileProcessor下的每个传感器memtable
      logDeletion(startTime, endTime, path);  //对于该存储组下当前正在工作的顺序或乱序TsFileProcessor中，若他们所属的时间分区正好是待删除数据的时间分区范围内，则使用该时间分区正在工作的TsFileProcessor把删除计划记录进写前日志

      Deletion deletion = new Deletion(path, MERGE_MOD_START_VERSION_NUM, startTime, endTime);  //新建一个删除操作类对象
      if (tsFileManagement.mergingModification != null) { //如果mergingModification不为空，说明此该虚拟存储组下存在正在合并的TsFile，则将此次删除操作写入临时mods文件里 //由于乱序合并在开始时会在当前存储组创建一个名为mergingModification的临时ModificationFile变量，因此每次deleteion操作前都需要判断存储组中的mergingModification变量是否为null，如果不为null，说明当前正在进行乱序合并则将此次delete操作写入mergingModification临时变量中，不再写入TSFile对应的本地mods文件中；否则写入对应的本地mods文件中。
        tsFileManagement.mergingModification.write(deletion);//将此delete操作写入mergingModification临时文件中
        updatedModFiles.add(tsFileManagement.mergingModification);  //将此次删除操作的ModificationFile对象加入列表
      }

      deleteDataInFiles( //对该存储组下的所有顺序TsFile中存在待删除的设备的在待删除时间范围内的数据进行删除操作，1. 首先往本地对应的.mods文件记录此删除操作  2. 若某TsFile的内存workMemTable中存在数据，则根据该workMemTable是未flush还是正在flushing进行相关的删除操作
          tsFileManagement.getTsFileList(true), deletion, devicePaths, updatedModFiles, planIndex);
      deleteDataInFiles( //对该存储组下的所有乱序TsFile中存在待删除的设备的在待删除时间范围内的数据进行删除操作，1. 首先往本地对应的.mods文件记录此删除操作  2. 若某TsFile的内存workMemTable中存在数据，则根据该workMemTable是未flush还是正在flushing进行相关的删除操作
          tsFileManagement.getTsFileList(false), deletion, devicePaths, updatedModFiles, planIndex);

    } catch (Exception e) { //若出现意外，则roll back
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
      }
      throw new IOException(e);
    } finally {
      writeUnlock();  //释放锁
    }
  }

  private void logDeletion(long startTime, long endTime, PartialPath path) throws IOException { //对于当前正在工作的顺序或乱序TsFileProcessor中，若他们所属的时间分区正好是待删除数据的时间分区范围内，则使用该时间分区正在工作的TsFileProcessor把删除计划记录进写前日志//此处是只明确了存储组的之间序列路径，eg:root.ln.*.*.*
    long timePartitionStartId = StorageEngine.getTimePartition(startTime);  //开始时间所在的时间分区ID
    long timePartitionEndId = StorageEngine.getTimePartition(endTime); //结束时间所在的时间分区ID
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {  //若系统允许记录写前日志
      DeletePlan deletionPlan = new DeletePlan(startTime, endTime, path);
      //当前面有对该存储组的插入操作时，该存储组下就会有某时间分区的正在工作的workSequenceTsFileProcessors和workUnsequenceTsFileProcessors 列表
      for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {  //遍历每个时间分区对应的顺序工作TsFileProcessor
        if (timePartitionStartId <= entry.getKey() && entry.getKey() <= timePartitionEndId) { //当此工作的顺序TsFile的TsFileProcessor所在时间分区正好是待删除数据的时间分区范围内
          entry.getValue().getLogNode().write(deletionPlan);  //使用该时间分区正在工作的TsFileProcessor把删除计划记录进写前日志
        }
      }

      for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {//遍历每个时间分区对应的乱序工作TsFileProcessor
        if (timePartitionStartId <= entry.getKey() && entry.getKey() <= timePartitionEndId) {
          entry.getValue().getLogNode().write(deletionPlan);
        }
      }
    }
  }

  private boolean canSkipDelete(    //判断该顺序（乱序）TsFile里的待删除设备，他们是否可以跳过删除操作（若有任一设备下的数据待删除则不能跳过）
      TsFileResource tsFileResource,
      Set<PartialPath> devicePaths,
      long deleteStart,
      long deleteEnd) {
    for (PartialPath device : devicePaths) {//遍历设备路径对象
      String deviceId = device.getFullPath();
      long endTime = tsFileResource.getEndTime(deviceId); //获取该TsFile里该设备的数据的最大时间戳。
      if (endTime == Long.MIN_VALUE) {  //如果最大时间戳=最小整数，说明该TSFile文件还未被写入数据，说明数据还有可能存在于内存workMemTable中，因此需要被删除，返回false
        return false;
      }

      if (tsFileResource.isDeviceIdExist(deviceId)  //若此TsFile文件里有包含此设备的数据  并且  该设备数据的时间范围与待删除的时间范围存在重叠区域  则返回false
          && (deleteEnd >= tsFileResource.getStartTime(deviceId) && deleteStart <= endTime)) {
        return false;
      }
    }
    return true;  //若此TsFile不存在所有给定设备的数据 或者 若此TsFile的所有给定设备的数据范围都不在待删除的时间范围内 则返回真
  }

  private void deleteDataInFiles(    //对该存储组下的所有顺序（乱序）TsFile中存在待删除的设备的在待删除时间范围内的数据进行删除操作，1. 首先往本地对应的.mods文件记录此删除操作  2. 若某TsFile的内存workMemTable中存在数据，则根据该workMemTable是未flush还是正在flushing进行相关的删除操作
      Collection<TsFileResource> tsFileResourceList,   //该存储组下的所有顺序的（不顺序的）TsFile的TsFileResource类对象
      Deletion deletion,          //删除操作类对象
      Set<PartialPath> devicePaths,   //待删除路径中包含的所有设备路径对象
      List<ModificationFile> updatedModFiles,
      long planIndex)
      throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {//循环遍历每个TsFileResource
      if (canSkipDelete(  //判断当前TSFile下给定的所有设备中是否存在待删除的数据，若存在则不能跳过删除操作
          tsFileResource, devicePaths, deletion.getStartTime(), deletion.getEndTime())) { //如果当前TsFile能跳过删除操作则跳到下个TsFile
        continue;
      }
      //若当前TSFile的给定设备中存在待删除的数据，则进行下面的操作：
        //1. 往本地对应的.mods文件记录此删除操作
      deletion.setFileOffset(tsFileResource.getTsFileSize());//(1) 设置删除操作里生效位置为当前TsFile的文件大小
      // write deletion into modification file
      tsFileResource.getModFile().write(deletion);  //(2) 往当前TsFile对应的本地.mods文件写入此次删除操作的记录，并往mods文件类ModificationFile对象里写入此次删除操作的相关信息
      // remember to close mod file
      tsFileResource.getModFile().close();  //(3) 关闭此TsFile的修改文件类对象，首先会把writer资源释放，并且把此mods文件类对象的修改操作列表清空
      logger.info(
          "[Deletion] Deletion with path:{}, time:{}-{} written into mods file.",
          deletion.getPath(),
          deletion.getStartTime(),
          deletion.getEndTime());

      tsFileResource.updatePlanIndexes(planIndex);//更新TsFileResource文件里的planIndex字段

      // delete data in memory of unsealed file
        //2. 若此TsFile的内存workMemTable中存在数据，则根据该workMemTable是未flush还是正在flushing进行相关的删除操作
      if (!tsFileResource.isClosed()) { //若当前TsFile是未封口、未关闭的，说明可能还存在那些存放于内存memtable的数据
        TsFileProcessor tsfileProcessor = tsFileResource.getUnsealedFileProcessor();  //获取该未封口TsFile文件的TsFileProcessor
        tsfileProcessor.deleteDataInMemory(deletion, devicePaths);//根据指定的设备路径和删除操作信息(时间范围等)，删除那些还在内存中的数据（包含存在各个传感器里未被flush的memtable的数据和正在被进行flush的memtable的数据）
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }

  private void tryToDeleteLastCache(  //在删除操作时，根据给定的设备判断其下的每个传感器节点是否需要重置清空上个最大时间戳数据点缓存（若该缓存在此删除操作的时间范围里，则清空）
      PartialPath deviceId, PartialPath originalPath, long startTime, long endTime)//originalPath是只确定了存储组的时间序列路径（设备可能不确定）
      throws WriteProcessException {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {  //如果系统没开启允许缓存上个数据点，则返回
      return;
    }
    try {
      IMNode node = IoTDB.metaManager.getDeviceNode(deviceId);  //根据设备路径对象获取对应的设备节点对象

      for (IMNode measurementNode : node.getChildren().values()) {  //依次遍历该设备下的所有传感器节点对象
        if (measurementNode != null
            && originalPath.matchFullPath(measurementNode.getPartialPath())) {//若传感器节点对象不为空且该具体的传感器路径与只确定了存储组的通配originalPath路径匹配，则
          TimeValuePair lastPair = ((IMeasurementMNode) measurementNode).getCachedLast();//获取该传感器节点对象缓存的上个最大时间戳的数据点
          if (lastPair != null        //若上个缓存数据点不为空，并且其时间戳在这次待删除的时间范围内
              && startTime <= lastPair.getTimestamp()
              && lastPair.getTimestamp() <= endTime) {
            ((IMeasurementMNode) measurementNode).resetCache();//把该传感器节点对象的上个数据点缓存清空
            logger.info(
                "[tryToDeleteLastCache] Last cache for path: {} is set to null",
                measurementNode.getFullPath());
          }
        }
      }
    } catch (MetadataException e) {
      throw new WriteProcessException(e);
    }
  }

  /**
   * when close an TsFileProcessor, update its EndTimeMap immediately
   *
   * @param tsFileProcessor processor to be closed
   */                                                         //由于每个时区有自己的TsFile，每个TsFile文件对应有自己的TsFileProcessor和TsFileResource，TsFileResource记录了该文件里不同设备写入数据的时间戳范围，因此
  private void updateEndTimeMap(TsFileProcessor tsFileProcessor) {  //对于要被关闭的TsFileProcessor，要即时更新它的TsFileResource对象里记录的每个设备的最后写入时间戳。
    TsFileResource resource = tsFileProcessor.getTsFileResource();  //获取TsFileProcessor对应的TsFileResource
    for (String deviceId : resource.getDevices()) { //更新每个设备的最后写入的数据时间戳
      resource.updateEndTime(
          deviceId, latestTimeForEachDevice.get(tsFileProcessor.getTimeRangeId()).get(deviceId));
    }
  }

  private boolean unsequenceFlushCallback(TsFileProcessor processor) {
    return true;
  }

  private boolean updateLatestFlushTimeCallback(TsFileProcessor processor) {
    // update the largest timestamp in the last flushing memtable
    Map<String, Long> curPartitionDeviceLatestTime =
        latestTimeForEachDevice.get(processor.getTimeRangeId());

    if (curPartitionDeviceLatestTime == null) {
      logger.warn(
          "Partition: {} does't have latest time for each device. "
              + "No valid record is written into memtable. Flushing tsfile is: {}",
          processor.getTimeRangeId(),
          processor.getTsFileResource().getTsFile());
      return false;
    }

    for (Entry<String, Long> entry : curPartitionDeviceLatestTime.entrySet()) {
      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(processor.getTimeRangeId(), id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
      updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
          processor.getTimeRangeId(), entry.getKey(), entry.getValue());
      if (globalLatestFlushedTimeForEachDevice.getOrDefault(entry.getKey(), Long.MIN_VALUE)
          < entry.getValue()) {
        globalLatestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
    }
    return true;
  }

  /**
   * update latest flush time for partition id
   *
   * @param partitionId partition id
   * @param latestFlushTime lastest flush time
   * @return true if update latest flush time success
   */
  private boolean updateLatestFlushTimeToPartition(long partitionId, long latestFlushTime) {
    // update the largest timestamp in the last flushing memtable
    Map<String, Long> curPartitionDeviceLatestTime = latestTimeForEachDevice.get(partitionId);

    if (curPartitionDeviceLatestTime == null) {
      logger.warn(
          "Partition: {} does't have latest time for each device. "
              + "No valid record is written into memtable.  latest flush time is: {}",
          partitionId,
          latestFlushTime);
      return false;
    }

    for (Entry<String, Long> entry : curPartitionDeviceLatestTime.entrySet()) {
      // set lastest flush time to latestTimeForEachDevice
      entry.setValue(latestFlushTime);

      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(partitionId, id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
      newlyFlushedPartitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(partitionId, id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
      if (globalLatestFlushedTimeForEachDevice.getOrDefault(entry.getKey(), Long.MIN_VALUE)
          < entry.getValue()) {
        globalLatestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
    }
    return true;
  }

  /** used for upgrading */
  public void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(//在文件升级成功后，调用此方法来记录每一个新TsFile文件所属时间分区的每个设备的最大时间戳，用于最终更新partitionLatestFlushedTimeForEachDevice
      long partitionId, String deviceId, long time) {
    newlyFlushedPartitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(partitionId, id -> new HashMap<>())
        .compute(deviceId, (k, v) -> v == null ? time : Math.max(v, time));
  }

  /** put the memtable back to the MemTablePool and make the metadata in writer visible */
  // TODO please consider concurrency with query and insert method.
  private void closeUnsealedTsFileProcessorCallBack(TsFileProcessor tsFileProcessor)
      throws TsFileProcessorException {
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
    } finally {
      closeQueryLock.writeLock().unlock();
    }
    // closingSequenceTsFileProcessor is a thread safety class.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
    }
    synchronized (closeStorageGroupCondition) {
      closeStorageGroupCondition.notifyAll();
    }
    logger.info(
        "signal closing storage group condition in {}",
        logicalStorageGroupName + "-" + virtualStorageGroupId);

    executeCompaction(
        tsFileProcessor.getTimeRangeId(),
        IoTDBDescriptor.getInstance().getConfig().isForceFullMerge());
  }

  private void executeCompaction(long timePartition, boolean fullMerge) {
    if (!compactionMergeWorking && !CompactionMergeTaskPoolManager.getInstance().isTerminated()) {
      compactionMergeWorking = true;
      logger.info(
          "{} submit a compaction merge task",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
      try {
        // fork and filter current tsfile, then commit then to compaction merge
        tsFileManagement.forkCurrentFileList(timePartition);
        tsFileManagement.setForceFullMerge(fullMerge);
        CompactionMergeTaskPoolManager.getInstance()
            .submitTask(
                logicalStorageGroupName,
                tsFileManagement
                .new CompactionMergeTask(this::closeCompactionMergeCallBack, timePartition));
      } catch (IOException | RejectedExecutionException e) {
        this.closeCompactionMergeCallBack(false, timePartition);
        logger.error(
            "{} compaction submit task failed",
            logicalStorageGroupName + "-" + virtualStorageGroupId,
            e);
      }
    } else {
      logger.info(
          "{} last compaction merge task is working, skip current merge",
          logicalStorageGroupName + "-" + virtualStorageGroupId);
    }
  }

  /** close compaction merge callback, to release some locks */
  private void closeCompactionMergeCallBack(boolean isMerge, long timePartitionId) {
    if (isMerge && IoTDBDescriptor.getInstance().getConfig().isEnableContinuousCompaction()) {
      executeCompaction(
          timePartitionId, IoTDBDescriptor.getInstance().getConfig().isForceFullMerge());
    } else {
      this.compactionMergeWorking = false;
    }
  }

  /**
   * count all Tsfiles in the storage group which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded in the storage group
   */
  public int countUpgradeFiles() {//计算该存储下的该虚拟存储组下对应的待升级的TSFile文件数量
    return upgradeFileCount.get();//返回待升级的TSFile文件数量
  }

  /** upgrade all files belongs to this storage group */
  public void upgrade() {//使用该存储组下的该虚拟存储组的StorageGroupProcessor对象进行升级文件
    for (TsFileResource seqTsFileResource : upgradeSeqFileList) { //遍历待升级的顺序TSFile文件的TsFileResource对象
      seqTsFileResource.setSeq(true);//设置为顺序
      seqTsFileResource.setUpgradeTsFileResourceCallBack(this::upgradeTsFileResourceCallBack);//升级完该TsFileResource文件后进行回调的函数
      seqTsFileResource.doUpgrade();  //进行升级该TSFileResource
    }
    for (TsFileResource unseqTsFileResource : upgradeUnseqFileList) {//遍历待升级的乱序TSFile文件的TsFileResource对象
      unseqTsFileResource.setSeq(false);
      unseqTsFileResource.setUpgradeTsFileResourceCallBack(this::upgradeTsFileResourceCallBack);
      unseqTsFileResource.doUpgrade();
    }
  }

  private void upgradeTsFileResourceCallBack(TsFileResource tsFileResource) {//升级完指定的TSFile文件的TsFileResource文件后进行回调的函数，此参数是旧的TsFile文件对应的TsFileResource
    List<TsFileResource> upgradedResources = tsFileResource.getUpgradedResources(); //获取所有升级完后生成的新TsFile对应的TsFileResource
    for (TsFileResource resource : upgradedResources) { //遍历每个新TsFile对应的TsFileResource
      long partitionId = resource.getTimePartition(); //获取该新TsFile所属时间分区
      resource
          .getDevices()
          .forEach(
              device ->
                  updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
                      partitionId, device, resource.getEndTime(device)));   //在文件升级成功后，记录每一个新TsFile文件所属时间分区的每个设备的最大时间戳，用于最终更新partitionLatestFlushedTimeForEachDevice
    }
    upgradeFileCount.getAndAdd(-1); //将待升级的旧的TsFile文件数量减1
    // load all upgraded resources in this sg to tsFileManagement
    if (upgradeFileCount.get() == 0) {    //如果该虚拟存储组下所有的旧TsFile都已经升级完毕了，则
      writeLock("upgradeTsFileResourceCallBack");
      try {
        loadUpgradedResources(upgradeSeqFileList, true);//遍历每个旧TsFileResource，对每个旧TsFile文件升级后生成的每个新TsFile的TsFileResouce做如下操作：（1）创建本地虚拟存储组目录下对应的时间分区目录，并将该新TsFile和对应新.mods文件移动到此时间分区目录下（2）将本地新的.mods文件和新TsFile文件反序列化到该TsFileResource对象的属性里，然后关闭该新TsFileResource，并把它的内容序列化写到本地的.resource文件里。  然后删除本地该旧的TsFile文件和对应的旧.resource和.mods文件。最后往升级日志里写入该旧TsFile对应的升级状态为3，代表升级操作全部结束！
        loadUpgradedResources(upgradeUnseqFileList, false);
      } finally {
        writeUnlock();
      }
      // after upgrade complete, update partitionLatestFlushedTimeForEachDevice
      for (Entry<Long, Map<String, Long>> entry :
          newlyFlushedPartitionLatestFlushedTimeForEachDevice.entrySet()) {
        long timePartitionId = entry.getKey();
        Map<String, Long> latestFlushTimeForPartition =//获取原来的该时间分区下每个设备的最大时间戳
            partitionLatestFlushedTimeForEachDevice.getOrDefault(timePartitionId, new HashMap<>());
        for (Entry<String, Long> endTimeMap : entry.getValue().entrySet()) {  //获取更新过程中所记录的该时间分区下每个设备的最大时间戳
          String device = endTimeMap.getKey();
          long endTime = endTimeMap.getValue();
          if (latestFlushTimeForPartition.getOrDefault(device, Long.MIN_VALUE) < endTime) { //若更新过程中记录的该时间分区下此设备的最大时间戳大于原先记录的该时间分区的该设备的最大时间戳，则更新原先记录的该时间分区的该设备的最大时间戳
            partitionLatestFlushedTimeForEachDevice
                .computeIfAbsent(timePartitionId, id -> new HashMap<>())
                .put(device, endTime);
          }
        }
      }
    }
  }

  private void loadUpgradedResources(List<TsFileResource> resources, boolean isseq) {//参数是该虚拟存储组目录下旧的顺序or乱序TsFile对应的TsFileResource列表。遍历每个旧TsFileResource，对每个旧TsFile文件升级后生成的每个新TsFile的TsFileResouce做如下操作：（1）创建本地虚拟存储组目录下对应的时间分区目录，并将该新TsFile和对应新.mods文件移动到此时间分区目录下（2）将本地新的.mods文件和新TsFile文件反序列化到该TsFileResource对象的属性里，然后关闭该新TsFileResource，并把它的内容序列化写到本地的.resource文件里。  然后删除本地该旧的TsFile文件和对应的旧.resource和.mods文件。最后往升级日志里写入该旧TsFile对应的升级状态为3，代表升级操作全部结束！
    if (resources.isEmpty()) {
      return;
    }
    for (TsFileResource resource : resources) {//遍历每个旧的TsFile对应的TsFileResource
      resource.writeLock();
      try {
        UpgradeUtils.moveUpgradedFiles(resource);//参数是旧TsFile的TsFileResource，遍历该旧TsFile文件升级后生成的每个新TsFile的TsFileResouce：（1）创建本地虚拟存储组目录下对应的时间分区目录，并将该新TsFile和对应新.mods文件移动到此时间分区目录下（2）将本地新的.mods文件和新TsFile文件反序列化到该TsFileResource对象的属性里，然后关闭该新TsFileResource，并把它的内容序列化写到本地的.resource文件里
        tsFileManagement.addAll(resource.getUpgradedResources(), isseq);//往TsFile文件管理器增加这些升级后生成的新TsFile对应的TsFileResource
        // delete old TsFile and resource
        resource.delete();//删除本地该旧的TsFile文件和对应的旧.resource文件
        Files.deleteIfExists( //删除对应的旧.mods文件
            fsFactory
                .getFile(resource.getTsFile().toPath() + ModificationFile.FILE_SUFFIX)
                .toPath());
        UpgradeLog.writeUpgradeLogFile(       //往升级日志里写入该旧TsFile对应的升级状态为3，代表升级操作全部结束！
            resource.getTsFile().getAbsolutePath() + "," + UpgradeCheckStatus.UPGRADE_SUCCESS);
      } catch (IOException e) {
        logger.error("Unable to load {}, caused by ", resource, e);
      } finally {
        resource.writeUnlock();
      }
    }
    // delete upgrade folder when it is empty
    if (resources.get(0).getTsFile().getParentFile().isDirectory()
        && resources.get(0).getTsFile().getParentFile().listFiles().length == 0) {
      try {
        Files.delete(resources.get(0).getTsFile().getParentFile().toPath());
      } catch (IOException e) {
        logger.error(
            "Delete upgrade folder {} failed, caused by ",
            resources.get(0).getTsFile().getParentFile(),
            e);
      }
    }
    resources.clear();
  }

  /**
   * merge file under this storage group processor
   *
   * @param isFullMerge whether this merge is a full merge or not
   */
  public void merge(boolean isFullMerge) {
    writeLock("merge");
    try {
      for (long timePartitionId : partitionLatestFlushedTimeForEachDevice.keySet()) {
        executeCompaction(timePartitionId, isFullMerge);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files.
   *
   * <p>or unsequence list.
   *
   * <p>Secondly, execute the loading process by the type.
   *
   * <p>Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource @UsedBy sync module.
   */
  public void loadNewTsFileForSync(TsFileResource newTsFileResource) throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock("loadNewTsFileForSync");
    try {
      if (loadTsFileByType(
          LoadTsFileType.LOAD_SEQUENCE,
          tsfileToBeInserted,
          newTsFileResource,
          newFilePartitionId)) {
        updateLatestTimeMap(newTsFileResource);
      }
      resetLastCacheWhenLoadingTsfile(newTsFileResource);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(),
          tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new LoadFileException(e);
    } catch (IllegalPathException e) {
      logger.error(
          "Failed to reset last cache when loading file {}", newTsFileResource.getTsFilePath());
      throw new LoadFileException(e);
    } finally {
      writeUnlock();
    }
  }

  private void resetLastCacheWhenLoadingTsfile(TsFileResource newTsFileResource)
      throws IllegalPathException {
    for (String device : newTsFileResource.getDevices()) {
      tryToDeleteLastCacheByDevice(new PartialPath(device));
    }
  }

  private void tryToDeleteLastCacheByDevice(PartialPath deviceId) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    try {
      IMNode node = IoTDB.metaManager.getDeviceNode(deviceId);

      for (IMNode measurementNode : node.getChildren().values()) {
        if (measurementNode != null) {
          ((IMeasurementMNode) measurementNode).resetCache();
          logger.debug(
              "[tryToDeleteLastCacheByDevice] Last cache for path: {} is set to null",
              measurementNode.getFullPath());
        }
      }
    } catch (MetadataException e) {
      // the path doesn't cache in cluster mode now, ignore
    }
  }

  /**
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files.
   *
   * <p>that there has no file which is overlapping with the new file.
   *
   * <p>Firstly, determine the loading type of the file, whether it needs to be loaded in sequence
   * list or unsequence list.
   *
   * <p>Secondly, execute the loading process by the type.
   *
   * <p>Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource @UsedBy load external tsfile module
   */
  public void loadNewTsFile(TsFileResource newTsFileResource) throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock("loadNewTsFile");
    try {
      List<TsFileResource> sequenceList = tsFileManagement.getTsFileList(true);

      int insertPos = findInsertionPosition(newTsFileResource, newFilePartitionId, sequenceList);
      String newFileName, renameInfo;
      LoadTsFileType tsFileType;

      // loading tsfile by type
      if (insertPos == POS_OVERLAP) {
        newFileName =
            getNewTsFileName(
                System.currentTimeMillis(),
                getAndSetNewVersion(newFilePartitionId, newTsFileResource),
                0,
                0);
        renameInfo = IoTDBConstant.UNSEQUENCE_FLODER_NAME;
        tsFileType = LoadTsFileType.LOAD_UNSEQUENCE;
        newTsFileResource.setSeq(false);
      } else {
        // check whether the file name needs to be renamed.
        newFileName = getFileNameForSequenceLoadingFile(insertPos, newTsFileResource, sequenceList);
        renameInfo = IoTDBConstant.SEQUENCE_FLODER_NAME;
        tsFileType = LoadTsFileType.LOAD_SEQUENCE;
        newTsFileResource.setSeq(true);
      }

      if (!newFileName.equals(tsfileToBeInserted.getName())) {
        logger.info(
            "TsFile {} must be renamed to {} for loading into the " + renameInfo + " list.",
            tsfileToBeInserted.getName(),
            newFileName);
        newTsFileResource.setFile(
            fsFactory.getFile(tsfileToBeInserted.getParentFile(), newFileName));
      }
      loadTsFileByType(tsFileType, tsfileToBeInserted, newTsFileResource, newFilePartitionId);
      resetLastCacheWhenLoadingTsfile(newTsFileResource);

      // update latest time map
      updateLatestTimeMap(newTsFileResource);
      long partitionNum = newTsFileResource.getTimePartition();
      updatePartitionFileVersion(partitionNum, newTsFileResource.getVersion());
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(),
          tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new LoadFileException(e);
    } catch (IllegalPathException e) {
      logger.error(
          "Failed to reset last cache when loading file {}", newTsFileResource.getTsFilePath());
      throw new LoadFileException(e);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set the version in "partition" to "version" if "version" is larger than the current version.
   */
  public void setPartitionFileVersionToMax(long partition, long version) {
    partitionMaxFileVersions.compute(
        partition, (prt, oldVer) -> computeMaxVersion(oldVer, version));
  }

  private long computeMaxVersion(Long oldVersion, Long newVersion) {
    if (oldVersion == null) {
      return newVersion;
    }
    return Math.max(oldVersion, newVersion);
  }

  /**
   * Find the position of "newTsFileResource" in the sequence files if it can be inserted into them.
   *
   * @return POS_ALREADY_EXIST(- 2) if some file has the same name as the one to be inserted
   *     POS_OVERLAP(-3) if some file overlaps the new file an insertion position i >= -1 if the new
   *     file can be inserted between [i, i+1]
   */
  private int findInsertionPosition(
      TsFileResource newTsFileResource,
      long newFilePartitionId,
      List<TsFileResource> sequenceList) {

    int insertPos = -1;

    // find the position where the new file should be inserted
    for (int i = 0; i < sequenceList.size(); i++) {
      TsFileResource localFile = sequenceList.get(i);
      long localPartitionId = Long.parseLong(localFile.getTsFile().getParentFile().getName());

      if (newFilePartitionId > localPartitionId) {
        insertPos = i;
        continue;
      }

      if (!localFile.isClosed() && localFile.getProcessor() != null) {
        // we cannot compare two files by TsFileResource unless they are both closed
        syncCloseOneTsFileProcessor(true, localFile.getProcessor());
      }
      int fileComparison = compareTsFileDevices(newTsFileResource, localFile);
      switch (fileComparison) {
        case 0:
          // some devices are newer but some devices are older, the two files overlap in general
          return POS_OVERLAP;
        case -1:
          // all devices in localFile are newer than the new file, the new file can be
          // inserted before localFile
          return i - 1;
        default:
          // all devices in the local file are older than the new file, proceed to the next file
          insertPos = i;
      }
    }
    return insertPos;
  }

  /**
   * Compare each device in the two files to find the time relation of them.
   *
   * @return -1 if fileA is totally older than fileB (A < B) 0 if fileA is partially older than
   *     fileB and partially newer than fileB (A X B) 1 if fileA is totally newer than fileB (B < A)
   */
  private int compareTsFileDevices(TsFileResource fileA, TsFileResource fileB) {
    boolean hasPre = false, hasSubsequence = false;
    for (String device : fileA.getDevices()) {
      if (!fileB.getDevices().contains(device)) {
        continue;
      }
      long startTimeA = fileA.getStartTime(device);
      long endTimeA = fileA.getEndTime(device);
      long startTimeB = fileB.getStartTime(device);
      long endTimeB = fileB.getEndTime(device);
      if (startTimeA > endTimeB) {
        // A's data of the device is later than to the B's data
        hasPre = true;
      } else if (startTimeB > endTimeA) {
        // A's data of the device is previous to the B's data
        hasSubsequence = true;
      } else {
        // the two files overlap in the device
        return 0;
      }
    }
    if (hasPre && hasSubsequence) {
      // some devices are newer but some devices are older, the two files overlap in general
      return 0;
    }
    if (!hasPre && hasSubsequence) {
      // all devices in B are newer than those in A
      return -1;
    }
    // all devices in B are older than those in A
    return 1;
  }

  /**
   * If the historical versions of a file is a sub-set of the given file's, (close and) remove it to
   * reduce unnecessary merge. Only used when the file sender and the receiver share the same file
   * close policy. Warning: DO NOT REMOVE
   */
  @SuppressWarnings("unused")
  public void removeFullyOverlapFiles(TsFileResource resource) {
    writeLock("removeFullyOverlapFiles");
    try {
      Iterator<TsFileResource> iterator = tsFileManagement.getIterator(true);
      removeFullyOverlapFiles(resource, iterator, true);

      iterator = tsFileManagement.getIterator(false);
      removeFullyOverlapFiles(resource, iterator, false);
    } finally {
      writeUnlock();
    }
  }

  private void removeFullyOverlapFiles(
      TsFileResource newTsFile, Iterator<TsFileResource> iterator, boolean isSeq) {
    while (iterator.hasNext()) {
      TsFileResource existingTsFile = iterator.next();
      if (newTsFile.isPlanRangeCovers(existingTsFile)
          && !newTsFile.getTsFile().equals(existingTsFile.getTsFile())
          && existingTsFile.tryWriteLock()) {
        logger.info(
            "{} is covered by {}: [{}, {}], [{}, {}], remove it",
            existingTsFile,
            newTsFile,
            existingTsFile.minPlanIndex,
            existingTsFile.maxPlanIndex,
            newTsFile.minPlanIndex,
            newTsFile.maxPlanIndex);
        // if we fail to lock the file, it means it is being queried or merged and we will not
        // wait until it is free, we will just leave it to the next merge
        try {
          removeFullyOverlapFile(existingTsFile, iterator, isSeq);
        } catch (Exception e) {
          logger.error(
              "Something gets wrong while removing FullyOverlapFiles: {}",
              existingTsFile.getTsFile().getAbsolutePath(),
              e);
        } finally {
          existingTsFile.writeUnlock();
        }
      }
    }
  }

  /**
   * remove the given tsFileResource. If the corresponding tsFileProcessor is in the working status,
   * close it before remove the related resource files. maybe time-consuming for closing a tsfile.
   */
  private void removeFullyOverlapFile(
      TsFileResource tsFileResource, Iterator<TsFileResource> iterator, boolean isSeq) {
    logger.info(
        "Removing a covered file {}, closed: {}", tsFileResource, tsFileResource.isClosed());
    if (!tsFileResource.isClosed()) {
      try {
        // also remove the TsFileProcessor if the overlapped file is not closed
        long timePartition = tsFileResource.getTimePartition();
        Map<Long, TsFileProcessor> fileProcessorMap =
            isSeq ? workSequenceTsFileProcessors : workUnsequenceTsFileProcessors;
        TsFileProcessor tsFileProcessor = fileProcessorMap.get(timePartition);
        if (tsFileProcessor != null && tsFileProcessor.getTsFileResource() == tsFileResource) {
          // have to take some time to close the tsFileProcessor
          tsFileProcessor.syncClose();
          fileProcessorMap.remove(timePartition);
        }
      } catch (Exception e) {
        logger.error("Cannot close {}", tsFileResource, e);
      }
    }
    tsFileManagement.remove(tsFileResource, isSeq);
    iterator.remove();
    tsFileResource.remove();
  }

  /**
   * Get an appropriate filename to ensure the order between files. The tsfile is named after
   * ({systemTime}-{versionNum}-{in_space_compaction_num}-{cross_space_compaction_num}.tsfile).
   *
   * <p>The sorting rules for tsfile names @see {@link this#compareFileName}, we can restore the
   * list based on the file name and ensure the correctness of the order, so there are three cases.
   *
   * <p>1. The tsfile is to be inserted in the first place of the list. Timestamp can be set to half
   * of the timestamp value in the file name of the first tsfile in the list , and the version
   * number will be updated to the largest number in this time partition.
   *
   * <p>2. The tsfile is to be inserted in the last place of the list. The file name is generated by
   * the system according to the naming rules and returned.
   *
   * <p>3. This file is inserted between two files. The time stamp is the mean of the timestamps of
   * the two files, the version number will be updated to the largest number in this time partition.
   *
   * @param insertIndex the new file will be inserted between the files [insertIndex, insertIndex +
   *     1]
   * @return appropriate filename
   */
  private String getFileNameForSequenceLoadingFile(
      int insertIndex, TsFileResource newTsFileResource, List<TsFileResource> sequenceList)
      throws LoadFileException {
    long timePartitionId = newTsFileResource.getTimePartition();
    long preTime, subsequenceTime;

    if (insertIndex == -1) {
      preTime = 0L;
    } else {
      String preName = sequenceList.get(insertIndex).getTsFile().getName();
      preTime = Long.parseLong(preName.split(FILE_NAME_SEPARATOR)[0]);
    }
    if (insertIndex == tsFileManagement.size(true) - 1) {
      subsequenceTime = preTime + ((System.currentTimeMillis() - preTime) << 1);
    } else {
      String subsequenceName = sequenceList.get(insertIndex + 1).getTsFile().getName();
      subsequenceTime = Long.parseLong(subsequenceName.split(FILE_NAME_SEPARATOR)[0]);
    }

    long meanTime = preTime + ((subsequenceTime - preTime) >> 1);
    if (insertIndex != tsFileManagement.size(true) - 1 && meanTime == subsequenceTime) {
      throw new LoadFileException("can not load TsFile because of can not find suitable location");
    }

    return getNewTsFileName(
        meanTime, getAndSetNewVersion(timePartitionId, newTsFileResource), 0, 0);
  }

  private long getAndSetNewVersion(long timePartitionId, TsFileResource tsFileResource) {
    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, -1L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    tsFileResource.setVersion(version);
    return version;
  }

  /**
   * Update latest time in latestTimeForEachDevice and
   * partitionLatestFlushedTimeForEachDevice. @UsedBy sync module, load external tsfile module.
   */
  private void updateLatestTimeMap(TsFileResource newTsFileResource) {
    for (String device : newTsFileResource.getDevices()) {
      long endTime = newTsFileResource.getEndTime(device);
      long timePartitionId = StorageEngine.getTimePartition(endTime);
      if (!latestTimeForEachDevice
              .computeIfAbsent(timePartitionId, id -> new HashMap<>())
              .containsKey(device)
          || latestTimeForEachDevice.get(timePartitionId).get(device) < endTime) {
        latestTimeForEachDevice.get(timePartitionId).put(device, endTime);
      }

      Map<String, Long> latestFlushTimeForPartition =
          partitionLatestFlushedTimeForEachDevice.getOrDefault(timePartitionId, new HashMap<>());

      if (latestFlushTimeForPartition.getOrDefault(device, Long.MIN_VALUE) < endTime) {
        partitionLatestFlushedTimeForEachDevice
            .computeIfAbsent(timePartitionId, id -> new HashMap<>())
            .put(device, endTime);
      }
      if (globalLatestFlushedTimeForEachDevice.getOrDefault(device, Long.MIN_VALUE) < endTime) {
        globalLatestFlushedTimeForEachDevice.put(device, endTime);
      }
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param type load type
   * @param tsFileResource tsfile resource to be loaded
   * @param filePartitionId the partition id of the new file
   * @return load the file successfully @UsedBy sync module, load external tsfile module.
   */
  private boolean loadTsFileByType(
      LoadTsFileType type, File tsFileToLoad, TsFileResource tsFileResource, long filePartitionId)
      throws LoadFileException, DiskSpaceInsufficientException {
    File targetFile;
    switch (type) {
      case LOAD_UNSEQUENCE:
        targetFile =
            fsFactory.getFile(
                DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
                logicalStorageGroupName
                    + File.separatorChar
                    + virtualStorageGroupId
                    + File.separatorChar
                    + filePartitionId
                    + File.separator
                    + tsFileResource.getTsFile().getName());
        tsFileResource.setFile(targetFile);
        if (tsFileManagement.contains(tsFileResource, false)) {
          logger.error("The file {} has already been loaded in unsequence list", tsFileResource);
          return false;
        }
        tsFileManagement.add(tsFileResource, false);
        logger.info(
            "Load tsfile in unsequence list, move file from {} to {}",
            tsFileToLoad.getAbsolutePath(),
            targetFile.getAbsolutePath());
        break;
      case LOAD_SEQUENCE:
        targetFile =
            fsFactory.getFile(
                DirectoryManager.getInstance().getNextFolderForSequenceFile(),
                logicalStorageGroupName
                    + File.separatorChar
                    + virtualStorageGroupId
                    + File.separatorChar
                    + filePartitionId
                    + File.separator
                    + tsFileResource.getTsFile().getName());
        tsFileResource.setFile(targetFile);
        if (tsFileManagement.contains(tsFileResource, true)) {
          logger.error("The file {} has already been loaded in sequence list", tsFileResource);
          return false;
        }
        tsFileManagement.add(tsFileResource, true);
        logger.info(
            "Load tsfile in sequence list, move file from {} to {}",
            tsFileToLoad.getAbsolutePath(),
            targetFile.getAbsolutePath());
        break;
      default:
        throw new LoadFileException(String.format("Unsupported type of loading tsfile : %s", type));
    }

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    try {
      FileUtils.moveFile(tsFileToLoad, targetFile);
    } catch (IOException e) {
      logger.error(
          "File renaming failed when loading tsfile. Origin: {}, Target: {}",
          tsFileToLoad.getAbsolutePath(),
          targetFile.getAbsolutePath(),
          e);
      throw new LoadFileException(
          String.format(
              "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s",
              tsFileToLoad.getAbsolutePath(), targetFile.getAbsolutePath(), e.getMessage()));
    }

    File resourceFileToLoad =
        fsFactory.getFile(tsFileToLoad.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    File targetResourceFile =
        fsFactory.getFile(targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      FileUtils.moveFile(resourceFileToLoad, targetResourceFile);
    } catch (IOException e) {
      logger.error(
          "File renaming failed when loading .resource file. Origin: {}, Target: {}",
          resourceFileToLoad.getAbsolutePath(),
          targetResourceFile.getAbsolutePath(),
          e);
      throw new LoadFileException(
          String.format(
              "File renaming failed when loading .resource file. Origin: %s, Target: %s, because %s",
              resourceFileToLoad.getAbsolutePath(),
              targetResourceFile.getAbsolutePath(),
              e.getMessage()));
    }

    File modFileToLoad =
        fsFactory.getFile(tsFileToLoad.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
    if (modFileToLoad.exists()) {
      // when successfully loaded, the filepath of the resource will be changed to the IoTDB data
      // dir, so we can add a suffix to find the old modification file.
      File targetModFile =
          fsFactory.getFile(targetFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
      try {
        Files.deleteIfExists(targetFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete localModFile {}", targetModFile, e);
      }
      try {
        FileUtils.moveFile(modFileToLoad, targetModFile);
      } catch (IOException e) {
        logger.error(
            "File renaming failed when loading .mod file. Origin: {}, Target: {}",
            resourceFileToLoad.getAbsolutePath(),
            targetModFile.getAbsolutePath(),
            e);
        throw new LoadFileException(
            String.format(
                "File renaming failed when loading .mod file. Origin: %s, Target: %s, because %s",
                resourceFileToLoad.getAbsolutePath(),
                targetModFile.getAbsolutePath(),
                e.getMessage()));
      } finally {
        // ModFile will be updated during the next call to `getModFile`
        tsFileResource.setModFile(null);
      }
    }

    updatePartitionFileVersion(filePartitionId, tsFileResource.getVersion());
    return true;
  }

  /**
   * Delete tsfile if it exists.
   *
   * <p>Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   *
   * <p>Secondly, delete the tsfile and .resource file.
   *
   * @param tsfieToBeDeleted tsfile to be deleted
   * @return whether the file to be deleted exists. @UsedBy sync module, load external tsfile
   *     module.
   */
  public boolean deleteTsfile(File tsfieToBeDeleted) {
    writeLock("deleteTsfile");
    TsFileResource tsFileResourceToBeDeleted = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManagement.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(tsfieToBeDeleted.getName())) {
          tsFileResourceToBeDeleted = sequenceResource;
          tsFileManagement.remove(tsFileResourceToBeDeleted, true);
          break;
        }
      }
      if (tsFileResourceToBeDeleted == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManagement.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(tsfieToBeDeleted.getName())) {
            tsFileResourceToBeDeleted = unsequenceResource;
            tsFileManagement.remove(tsFileResourceToBeDeleted, false);
            break;
          }
        }
      }
    } finally {
      writeUnlock();
    }
    if (tsFileResourceToBeDeleted == null) {
      return false;
    }
    tsFileResourceToBeDeleted.writeLock();
    try {
      tsFileResourceToBeDeleted.remove();
      logger.info("Delete tsfile {} successfully.", tsFileResourceToBeDeleted.getTsFile());
    } finally {
      tsFileResourceToBeDeleted.writeUnlock();
    }
    return true;
  }

  /**
   * get all working sequence tsfile processors
   *
   * @return all working sequence tsfile processors
   */
  public Collection<TsFileProcessor> getWorkSequenceTsFileProcessors() {
    return workSequenceTsFileProcessors.values();
  }

  /**
   * Move tsfile to the target directory if it exists.
   *
   * <p>Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   *
   * <p>Secondly, move the tsfile and .resource file to the target directory.
   *
   * @param fileToBeMoved tsfile to be moved
   * @return whether the file to be moved exists. @UsedBy load external tsfile module.
   */
  public boolean moveTsfile(File fileToBeMoved, File targetDir) {
    writeLock("moveTsfile");
    TsFileResource tsFileResourceToBeMoved = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManagement.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(fileToBeMoved.getName())) {
          tsFileResourceToBeMoved = sequenceResource;
          tsFileManagement.remove(tsFileResourceToBeMoved, true);
          break;
        }
      }
      if (tsFileResourceToBeMoved == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManagement.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(fileToBeMoved.getName())) {
            tsFileResourceToBeMoved = unsequenceResource;
            tsFileManagement.remove(tsFileResourceToBeMoved, false);
            break;
          }
        }
      }
    } finally {
      writeUnlock();
    }
    if (tsFileResourceToBeMoved == null) {
      return false;
    }
    tsFileResourceToBeMoved.writeLock();
    try {
      tsFileResourceToBeMoved.moveTo(targetDir);
      logger.info(
          "Move tsfile {} to target dir {} successfully.",
          tsFileResourceToBeMoved.getTsFile(),
          targetDir.getPath());
    } finally {
      tsFileResourceToBeMoved.writeUnlock();
    }
    return true;
  }

  /**
   * get all working unsequence tsfile processors
   *
   * @return all working unsequence tsfile processors
   */
  public Collection<TsFileProcessor> getWorkUnsequenceTsFileProcessors() {
    return workUnsequenceTsFileProcessors.values();
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
    checkFilesTTL(); //检查该虚拟存储组下所有顺序和乱序的TsFile的数据是否超过TTL,若有则删除本地相关的文件(TsFile和mods文件和resource文件)
  }

  public List<TsFileResource> getSequenceFileTreeSet() {
    return tsFileManagement.getTsFileList(true);
  }

  public List<TsFileResource> getUnSequenceFileList() {
    return tsFileManagement.getTsFileList(false);
  }

  public String getVirtualStorageGroupId() {
    return virtualStorageGroupId;
  }

  public StorageGroupInfo getStorageGroupInfo() {
    return storageGroupInfo;
  }

  /**
   * Check if the data of "tsFileResource" all exist locally by comparing planIndexes in the
   * partition of "partitionNumber". This is available only when the IoTDB instances which generated
   * "tsFileResource" have the same plan indexes as the local one.
   *
   * @return true if any file contains plans with indexes no less than the max plan index of
   *     "tsFileResource", otherwise false.
   */
  public boolean isFileAlreadyExist(TsFileResource tsFileResource, long partitionNum) {
    // examine working processor first as they have the largest plan index
    return isFileAlreadyExistInWorking(
            tsFileResource, partitionNum, getWorkSequenceTsFileProcessors())
        || isFileAlreadyExistInWorking(
            tsFileResource, partitionNum, getWorkUnsequenceTsFileProcessors())
        || isFileAlreadyExistInClosed(tsFileResource, partitionNum, getSequenceFileTreeSet())
        || isFileAlreadyExistInClosed(tsFileResource, partitionNum, getUnSequenceFileList());
  }

  private boolean isFileAlreadyExistInClosed(
      TsFileResource tsFileResource, long partitionNum, Collection<TsFileResource> existingFiles) {
    for (TsFileResource resource : existingFiles) {
      if (resource.getTimePartition() == partitionNum
          && resource.getMaxPlanIndex() > tsFileResource.getMaxPlanIndex()) {
        logger.info(
            "{} is covered by a closed file {}: [{}, {}] [{}, {}]",
            tsFileResource,
            resource,
            tsFileResource.minPlanIndex,
            tsFileResource.maxPlanIndex,
            resource.minPlanIndex,
            resource.maxPlanIndex);
        return true;
      }
    }
    return false;
  }

  private boolean isFileAlreadyExistInWorking(
      TsFileResource tsFileResource,
      long partitionNum,
      Collection<TsFileProcessor> workingProcessors) {
    for (TsFileProcessor workingProcesssor : workingProcessors) {
      if (workingProcesssor.getTimeRangeId() == partitionNum) {
        TsFileResource workResource = workingProcesssor.getTsFileResource();
        boolean isCovered = workResource.getMaxPlanIndex() > tsFileResource.getMaxPlanIndex();
        if (isCovered) {
          logger.info(
              "{} is covered by a working file {}: [{}, {}] [{}, {}]",
              tsFileResource,
              workResource,
              tsFileResource.minPlanIndex,
              tsFileResource.maxPlanIndex,
              workResource.minPlanIndex,
              workResource.maxPlanIndex);
        }
        return isCovered;
      }
    }
    return false;
  }

  /** remove all partitions that satisfy a filter. */
  public void removePartitions(TimePartitionFilter filter) {
    // this requires blocking all other activities
    writeLock("removePartitions");
    try {
      // abort ongoing comapctions and merges
      CompactionMergeTaskPoolManager.getInstance().abortCompaction(logicalStorageGroupName);
      MergeManager.getINSTANCE().abortMerge(logicalStorageGroupName);
      // close all working files that should be removed
      removePartitions(filter, workSequenceTsFileProcessors.entrySet(), true);
      removePartitions(filter, workUnsequenceTsFileProcessors.entrySet(), false);

      // remove data files
      removePartitions(filter, tsFileManagement.getIterator(true), true);
      removePartitions(filter, tsFileManagement.getIterator(false), false);

    } finally {
      writeUnlock();
    }
  }

  // may remove the processorEntrys
  private void removePartitions(
      TimePartitionFilter filter,
      Set<Entry<Long, TsFileProcessor>> processorEntrys,
      boolean sequence) {
    for (Iterator<Entry<Long, TsFileProcessor>> iterator = processorEntrys.iterator();
        iterator.hasNext(); ) {
      Entry<Long, TsFileProcessor> longTsFileProcessorEntry = iterator.next();
      long partitionId = longTsFileProcessorEntry.getKey();
      TsFileProcessor processor = longTsFileProcessorEntry.getValue();
      if (filter.satisfy(logicalStorageGroupName, partitionId)) {
        processor.syncClose();
        iterator.remove();
        processor.getTsFileResource().remove();
        tsFileManagement.remove(processor.getTsFileResource(), sequence);
        updateLatestFlushTimeToPartition(partitionId, Long.MIN_VALUE);
        logger.debug(
            "{} is removed during deleting partitions",
            processor.getTsFileResource().getTsFilePath());
      }
    }
  }

  // may remove the iterator's data
  private void removePartitions(
      TimePartitionFilter filter, Iterator<TsFileResource> iterator, boolean sequence) {
    while (iterator.hasNext()) {
      TsFileResource tsFileResource = iterator.next();
      if (filter.satisfy(logicalStorageGroupName, tsFileResource.getTimePartition())) {
        tsFileResource.remove();
        tsFileManagement.remove(tsFileResource, sequence);
        updateLatestFlushTimeToPartition(tsFileResource.getTimePartition(), Long.MIN_VALUE);
        logger.debug("{} is removed during deleting partitions", tsFileResource.getTsFilePath());
      }
    }
  }

  public TsFileManagement getTsFileManagement() {
    return tsFileManagement;
  }

  /**
   * insert batch of rows belongs to one device
   *
   * @param insertRowsOfOneDevicePlan batch of rows belongs to one device
   */
  public void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws WriteProcessException, TriggerExecutionException {
    writeLock("InsertRowsOfOneDevice");
    try {
      boolean isSequence = false;
      InsertRowPlan[] rowPlans = insertRowsOfOneDevicePlan.getRowPlans();
      for (int i = 0, rowPlansLength = rowPlans.length; i < rowPlansLength; i++) {

        InsertRowPlan plan = rowPlans[i];
        if (!isAlive(plan.getTime()) || insertRowsOfOneDevicePlan.isExecuted(i)) {
          // we do not need to write these part of data, as they can not be queried
          // or the sub-plan has already been executed, we are retrying other sub-plans
          continue;
        }
        // init map
        long timePartitionId = StorageEngine.getTimePartition(plan.getTime());

        partitionLatestFlushedTimeForEachDevice.computeIfAbsent(
            timePartitionId, id -> new HashMap<>());
        // as the plans have been ordered, and we have get the write lock,
        // So, if a plan is sequenced, then all the rest plans are sequenced.
        //
        if (!isSequence) {
          isSequence =
              plan.getTime()
                  > partitionLatestFlushedTimeForEachDevice
                      .get(timePartitionId)
                      .getOrDefault(plan.getPrefixPath().getFullPath(), Long.MIN_VALUE);
        }
        // is unsequence and user set config to discard out of order data
        if (!isSequence
            && IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
          return;
        }
        latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>());

        // fire trigger before insertion
        TriggerEngine.fire(TriggerEvent.BEFORE_INSERT, plan);
        // insert to sequence or unSequence file
        insertToTsFileProcessor(plan, isSequence, timePartitionId);
        // fire trigger before insertion
        TriggerEngine.fire(TriggerEvent.AFTER_INSERT, plan);
      }
    } finally {
      writeUnlock();
    }
  }

  @TestOnly
  public long getPartitionMaxFileVersions(long partitionId) {
    return partitionMaxFileVersions.getOrDefault(partitionId, -1L);
  }

  public void setCustomCloseFileListeners(List<CloseFileListener> customCloseFileListeners) {
    this.customCloseFileListeners = customCloseFileListeners;
  }

  public void setCustomFlushListeners(List<FlushListener> customFlushListeners) {
    this.customFlushListeners = customFlushListeners;
  }

  private enum LoadTsFileType {
    LOAD_SEQUENCE,
    LOAD_UNSEQUENCE
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {

    void call(TsFileProcessor caller) throws TsFileProcessorException, IOException;
  }

  @FunctionalInterface
  public interface UpdateEndTimeCallBack {

    boolean call(TsFileProcessor caller);
  }

  @FunctionalInterface
  public interface UpgradeTsFileResourceCallBack {//升级完指定的TSFile文件的TsFileResource文件后进行回调的函数对象

    void call(TsFileResource caller);
  }

  @FunctionalInterface
  public interface CloseCompactionMergeCallBack {

    void call(boolean isMergeExecutedInCurrentTask, long timePartitionId);
  }

  @FunctionalInterface
  public interface TimePartitionFilter {

    boolean satisfy(String storageGroupName, long timePartitionId);
  }

  public String getInsertWriteLockHolder() {
    return insertWriteLockHolder;
  }
}
