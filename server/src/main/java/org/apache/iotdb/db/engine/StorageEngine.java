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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.ServerConfigConsistent;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.VirtualStorageGroupManager;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class StorageEngine implements IService { // 负责一个 IoTDB 实例的写入和访问，管理所有的 StorageGroupProsessor
  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long TTL_CHECK_INTERVAL = 60 * 1000L;

  /**
   * Time range for dividing storage group, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  @ServerConfigConsistent private static long timePartitionInterval = -1;
  /** whether enable data partition if disabled, all data belongs to partition 0 */
  @ServerConfigConsistent private static boolean enablePartition = config.isEnablePartition();

  private final boolean enableMemControl = config.isEnableMemControl();

  /**
   * a folder (system/storage_groups/ by default) that persist system info. Each Storage Processor
   * will have a subfolder under the systemDir.
   */
  private final String systemDir =
      FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";

  /** storage group name -> storage group processor */
  private final ConcurrentHashMap<PartialPath, VirtualStorageGroupManager> processorMap =
      new ConcurrentHashMap<>(); // 该属性存放了每个storageGroup路径对象 对应的
  // 虚拟存储组管理类VirtualStorageGroupManager对象

  private AtomicBoolean isAllSgReady = new AtomicBoolean(false);

  private ScheduledExecutorService ttlCheckThread;
  private ScheduledExecutorService seqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService unseqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService tsFileTimedCloseCheckThread;

  private TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();
  private ExecutorService recoveryThreadPool;
  // add customized listeners here for flush and close events
  private List<CloseFileListener> customCloseFileListeners = new ArrayList<>();
  private List<FlushListener> customFlushListeners = new ArrayList<>();

  private StorageEngine() {}

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static void initTimePartition() {
    timePartitionInterval =
        convertMilliWithPrecision(
            IoTDBDescriptor.getInstance().getConfig().getPartitionInterval() * 1000L);
  }

  public static long convertMilliWithPrecision(long milliTime) {
    long result = milliTime;
    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        result = milliTime * 1000_000L;
        break;
      case "us":
        result = milliTime * 1000L;
        break;
      default:
        break;
    }
    return result;
  }

  public static long getTimePartitionInterval() {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return timePartitionInterval;
  }

  @TestOnly
  public static void setTimePartitionInterval(long timePartitionInterval) {
    StorageEngine.timePartitionInterval = timePartitionInterval;
  }

  public static long getTimePartition(long time) { // 时间除以时间间隔来获得时间分区ID
    return enablePartition ? time / timePartitionInterval : 0;
  }

  public static boolean isEnablePartition() {
    return enablePartition;
  }

  @TestOnly
  public static void setEnablePartition(boolean enablePartition) {
    StorageEngine.enablePartition = enablePartition;
  }

  /** block insertion if the insertion is rejected by memory control */
  public static void blockInsertionIfReject(TsFileProcessor tsFileProcessor)
      throws WriteProcessRejectException { // 判断系统是否是阻塞写入，若是则进行等待
    long startTime = System.currentTimeMillis();
    while (SystemInfo.getInstance().isRejected()) { // 循环判断系统是否是阻塞写入，如果是则
      if (tsFileProcessor != null && tsFileProcessor.shouldFlush()) {
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked()); // 将该写入线程循环sleep
        // 50ms，等待flush线程释放内存，system置回正常可写入状态
        if (System.currentTimeMillis() - startTime
            > config
                .getMaxWaitingTimeWhenInsertBlocked()) { // 当等待时间超过max_waiting_time_when_insert_blocked，即系统仍为reject阻塞写入状态，则抛出异常。
          throw new WriteProcessRejectException(
              "System rejected over " + (System.currentTimeMillis() - startTime) + "ms");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isAllSgReady() {
    return isAllSgReady.get();
  }

  public void setAllSgReady(boolean allSgReady) {
    isAllSgReady.set(allSgReady);
  }

  public void recover() {
    setAllSgReady(false);
    recoveryThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), "Recovery-Thread-Pool");

    // recover all logic storage group processors
    List<IStorageGroupMNode> sgNodes = IoTDB.metaManager.getAllStorageGroupNodes();
    List<Future<Void>> futures = new LinkedList<>();
    for (IStorageGroupMNode storageGroup : sgNodes) {
      VirtualStorageGroupManager virtualStorageGroupManager =
          processorMap.computeIfAbsent(
              storageGroup.getPartialPath(), id -> new VirtualStorageGroupManager(true));

      // recover all virtual storage groups in one logic storage group
      virtualStorageGroupManager.asyncRecover(storageGroup, recoveryThreadPool, futures);
    }

    // operations after all virtual storage groups are recovered
    Thread recoverEndTrigger =
        new Thread(
            () -> {
              for (Future<Void> future : futures) {
                try {
                  future.get();
                } catch (ExecutionException e) {
                  throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
                }
              }
              recoveryThreadPool.shutdown();
              setAllSgReady(true);
            });
    recoverEndTrigger.start();
  }

  @Override
  public void start() {
    // build time Interval to divide time partition
    if (!enablePartition) {
      timePartitionInterval = Long.MAX_VALUE;
    } else {
      initTimePartition();
    }

    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }

    // recover upgrade process
    UpgradeUtils.recoverUpgrade();

    recover();

    ttlCheckThread = Executors.newSingleThreadScheduledExecutor();
    ttlCheckThread.scheduleAtFixedRate(
        this::checkTTL, TTL_CHECK_INTERVAL, TTL_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    logger.info("start ttl check thread successfully.");

    startTimedService();
  }

  private void checkTTL() {
    try {
      for (VirtualStorageGroupManager processor : processorMap.values()) {
        processor.checkTTL();
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("An error occurred when checking TTL", e);
    }
  }

  private void startTimedService() {
    // timed flush sequence memtable
    if (config.isEnableTimedFlushSeqMemtable()) {
      seqMemtableTimedFlushCheckThread = Executors.newSingleThreadScheduledExecutor();
      seqMemtableTimedFlushCheckThread.scheduleAtFixedRate(
          this::timedFlushSeqMemTable,
          config.getSeqMemtableFlushCheckInterval(),
          config.getSeqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start sequence memtable timed flush check thread successfully.");
    }
    // timed flush unsequence memtable
    if (config.isEnableTimedFlushUnseqMemtable()) {
      unseqMemtableTimedFlushCheckThread = Executors.newSingleThreadScheduledExecutor();
      unseqMemtableTimedFlushCheckThread.scheduleAtFixedRate(
          this::timedFlushUnseqMemTable,
          config.getUnseqMemtableFlushCheckInterval(),
          config.getUnseqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start unsequence memtable timed flush check thread successfully.");
    }
    // timed close tsfile
    if (config.isEnableTimedCloseTsFile()) {
      tsFileTimedCloseCheckThread = Executors.newSingleThreadScheduledExecutor();
      tsFileTimedCloseCheckThread.scheduleAtFixedRate(
          this::timedCloseTsFileProcessor,
          config.getCloseTsFileCheckInterval(),
          config.getCloseTsFileCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start tsfile timed close check thread successfully.");
    }
  }

  private void timedFlushSeqMemTable() {
    try {
      for (VirtualStorageGroupManager processor : processorMap.values()) {
        processor.timedFlushSeqMemTable();
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed flushing sequence memtables", e);
    }
  }

  private void timedFlushUnseqMemTable() {
    try {
      for (VirtualStorageGroupManager processor : processorMap.values()) {
        processor.timedFlushUnseqMemTable();
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed flushing unsequence memtables", e);
    }
  }

  private void timedCloseTsFileProcessor() {
    try {
      for (VirtualStorageGroupManager processor : processorMap.values()) {
        processor.timedCloseTsFileProcessor();
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed closing tsfiles interval", e);
    }
  }

  @Override
  public void stop() {
    syncCloseAllProcessor();
    stopTimedService(ttlCheckThread, "TTlCheckThread");
    stopTimedService(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    stopTimedService(unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    stopTimedService(tsFileTimedCloseCheckThread, "TsFileTimedCloseCheckThread");
    recoveryThreadPool.shutdownNow();
    for (PartialPath storageGroup : IoTDB.metaManager.getAllStorageGroupPaths()) {
      this.releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroup);
    }
    processorMap.clear();
  }

  private void stopTimedService(ScheduledExecutorService pool, String poolName) {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("{} still doesn't exit after 60s", poolName);
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException(
            String.format("StorageEngine failed to stop because of %s.", poolName), e);
      }
    }
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    shutdownTimedService(ttlCheckThread, "TTlCheckThread");
    shutdownTimedService(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    shutdownTimedService(unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    shutdownTimedService(tsFileTimedCloseCheckThread, "TsFileTimedCloseCheckThread");
    recoveryThreadPool.shutdownNow();
    processorMap.clear();
  }

  private void shutdownTimedService(ScheduledExecutorService pool, String poolName) {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("{} still doesn't exit after 30s", poolName);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** reboot timed flush sequence/unsequence memetable thread, timed close tsfile thread */
  public void rebootTimedService() throws ShutdownException {
    logger.info("Start rebooting all timed service.");

    // exclude ttl check thread
    stopTimedServiceAndThrow(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    stopTimedServiceAndThrow(
        unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    stopTimedServiceAndThrow(tsFileTimedCloseCheckThread, "TsFileTimedCloseCheckThread");

    logger.info("Stop all timed service successfully, and now restart them.");

    startTimedService();

    logger.info("Reboot all timed service successfully");
  }

  private void stopTimedServiceAndThrow(ScheduledExecutorService pool, String poolName)
      throws ShutdownException {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("{} still doesn't exit after 30s", poolName);
        throw new ShutdownException(e);
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  /**
   * This method is for sync, delete tsfile or sth like them, just get storage group directly by sg
   * name
   *
   * @param path storage group path
   * @return storage group processor
   */
  public StorageGroupProcessor getProcessorDirectly(PartialPath path)
      throws StorageEngineException {
    PartialPath storageGroupPath;
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(path);
      storageGroupPath = storageGroupMNode.getPartialPath();
      return getStorageGroupProcessorByPath(storageGroupPath, storageGroupMNode);
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * This method is for insert and query or sth like them, this may get a virtual storage group
   *
   * @param path device path
   * @return storage group processor
   */
  public StorageGroupProcessor getProcessor(PartialPath path)
      throws StorageEngineException { // 根据传过来的设备路径对象来获取对应虚拟存储组的StorageGroupProcessor
    try {
      IStorageGroupMNode storageGroupMNode =
          IoTDB.metaManager.getStorageGroupNodeByPath(path); // 获取设备路径对象获取存储组StorageGroup对象
      return getStorageGroupProcessorByPath(
          path, storageGroupMNode); // 根据设备路径对象和存储组节点对象来获取对应虚拟存储组的StorageGroupProcessor
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * get lock holder for each sg
   *
   * @return storage group processor
   */
  public List<String> getLockInfo(List<PartialPath> pathList) throws StorageEngineException {
    try {
      List<String> lockHolderList = new ArrayList<>(pathList.size());
      for (PartialPath path : pathList) {
        IStorageGroupMNode storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(path);
        StorageGroupProcessor storageGroupProcessor =
            getStorageGroupProcessorByPath(path, storageGroupMNode);
        lockHolderList.add(storageGroupProcessor.getInsertWriteLockHolder());
      }
      return lockHolderList;
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * get storage group processor by device path
   *
   * @param devicePath path of the device
   * @param storageGroupMNode mnode of the storage group, we need synchronize this to avoid
   *     modification in mtree
   * @return found or new storage group processor
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  private StorageGroupProcessor
      getStorageGroupProcessorByPath( // 根据设备路径对象和存储组节点对象来获取对应虚拟存储组的StorageGroupProcessor
      PartialPath devicePath, IStorageGroupMNode storageGroupMNode)
          throws StorageGroupProcessorException, StorageEngineException {
    VirtualStorageGroupManager virtualStorageGroupManager =
        processorMap.get(
            storageGroupMNode
                .getPartialPath()); // 根据存储组路径PartialPath对象获取对应的虚拟存储组管理类VirtualStorageGroupManager对象
    if (virtualStorageGroupManager == null) { // 若获取的虚拟存储组管理类VirtualStorageGroupManager对象为空
      synchronized (this) {
        virtualStorageGroupManager = processorMap.get(storageGroupMNode.getPartialPath());
        if (virtualStorageGroupManager == null) {
          virtualStorageGroupManager = new VirtualStorageGroupManager(); // 则新建一个
          processorMap.put(
              storageGroupMNode.getPartialPath(), virtualStorageGroupManager); // 放入processorMap
        }
      }
    }
    return virtualStorageGroupManager.getProcessor(
        devicePath, storageGroupMNode); // 使用指定存储组的虚拟存储组管理类对象来
    // 根据设备ID计算其属于该真实存储组下的哪个虚拟存储组，并返回该虚拟存储组的StorageGroupProcessor
  }

  /**
   * build a new storage group processor
   *
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public StorageGroupProcessor buildNewStorageGroupProcessor(
      PartialPath logicalStorageGroupName,
      IStorageGroupMNode storageGroupMNode,
      String virtualStorageGroupId) // 根据存储组路径对象、节点类对象和其下的某一虚拟存储组ID来创建该虚拟存储组的StorageGroupProcessor
      throws StorageGroupProcessorException {
    StorageGroupProcessor processor;
    logger.info(
        "construct a processor instance, the storage group is {}, Thread is {}",
        logicalStorageGroupName,
        Thread.currentThread().getId());
    processor = // 新建
        new StorageGroupProcessor(
            systemDir + File.separator + logicalStorageGroupName,
            virtualStorageGroupId,
            fileFlushPolicy,
            storageGroupMNode.getFullPath());
    processor.setDataTTL(storageGroupMNode.getDataTTL());
    processor.setCustomFlushListeners(customFlushListeners);
    processor.setCustomCloseFileListeners(customCloseFileListeners);
    return processor;
  }

  /** This function is just for unit test. */
  @TestOnly
  public synchronized void reset() {
    for (VirtualStorageGroupManager virtualStorageGroupManager : processorMap.values()) {
      virtualStorageGroupManager.reset();
    }
  }

  /**
   * insert an InsertRowPlan to a storage group.
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan) throws StorageEngineException {
    if (enableMemControl) { // 如果开启了内存控制
      try {
        blockInsertionIfReject(null); // 判断系统是否阻塞写入，若是则进行等待，等待若超过一定时间则抛出异常。
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }
    StorageGroupProcessor storageGroupProcessor =
        getProcessor(
            insertRowPlan
                .getPrefixPath()); // 根据设备路径对象来计算出其所属该存储组下的虚拟存储组ID，并获取该虚拟存储组的StorageGroupProcessor

    try {
      storageGroupProcessor.insert(insertRowPlan);
      if (config
          .isEnableStatMonitor()) { // true to enable statistics monitor service, false to disable
        // statistics service
        try {
          updateMonitorStatistics(
              processorMap.get(
                  IoTDB.metaManager.getBelongedStorageGroup(insertRowPlan.getPrefixPath())),
              insertRowPlan);
        } catch (MetadataException e) {
          logger.error("failed to record status", e);
        }
      }
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  public void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws StorageEngineException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }
    StorageGroupProcessor storageGroupProcessor =
        getProcessor(insertRowsOfOneDevicePlan.getPrefixPath());

    // TODO monitor: update statistics
    try {
      storageGroupProcessor.insert(insertRowsOfOneDevicePlan);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /** insert a InsertTabletPlan to a storage group */
  public void insertTablet(InsertTabletPlan insertTabletPlan)
      throws StorageEngineException, BatchProcessException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessRejectException e) {
        TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
        Arrays.fill(results, RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT));
        throw new BatchProcessException(results);
      }
    }
    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertTabletPlan.getPrefixPath());
    } catch (StorageEngineException e) {
      throw new StorageEngineException(
          String.format(
              "Get StorageGroupProcessor of device %s " + "failed",
              insertTabletPlan.getPrefixPath()),
          e);
    }

    storageGroupProcessor.insertTablet(insertTabletPlan);

    if (config.isEnableStatMonitor()) {
      try {
        updateMonitorStatistics(
            processorMap.get(
                IoTDB.metaManager.getBelongedStorageGroup(insertTabletPlan.getPrefixPath())),
            insertTabletPlan);
      } catch (MetadataException e) {
        logger.error("failed to record status", e);
      }
    }
  }

  private void updateMonitorStatistics(
      VirtualStorageGroupManager virtualStorageGroupManager, InsertPlan insertPlan) {
    StatMonitor monitor = StatMonitor.getInstance();
    int successPointsNum =
        insertPlan.getMeasurements().length - insertPlan.getFailedMeasurementNumber();
    // update to storage group statistics
    virtualStorageGroupManager.updateMonitorSeriesValue(successPointsNum);
    // update to global statistics
    monitor.updateStatGlobalValue(successPointsNum);
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (VirtualStorageGroupManager processor : processorMap.values()) {
      processor.syncCloseAllWorkingTsFileProcessors();
    }
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start force closing all storage group processor");
    for (VirtualStorageGroupManager processor : processorMap.values()) {
      processor.forceCloseAllWorkingTsFileProcessors();
    }
  }

  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, boolean isSeq, boolean isSync) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.get(storageGroupPath);
    virtualStorageGroupManager.closeStorageGroupProcessor(isSeq, isSync);
  }

  /**
   * @param storageGroupPath the storage group name
   * @param partitionId the partition id
   * @param isSeq is sequence tsfile or unsequence tsfile
   * @param isSync close tsfile synchronously or asynchronously
   * @throws StorageGroupNotSetException
   */
  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, long partitionId, boolean isSeq, boolean isSync)
      throws StorageGroupNotSetException {
    if (!processorMap.containsKey(storageGroupPath)) {
      throw new StorageGroupNotSetException(storageGroupPath.getFullPath());
    }

    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.get(storageGroupPath);
    virtualStorageGroupManager.closeStorageGroupProcessor(partitionId, isSeq, isSync);
  }

  public void delete( // 注意：时间序列路径可能包含通配符*，因此可能有多个存储组、多个设备,eg:root.*.*.*.*。
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
      TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      List<PartialPath> sgPaths =
          IoTDB.metaManager.getBelongedStorageGroups(path); // 获取此时间序列路径上所有存储组StorageGroup路径对象，存进列表
      for (PartialPath storageGroupPath :
          sgPaths) { // 遍历所有的存储组路径对象，然后依次用每次遍历的存储组去明确时间序列路径里的存储组，最后用该存储组对应的虚拟存储组管理类VirtualStorageGroupManager对象去执行删除操作
        // storage group has no data
        if (!processorMap.containsKey(
            storageGroupPath)) { // 如果不存在该存储组对应的虚拟存储组管理类VirtualStorageGroupManager对象
          continue;
        }

        PartialPath newPath =
            path.alterPrefixPath(
                storageGroupPath); // 此处仍是完整的时间序列,当时间序列路径里的存储组不确定时，给序列路径依次明确具体的存储组。如原先序列路径是root.*.*.*.*后来依次明确成root.ln.*.*.*和root.demo.*.*.*等。如果原来时间序列的存储组就是明确给出的，则此句无作用。
        processorMap
            .get(storageGroupPath)
            .delete(
                newPath,
                startTime,
                endTime,
                planIndex,
                timePartitionFilter); // 使用该存储组对应的虚拟存储组管理类VirtualStorageGroupManager对象去执行删除操作
      }
    } catch (IOException | MetadataException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /** delete data of timeseries "{deviceId}.{measurementId}" */
  public void deleteTimeseries(
      PartialPath path, long planIndex, TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      List<PartialPath> sgPaths = IoTDB.metaManager.getBelongedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        PartialPath newPath = path.alterPrefixPath(storageGroupPath);
        processorMap
            .get(storageGroupPath)
            .delete(newPath, Long.MIN_VALUE, Long.MAX_VALUE, planIndex, timePartitionFilter);
      }
    } catch (IOException | MetadataException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /** query data. */
  public QueryDataSource
      query( // 获取此次查询需要用到的所有顺序or乱序TsFileResource,并把它们往添加入查询文件管理类里，即添加此次查询ID对应需要用到的顺序和乱序TsFileResource,并创建返回QueryDataSource对象，该类对象存放了一次查询里对一条时间序列涉及到的所有顺序TsFileResource和乱序TsFileResource和数据TTL
          SingleSeriesExpression seriesExpression, // 单时间序列一元表达式，它包含了此查询的某一时间序列路径
          QueryContext context, // 此次查询的查询环境
          QueryFileManager filePathsManager) // 查询文件管理类对象，该类存放了每个查询ID对应需要的已封口和为封口的TsFileResource
          throws StorageEngineException, QueryProcessException {
    PartialPath fullPath =
        (PartialPath) seriesExpression.getSeriesPath(); // 获取此次查询的单序列表达式里的此次时间序列路径
    PartialPath deviceId = fullPath.getDevicePath(); // 根据此次查询的此时间序列路径获取对应的设备路径
    StorageGroupProcessor storageGroupProcessor =
        getProcessor(deviceId); // 根据设备路径获取对应的虚拟存储组的StorageGroupProcessor
    return storageGroupProcessor
        .query( // 获取此次查询需要用到的所有顺序or乱序TsFileResource,并把它们往添加入查询文件管理类里，即添加此次查询ID对应需要用到的顺序和乱序TsFileResource,并创建返回QueryDataSource对象，该类对象存放了一次查询里对一条时间序列涉及到的所有顺序TsFileResource和乱序TsFileResource和数据TTL
            fullPath, context, filePathsManager, seriesExpression.getFilter());
  }

  /**
   * count all Tsfiles which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded
   */
  public int countUpgradeFiles() { // 计算待升级的TSFile文件数量
    int totalUpgradeFileNum = 0;
    for (VirtualStorageGroupManager virtualStorageGroupManager :
        processorMap.values()) { // 遍历每个存储组的虚拟存储组管理类进行计算相应的待升级的TSFile文件数量
      totalUpgradeFileNum += virtualStorageGroupManager.countUpgradeFiles();
    }
    return totalUpgradeFileNum;
  }

  /**
   * upgrade all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void upgradeAll() throws StorageEngineException { // 升级所有待升级的TSFile
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException(
          "Current system mode is read only, does not support file upgrade");
    }
    for (VirtualStorageGroupManager virtualStorageGroupManager :
        processorMap.values()) { // 使用该系统里每个存储组的虚拟存储组管理器进行升级文件
      virtualStorageGroupManager.upgradeAll();
    }
  }

  public void getResourcesToBeSettled(
      PartialPath sgPath,
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths)
      throws StorageEngineException {
    VirtualStorageGroupManager vsg = processorMap.get(sgPath);
    if (vsg == null) {
      throw new StorageEngineException(
          "The Storage Group " + sgPath.toString() + " is not existed.");
    }
    if (!vsg.getIsSettling().compareAndSet(false, true)) {
      throw new StorageEngineException(
          "Storage Group " + sgPath.getFullPath() + " is already being settled now.");
    }
    vsg.getResourcesToBeSettled(seqResourcesToBeSettled, unseqResourcesToBeSettled, tsFilePaths);
  }

  public void setSettling(PartialPath sgPath, boolean isSettling) {
    if (processorMap.get(sgPath) == null) {
      return;
    }
    processorMap.get(sgPath).setSettling(isSettling);
  }

  /**
   * merge all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll(boolean isFullMerge) throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }

    for (VirtualStorageGroupManager virtualStorageGroupManager : processorMap.values()) {
      virtualStorageGroupManager.mergeAll(isFullMerge);
    }
  }

  /**
   * delete all data files (both memory data and file on disk) in a storage group. It is used when
   * there is no timeseries (which are all deleted) in this storage group)
   */
  public void deleteAllDataFilesInOneStorageGroup(PartialPath storageGroupPath) {
    if (processorMap.containsKey(storageGroupPath)) {
      syncDeleteDataFiles(storageGroupPath);
    }
  }

  private void syncDeleteDataFiles(PartialPath storageGroupPath) {
    logger.info("Force to delete the data in storage group processor {}", storageGroupPath);
    processorMap.get(storageGroupPath).syncDeleteDataFiles();
  }

  /** release all the allocated non-heap */
  public void releaseWalDirectByteBufferPoolInOneStorageGroup(PartialPath storageGroupPath) {
    if (processorMap.containsKey(storageGroupPath)) {
      processorMap.get(storageGroupPath).releaseWalDirectByteBufferPool();
    }
  }

  /** delete all data of storage groups' timeseries. */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (PartialPath storageGroup : IoTDB.metaManager.getAllStorageGroupPaths()) {
      this.deleteAllDataFilesInOneStorageGroup(storageGroup);
    }
    return true;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) {
    // storage group has no data
    if (!processorMap.containsKey(storageGroup)) {
      return;
    }

    processorMap.get(storageGroup).setTTL(dataTTL);
  }

  public void deleteStorageGroup(PartialPath storageGroupPath) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    deleteAllDataFilesInOneStorageGroup(storageGroupPath);
    releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroupPath);
    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.remove(storageGroupPath);
    virtualStorageGroupManager.deleteStorageGroup(
        systemDir + File.pathSeparator + storageGroupPath);
  }

  public void loadNewTsFileForSync(TsFileResource newTsFileResource)
      throws StorageEngineException, LoadFileException, IllegalPathException {
    getProcessorDirectly(new PartialPath(getSgByEngineFile(newTsFileResource.getTsFile())))
        .loadNewTsFileForSync(newTsFileResource);
  }

  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws LoadFileException, StorageEngineException, MetadataException {
    Set<String> deviceSet = newTsFileResource.getDevices();
    if (deviceSet == null || deviceSet.isEmpty()) {
      throw new StorageEngineException("Can not get the corresponding storage group.");
    }
    String device = deviceSet.iterator().next();
    PartialPath devicePath = new PartialPath(device);
    PartialPath storageGroupPath = IoTDB.metaManager.getBelongedStorageGroup(devicePath);
    getProcessorDirectly(storageGroupPath).loadNewTsFile(newTsFileResource);
  }

  public boolean deleteTsfileForSync(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(deletedTsfile)))
        .deleteTsfile(deletedTsfile);
  }

  public boolean deleteTsfile(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(deletedTsfile)))
        .deleteTsfile(deletedTsfile);
  }

  public boolean unloadTsfile(File tsfileToBeUnloaded, File targetDir)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(tsfileToBeUnloaded)))
        .unloadTsfile(tsfileToBeUnloaded, targetDir);
  }

  /**
   * The internal file means that the file is in the engine, which is different from those external
   * files which are not loaded.
   *
   * @param file internal file
   * @return sg name
   */
  public String getSgByEngineFile(File file) {
    return file.getParentFile().getParentFile().getParentFile().getName();
  }

  /** @return TsFiles (seq or unseq) grouped by their storage group and partition number. */
  public Map<PartialPath, Map<Long, List<TsFileResource>>> getAllClosedStorageGroupTsFile() {
    Map<PartialPath, Map<Long, List<TsFileResource>>> ret = new HashMap<>();
    for (Entry<PartialPath, VirtualStorageGroupManager> entry : processorMap.entrySet()) {
      entry.getValue().getAllClosedStorageGroupTsFile(entry.getKey(), ret);
    }
    return ret;
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  public boolean isFileAlreadyExist(
      TsFileResource tsFileResource, PartialPath storageGroup, long partitionNum) {
    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.get(storageGroup);
    if (virtualStorageGroupManager == null) {
      return false;
    }

    Iterator<String> partialPathIterator = tsFileResource.getDevices().iterator();
    try {
      return getProcessor(new PartialPath(partialPathIterator.next()))
          .isFileAlreadyExist(tsFileResource, partitionNum);
    } catch (StorageEngineException | IllegalPathException e) {
      logger.error("can't find processor with: " + tsFileResource, e);
    }

    return false;
  }

  /**
   * Set the version of given partition to newMaxVersion if it is larger than the current version.
   */
  public void setPartitionVersionToMax(
      PartialPath storageGroup, long partitionId, long newMaxVersion) {
    processorMap.get(storageGroup).setPartitionVersionToMax(partitionId, newMaxVersion);
  }

  public void removePartitions(PartialPath storageGroupPath, TimePartitionFilter filter) {
    if (processorMap.get(storageGroupPath) != null) {
      processorMap.get(storageGroupPath).removePartitions(filter);
    }
  }

  public Map<PartialPath, VirtualStorageGroupManager> getProcessorMap() {
    return processorMap;
  }

  /**
   * Get a map indicating which storage groups have working TsFileProcessors and its associated
   * partitionId and whether it is sequence or not.
   *
   * @return storage group -> a list of partitionId-isSequence pairs
   */
  public Map<String, List<Pair<Long, Boolean>>> getWorkingStorageGroupPartitions() {
    Map<String, List<Pair<Long, Boolean>>> res = new ConcurrentHashMap<>();
    for (Entry<PartialPath, VirtualStorageGroupManager> entry : processorMap.entrySet()) {
      entry.getValue().getWorkingStorageGroupPartitions(entry.getKey().getFullPath(), res);
    }
    return res;
  }

  /**
   * Add a listener to listen flush start/end events. Notice that this addition only applies to
   * TsFileProcessors created afterwards.
   *
   * @param listener
   */
  public void registerFlushListener(FlushListener listener) {
    customFlushListeners.add(listener);
  }

  /**
   * Add a listener to listen file close events. Notice that this addition only applies to
   * TsFileProcessors created afterwards.
   *
   * @param listener
   */
  public void registerCloseFileListener(CloseFileListener listener) {
    customCloseFileListeners.add(listener);
  }

  /** get all merge lock of the storage group processor related to the query */
  public List<StorageGroupProcessor> mergeLock(List<PartialPath> pathList)
      throws
          StorageEngineException { // 对给定查询相关的时间序列路径列表对应的各自存储组下的所有虚拟存储组加读锁，并返回这些所有虚拟存储组的StorageGroupProcessor
    Set<StorageGroupProcessor> set =
        new HashSet<>(); // 存放pathList里所有时间序列路径对应的所有虚拟存储组的StorageGroupProcessor
    for (PartialPath path : pathList) {
      set.add(getProcessor(path.getDevicePath())); // 根据传过来的设备路径对象来获取对应虚拟存储组的StorageGroupProcessor
    }
    List<StorageGroupProcessor> list =
        set.stream()
            .sorted(Comparator.comparing(StorageGroupProcessor::getVirtualStorageGroupId))
            .collect(Collectors.toList());
    list.forEach(StorageGroupProcessor::readLock);
    return list;
  }

  /** unlock all merge lock of the storage group processor related to the query */
  public void mergeUnLock(List<StorageGroupProcessor> list) {
    list.forEach(StorageGroupProcessor::readUnlock);
  }

  static class InstanceHolder {

    private static final StorageEngine INSTANCE = new StorageEngine();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
