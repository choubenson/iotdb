package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.ISettleSelector;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl.DirtyStatus.PARTIAL_DELETED;

public class SettleSelectorImpl implements ISettleSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  boolean heavySelect;

  protected String storageGroupName;
  protected String dataRegionId;
  protected long timePartition;
  protected TsFileManager tsFileManager;

  public SettleSelectorImpl(
      boolean heavySelect,
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager) {
    this.heavySelect = heavySelect;
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
  }

  static class AllDirtyResource {
    List<TsFileResource> resources = new ArrayList<>();

    public void add(TsFileResource resource) {
      resources.add(resource);
    }

    public List<TsFileResource> getResources() {
      return resources;
    }
  }

  static class PartialDirtyResource {
    List<List<TsFileResource>> resourceGroupList = new ArrayList<>();

    List<TsFileResource> resources = new ArrayList<>();
    long resourcesFileSize = 0;

    long totalFileSize = 0;
    long totalFileNum = 0;

    public void add(TsFileResource resource, long dirtyDataSize) {
      resources.add(resource);
      resourcesFileSize += resource.getTsFileSize();
      resourcesFileSize -= dirtyDataSize;
      totalFileSize += resource.getTsFileSize();
      totalFileSize -= dirtyDataSize;
      totalFileNum++;
      if (resources.size() >= config.getFileLimitPerInnerTask()
          || resourcesFileSize >= config.getTargetCompactionFileSize()) {
        submitTmpResources();
      }
    }

    public void submitTmpResources() {
      if (resources.isEmpty()) {
        return;
      }
      resourceGroupList.add(resources);
      resources = new ArrayList<>();
      resourcesFileSize = 0;
    }

    public List<List<TsFileResource>> getResourceGroupList() {
      return resourceGroupList;
    }

    public boolean checkHasReachedThreshold() {
      return totalFileSize >= config.getFileLimitPerInnerTask()
          || totalFileSize >= config.getTargetCompactionFileSize();
    }
  }

  @Override
  public List<SettleCompactionTask> selectSettleTask(List<TsFileResource> tsFileResources) {
    return selectTasks(tsFileResources);
  }

  private List<SettleCompactionTask> selectTasks(List<TsFileResource> resources) {
    AllDirtyResource allDirtyResource = new AllDirtyResource();
    PartialDirtyResource partialDirtyResource = new PartialDirtyResource();
    try {
      for (TsFileResource resource : resources) {
        if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
          continue;
        }

        DirtyStatus dirtyStatus;
        if (!heavySelect) {
          dirtyStatus = selectFileBaseOnModSize(resource);
        } else {
          dirtyStatus = selectFileBaseOnDirtyData(resource);
          if (dirtyStatus == DirtyStatus.NOT_SATISFIED) {
            dirtyStatus = selectFileBaseOnModSize(resource);
          }
        }

        switch (dirtyStatus) {
          case ALL_DELETED:
            allDirtyResource.add(resource);
            break;
          case PARTIAL_DELETED:
            partialDirtyResource.add(resource, dirtyStatus.getDirtyDataSize());
            break;
          case NOT_SATISFIED:
            partialDirtyResource.submitTmpResources();
            break;
          default:
            // do nothing
        }

        // Non-heavy selection are triggered more frequently. In order to avoid selecting too many
        // files containing mods for compaction when the disk is insufficient, the number and size
        // of files are limited here.
        if (!heavySelect && !partialDirtyResource.getResourceGroupList().isEmpty()) {
          break;
        }
      }
      partialDirtyResource.submitTmpResources();
      return createTask(allDirtyResource, partialDirtyResource);
    } catch (Exception e) {
      LOGGER.error(
          "{}-{} cannot select file for settle compaction", storageGroupName, dataRegionId, e);
    }
    return Collections.emptyList();
  }

  private DirtyStatus selectFileBaseOnModSize(TsFileResource resource) {
    ModificationFile modFile = resource.getModFile();
    if (modFile == null || !modFile.exists()) {
      return DirtyStatus.NOT_SATISFIED;
    }
    return modFile.getSize() > config.getInnerCompactionTaskSelectionModsFileThreshold()
            || (!heavySelect
                && !CompactionUtils.isDiskHasSpace(
                    config.getInnerCompactionTaskSelectionDiskRedundancy()))
        ? PARTIAL_DELETED
        : DirtyStatus.NOT_SATISFIED;
  }

  /**
   * Only when all devices with ttl are deleted may they be selected. On the basic of the previous,
   * only when the number of deleted devices exceeds the threshold or has expired for too long will
   * they be selected.
   *
   * @return dirty status means the status of current resource.
   */
  private DirtyStatus selectFileBaseOnDirtyData(TsFileResource resource)
      throws IOException, IllegalPathException {
    ModificationFile modFile = resource.getModFile();
    DeviceTimeIndex deviceTimeIndex =
        resource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE
            ? resource.buildDeviceTimeIndex()
            : (DeviceTimeIndex) resource.getTimeIndex();
    Set<String> deletedDevices = new HashSet<>();
    boolean hasExpiredTooLong = false;
    long currentTime = CommonDateTimeUtils.currentTime();

    Collection<Modification> modifications = modFile.getModifications();
    for (String device : deviceTimeIndex.getDevices()) {
      // check expired device by ttl
      long deviceTTL = DataNodeTTLCache.getInstance().getTTL(device);
      boolean hasSetTTL = deviceTTL != Long.MAX_VALUE;
      boolean isDeleted = !deviceTimeIndex.isDeviceAlive(device, deviceTTL);
      if (!isDeleted) {
        // check deleted device by mods
        isDeleted =
            isDeviceDeletedByMods(
                modifications,
                device,
                deviceTimeIndex.getStartTime(device),
                deviceTimeIndex.getEndTime(device));
      }
      if (hasSetTTL) {
        if (!isDeleted) {
          return DirtyStatus.NOT_SATISFIED;
        }
        long outdatedTimeDiff = currentTime - deviceTimeIndex.getEndTime(device);
        hasExpiredTooLong =
            hasExpiredTooLong
                || outdatedTimeDiff > Math.min(config.getLongestExpiredTime(), 3 * deviceTTL);
      }

      if (isDeleted) {
        deletedDevices.add(device);
      }
    }

    float deletedDeviceRate = (float) (deletedDevices.size()) / deviceTimeIndex.getDevices().size();
    if (deletedDeviceRate == 1f) {
      // the whole file is completely dirty
      return DirtyStatus.ALL_DELETED;
    }
    hasExpiredTooLong = config.getLongestExpiredTime() != Long.MAX_VALUE && hasExpiredTooLong;
    if (hasExpiredTooLong || deletedDeviceRate >= config.getExpiredDataRate()) {
      // evaluate dirty data size in the tsfile
      DirtyStatus partialDeleted = DirtyStatus.PARTIAL_DELETED;
      partialDeleted.setDirtyDataSize((long) (deletedDeviceRate * resource.getTsFileSize()));
      return partialDeleted;
    }
    return DirtyStatus.NOT_SATISFIED;
  }

  /** Check whether the device is completely deleted by mods or not. */
  private boolean isDeviceDeletedByMods(
      Collection<Modification> modifications, String device, long startTime, long endTime)
      throws IllegalPathException {
    for (Modification modification : modifications) {
      PartialPath path = modification.getPath();
      if (path.endWithMultiLevelWildcard()
          && path.getDevicePath().matchFullPath(new PartialPath(device))
          && ((Deletion) modification).getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  private List<SettleCompactionTask> createTask(
      AllDirtyResource allDirtyResource, PartialDirtyResource partialDirtyResource) {
    return Collections.singletonList(
        new SettleCompactionTask(
            timePartition,
            tsFileManager,
            allDirtyResource.getResources(),
            partialDirtyResource.getResourceGroupList(),
            new FastCompactionPerformer(false),
            tsFileManager.getNextCompactionTaskId()));
  }

  enum DirtyStatus {
    ALL_DELETED, // the whole file is deleted
    PARTIAL_DELETED, // the file is partial deleted
    NOT_SATISFIED; // do not satisfy settle condition, which does not mean there is no dirty data

    private long dirtyDataSize = 0;

    public void setDirtyDataSize(long dirtyDataSize) {
      this.dirtyDataSize = dirtyDataSize;
    }

    public long getDirtyDataSize() {
      return dirtyDataSize;
    }
  }
}
