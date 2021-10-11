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
package org.apache.iotdb.db.engine.upgrade;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.tools.upgrade.TsFileOnlineUpgradeTool;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class UpgradeTask extends WrappedRunnable {    //升级线程类，每个升级线程类用于处理一个待升级的TsFile文件，并对他进行相关升级操作

  private TsFileResource upgradeResource; //待升级旧的TsFile文件的TsFileResource
  private static final Logger logger = LoggerFactory.getLogger(UpgradeTask.class);
  private static final String COMMA_SEPERATOR = ",";

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public UpgradeTask(TsFileResource upgradeResource) {
    this.upgradeResource = upgradeResource;
  }

  @Override
  public void runMayThrow() {   //升级线程运行的内容
    try {
      String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath(); //获取待升级TsFile文件的路径
      List<TsFileResource> upgradedResources; //存储升级后的新TsFile对应的多个TsFileResource文件对象
      if (!UpgradeUtils.isUpgradedFileGenerated(upgradeResource.getTsFile().getName())) {//如果该TsFile的新的升级文件(.tsfile)没有生成、封口，则
        logger.info("generate upgraded file for {}", upgradeResource.getTsFile());
        upgradedResources = generateUpgradedFiles();//对旧的TsFile文件进行升级并生成新的TsFile文件和对应新的.resource文件和.mods修改文件，并返回升级完成后生成的一个or多个新TsFile的TsFileResource
      } else {//若该TsFile的新的升级文件(.tsfile)已经生成并封口了
        logger.info("find upgraded file for {}", upgradeResource.getTsFile());
        upgradedResources = findUpgradedFiles();//该方法返回升级后的新TsFile对应的多个TsFileResource文件对象（因为数据的时间戳所在时间分区不同，导致升级后存在多个TsFile）
      }
      upgradeResource.setUpgradedResources(upgradedResources);//设置该旧TsFileResource对应旧TsFile对应的升级完后的多个TsFile对应TsFileRewource。//对于该旧TsFileResource对应的TsFile文件升级后（从v0.11.x/v2 升级到0.12/v3）产生的新的多个TsFileResource存入该列表里。
      upgradeResource.getUpgradeTsFileResourceCallBack().call(upgradeResource);
      UpgradeSevice.getTotalUpgradeFileNum().getAndAdd(-1);
      logger.info(
          "Upgrade completes, file path:{} , the remaining upgraded file num: {}",
          oldTsfilePath,
          UpgradeSevice.getTotalUpgradeFileNum().get());
      if (UpgradeSevice.getTotalUpgradeFileNum().get() == 0) {
        logger.info("Start delete empty tmp folders");
        clearTmpFolders(DirectoryManager.getInstance().getAllSequenceFileFolders());
        clearTmpFolders(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
        UpgradeSevice.getINSTANCE().stop();
        logger.info("All files upgraded successfully! ");
      }
    } catch (Exception e) {
      logger.error(
          "meet error when upgrade file:{}", upgradeResource.getTsFile().getAbsolutePath(), e);
    }
  }

  private List<TsFileResource> generateUpgradedFiles() throws IOException, WriteProcessException {  //对旧的TsFile文件进行升级并生成新的一个或多个时间分区的TsFile文件和对应新的.resource文件和.mods修改文件，并返回升级完成后生成的一个or多个新TsFile的TsFileResource
    upgradeResource.readLock();
    String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath();//获取该升级线程对应处理的TsFile文件的路径
    List<TsFileResource> upgradedResources = new ArrayList<>();
    UpgradeLog.writeUpgradeLogFile(   //向系统的升级日志文件upgrade.txt里写入该待升级TsFile的路径和升级状态1，说明准备开始升级文件
        oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.BEGIN_UPGRADE_FILE);
    try {
      TsFileOnlineUpgradeTool.upgradeOneTsFile(upgradeResource, upgradedResources);//该方法用于升级一个TsFile文件，第一个参数是旧的待升级文件的TsFileResource对象，第二个参数是升级完毕新的TsFile文件TsFileResource列表。由于升级时可能会对旧的TsFile根据时间分区分成多个TsFile，因此第二个参数是列表
      UpgradeLog.writeUpgradeLogFile(//向系统的升级日志文件upgrade.txt里写入该待升级TsFile的路径和升级状态2，说明升级完文件了
          oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.AFTER_UPGRADE_FILE);
    } finally {
      upgradeResource.readUnlock();
    }
    return upgradedResources;
  }

  private List<TsFileResource> findUpgradedFiles() throws IOException {//该方法返回升级后的新TsFile对应的多个TsFileResource文件对象（因为数据的时间戳所在时间分区不同，导致升级后存在多个TsFile）
    upgradeResource.readLock();
    List<TsFileResource> upgradedResources = new ArrayList<>();
    String oldTsfilePath = upgradeResource.getTsFile().getAbsolutePath(); //旧TsFile路径
    UpgradeLog.writeUpgradeLogFile(
        oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.BEGIN_UPGRADE_FILE);
    try {
      File upgradeFolder = upgradeResource.getTsFile().getParentFile(); //获取旧TsFile所在目录
      for (File tempPartitionDir : upgradeFolder.listFiles()) {
        if (tempPartitionDir.isDirectory()
            && fsFactory
                .getFile(
                    tempPartitionDir,
                    upgradeResource.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
                .exists()) {
          TsFileResource resource =
              new TsFileResource(
                  fsFactory.getFile(tempPartitionDir, upgradeResource.getTsFile().getName()));
          resource.deserialize();
          upgradedResources.add(resource);
        }
      }
      UpgradeLog.writeUpgradeLogFile(
          oldTsfilePath + COMMA_SEPERATOR + UpgradeCheckStatus.AFTER_UPGRADE_FILE);
    } finally {
      upgradeResource.readUnlock();
    }
    return upgradedResources;
  }

  private void clearTmpFolders(List<String> folders) {
    for (String baseDir : folders) {
      File fileFolder = fsFactory.getFile(baseDir);
      if (!fileFolder.isDirectory()) {
        continue;
      }
      for (File storageGroup : fileFolder.listFiles()) {
        if (!storageGroup.isDirectory()) {
          continue;
        }
        File virtualStorageGroupDir = fsFactory.getFile(storageGroup, "0");
        File upgradeDir = fsFactory.getFile(virtualStorageGroupDir, "upgrade");
        if (upgradeDir == null) {
          continue;
        }
        File[] tmpPartitionDirList = upgradeDir.listFiles();
        if (tmpPartitionDirList == null) {
          continue;
        }
        for (File tmpPartitionDir : tmpPartitionDirList) {
          if (tmpPartitionDir.isDirectory()) {
            try {
              Files.delete(tmpPartitionDir.toPath());
            } catch (IOException e) {
              logger.error("Delete tmpPartitionDir {} failed", tmpPartitionDir);
            }
          }
        }
        // delete upgrade folder when it is empty
        if (upgradeDir.isDirectory()) {
          try {
            Files.delete(upgradeDir.toPath());
          } catch (IOException e) {
            logger.error("Delete tmpUpgradeDir {} failed", upgradeDir);
          }
        }
      }
    }
  }
}
