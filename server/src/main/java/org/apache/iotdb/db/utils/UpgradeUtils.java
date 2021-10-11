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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.upgrade.UpgradeCheckStatus;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.v2.read.TsFileSequenceReaderForV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UpgradeUtils {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeUtils.class);
  private static final String COMMA_SEPERATOR = ",";
  private static final ReadWriteLock cntUpgradeFileLock = new ReentrantReadWriteLock();
  private static final ReadWriteLock upgradeLogLock = new ReentrantReadWriteLock();

  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private static Map<String, Integer> upgradeRecoverMap = new HashMap<>();    //存放了系统升级日志文件upgrade.txt里的相关内容，即（TsFile文件名，对应的升级状态）

  public static ReadWriteLock getCntUpgradeFileLock() {
    return cntUpgradeFileLock;
  }

  public static ReadWriteLock getUpgradeLogLock() {
    return upgradeLogLock;
  }

  /** judge whether a tsfile needs to be upgraded */
  public static boolean isNeedUpgrade(TsFileResource tsFileResource) {
    tsFileResource.readLock();
    // case the TsFile's length is equal to 0, the TsFile does not need to be upgraded
    try {
      if (tsFileResource.getTsFile().length() == 0) {
        return false;
      }
    } finally {
      tsFileResource.readUnlock();
    }
    tsFileResource.readLock();
    try (TsFileSequenceReaderForV2 tsFileSequenceReader =
        new TsFileSequenceReaderForV2(tsFileResource.getTsFile().getAbsolutePath())) {
      String versionNumber = tsFileSequenceReader.readVersionNumberV2();
      if (versionNumber.equals(TSFileConfig.VERSION_NUMBER_V2)
          || versionNumber.equals(TSFileConfig.VERSION_NUMBER_V1)) {
        return true;
      }
    } catch (IOException e) {
      logger.error(
          "meet error when judge whether file needs to be upgraded, the file's path:{}",
          tsFileResource.getTsFile().getAbsolutePath(),
          e);
    } finally {
      tsFileResource.readUnlock();
    }
    return false;
  }

  public static void moveUpgradedFiles(TsFileResource resource) throws IOException {  //参数是旧TsFile的TsFileResource，遍历该旧TsFile文件升级后生成的每个新TsFile的TsFileResouce：（1）创建本地虚拟存储组目录下对应的时间分区目录，并将该新TsFile和对应新.mods文件移动到此时间分区目录下（2）将本地新的.mods文件和新TsFile文件反序列化到该TsFileResource对象的属性里，然后关闭该新TsFileResource，并把它的内容序列化写到本地的.resource文件里
    List<TsFileResource> upgradedResources = resource.getUpgradedResources(); //获取该旧TsFile升级后生成的多个新TsFile对应的TsFileResource
    for (TsFileResource upgradedResource : upgradedResources) {//遍历每个新TsFile对应的TsFileResource
      File upgradedFile = upgradedResource.getTsFile(); //获取新TsFile的文件对象
      long partition = upgradedResource.getTimePartition();//获取该新TsFile对应的时间分区
      String virtualStorageGroupDir = upgradedFile.getParentFile().getParentFile().getParent();//获取该新TsFile所属虚拟存储组的目录路径
      File partitionDir = fsFactory.getFile(virtualStorageGroupDir, String.valueOf(partition));//获取该虚拟存储组下的该新TsFile所属时间分区的目录
      if (!partitionDir.exists()) { //若该虚拟存储组下的该新TsFile所属时间分区的目录不存在，则本地新建该新TsFile所属的时间分区目录
        partitionDir.mkdir();
      }
      // move upgraded TsFile
      if (upgradedFile.exists()) {  //若本地存在该新TsFile文件，则把它移动到对应的时间分区目录下
        fsFactory.moveFile(upgradedFile, fsFactory.getFile(partitionDir, upgradedFile.getName()));
      }
      // get temp resource
      File tempResourceFile =   //获取该新的TsFile文件对应的.resource文件对象
          fsFactory.getFile(upgradedResource.getTsFile().toPath() + TsFileResource.RESOURCE_SUFFIX);
      // move upgraded mods file
      File newModsFile =      //获取该新的TsFile文件对应的.mods文件对象
          fsFactory.getFile(upgradedResource.getTsFile().toPath() + ModificationFile.FILE_SUFFIX);
      if (newModsFile.exists()) { //若本地存在该新的TsFile文件对应的.mods文件，则把它移动到对应的时间分区目录下
        fsFactory.moveFile(newModsFile, fsFactory.getFile(partitionDir, newModsFile.getName()));
      }
      // re-serialize upgraded resource to correct place
      upgradedResource.setFile(fsFactory.getFile(partitionDir, upgradedFile.getName()));//设置该新的TsFileResource对应的新TsFile文件对象，即把TsFile文件解码反序列化到该新TsFileResource的TsFile类对象属性里
      if (fsFactory.getFile(partitionDir, newModsFile.getName()).exists()) {  //如果对应时间分区目录下存在该新TsFile对应的新.mods文件，则
        upgradedResource.getModFile();//将本地新的.mods文件解码反序列化到该新TsFileResource的ModificationFile类对象属性里
      }
      upgradedResource.setClosed(true); //关闭、封口此新TsFileResource
      upgradedResource.serialize(); //将该新TsFile文件对应的新TsFileResource对象里的内容序列化写到本地的.resource文件里
      // delete generated temp resource file
      Files.delete(tempResourceFile.toPath());//删除原本新TsFile对应的.resource文件
    }
  }

  public static boolean isUpgradedFileGenerated(String oldFileName) {   //一般传递的是TsFile文件名，用于判断该TsFile的新的升级文件(.tsfile)是否已经生成并封口了
    return upgradeRecoverMap.containsKey(oldFileName) //若upgradeRecoverMap这个数据结构（TsFile文件名，对应的升级状态）包含该TsFile，并且该TsFile对应的升级状态是2（即该TsFile对应的新的升级文件.tsfile已经生成并封口了），则返回真
        && upgradeRecoverMap.get(oldFileName)
            == UpgradeCheckStatus.AFTER_UPGRADE_FILE.getCheckStatusCode();
  }

  public static void clearUpgradeRecoverMap() {
    upgradeRecoverMap = null;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void recoverUpgrade() { //读取系统原先的升级日志文件upgrade.txt，把其内容读进upgradeRecoverMap这个数据结构（TsFile文件名，对应的升级状态）中，并更新对应TsFile文件的升级状态
    if (FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath()).exists()) {//若升级日志文件upgrade.txt存在，"data/system/upgrade/upgrade.txt"
      try (BufferedReader upgradeLogReader =    //读取升级日志文件upgrade.txt内容
          new BufferedReader(
              new FileReader(
                  FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath())))) {
        String line = null;
        while ((line = upgradeLogReader.readLine()) != null) {  //一行一行读取升级日志文件内容
          String oldFilePath = line.split(COMMA_SEPERATOR)[0];  //获取日志文件该行的TSFile路径
          String oldFileName = new File(oldFilePath).getName();   //获取日志文件该行的TSFile文件名
          if (upgradeRecoverMap.containsKey(oldFileName)) { //如果upgradeRecoverMap已经包含该TsFile文件，则把其升级状态+1
            upgradeRecoverMap.put(oldFileName, upgradeRecoverMap.get(oldFileName) + 1);
          } else {  //若不存在，则把该TsFile文件放入upgradeRecoverMap，并设置升级状态为1
            upgradeRecoverMap.put(oldFileName, 1);
          }
        }
      } catch (IOException e) {
        logger.error(
            "meet error when recover upgrade process, file path:{}",
            UpgradeLog.getUpgradeLogPath(),
            e);
      } finally {
        FSFactoryProducer.getFSFactory().getFile(UpgradeLog.getUpgradeLogPath()).delete();  //把系统原先的upgrade.txt升级日志文件删除
      }
    }
  }
}
