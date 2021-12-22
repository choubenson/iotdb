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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileNameGenerator {

  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public static String generateNewTsFilePath(
      String tsFileDir,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount) {
    return tsFileDir
        + File.separator
        + generateNewTsFileName(
            time, version, innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  public static String generateNewTsFilePathWithMkdir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount)
      throws DiskSpaceInsufficientException {
    String tsFileDir =
        generateTsFileDir(sequence, logicalStorageGroup, virtualStorageGroup, timePartitionId);
    fsFactory.getFile(tsFileDir).mkdirs();
    return tsFileDir
        + File.separator
        + generateNewTsFileName(
            time, version, innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  private static String generateTsFileDir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId)
      throws DiskSpaceInsufficientException {
    DirectoryManager directoryManager = DirectoryManager.getInstance();
    String baseDir =
        sequence
            ? directoryManager.getNextFolderForSequenceFile()
            : directoryManager.getNextFolderForUnSequenceFile();
    return baseDir
        + File.separator
        + logicalStorageGroup
        + File.separator
        + virtualStorageGroup
        + File.separator
        + timePartitionId;
  }

  public static String generateNewTsFileName(
      long time, long version, int innerSpaceCompactionCount, int crossSpaceCompactionCount) {
    return time
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + version
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + innerSpaceCompactionCount
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + crossSpaceCompactionCount
        + TsFileConstant.TSFILE_SUFFIX;
  }

  // 根据本地TsFile文件的文件名获取对应的TsFileName对象
  public static TsFileName getTsFileName(String fileName) throws IOException {
    Matcher matcher = TsFileName.FILE_NAME_MATCHER.matcher(fileName);
    if (matcher.find()) {
      try {
        TsFileName tsFileName =
            new TsFileName(
                Long.parseLong(matcher.group(1)),
                Long.parseLong(matcher.group(2)),
                Integer.parseInt(matcher.group(3)),
                Integer.parseInt(matcher.group(4)));
        return tsFileName;
      } catch (NumberFormatException e) {
        throw new IOException("tsfile file name format is incorrect:" + fileName);
      }
    } else {
      throw new IOException("tsfile file name format is incorrect:" + fileName);
    }
  }

  public static TsFileResource increaseCrossCompactionCnt(TsFileResource tsFileResource)
      throws IOException {
    File tsFile = tsFileResource.getTsFile();
    String path = tsFile.getParent();
    // 根据本地TsFile文件的文件名获取对应的TsFileName对象
    TsFileName tsFileName = getTsFileName(tsFileResource.getTsFile().getName());
    tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
    tsFileResource.setFile(
        new File(
            path,
            tsFileName.time
                + FILE_NAME_SEPARATOR
                + tsFileName.version
                + FILE_NAME_SEPARATOR
                + tsFileName.innerCompactionCnt
                + FILE_NAME_SEPARATOR
                + tsFileName.crossCompactionCnt
                + TSFILE_SUFFIX));
    return tsFileResource;
  }

  //根据旧的顺序文件名返回新的最终目标文件，该最终目标文件名里跨空间合并次数比旧文件增加了1
  public static File increaseCrossCompactionCnt(File tsFile) throws IOException {
    String path = tsFile.getParent();
    // 根据本地TsFile文件的文件名获取对应的TsFileName对象
    TsFileName tsFileName = getTsFileName(tsFile.getName());
    //跨空间合并次数+1
    tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
    return new File(
        path,
        tsFileName.time
            + FILE_NAME_SEPARATOR
            + tsFileName.version
            + FILE_NAME_SEPARATOR
            + tsFileName.innerCompactionCnt
            + FILE_NAME_SEPARATOR
            + tsFileName.crossCompactionCnt
            + TSFILE_SUFFIX);
  }

  // 根据待合并的文件列表和是否顺序创建对应合并的新的一个TsFile，命名规则是：（1）若是顺序文件的空间内合并，则新生成的文件是“最小时间戳-最小版本号-空间内合并数+1-跨空间合并数”（2）若是乱序文件的空间内合并，则新生成的文件是“最大时间戳-最大版本号-空间内合并数+1-跨空间合并数”
  public static File getInnerCompactionFileName(
      List<TsFileResource> tsFileResources, boolean sequence) throws IOException {
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long minVersion = Long.MAX_VALUE;
    long maxVersion = Long.MIN_VALUE;
    long maxInnerMergeCount = Long.MIN_VALUE;
    long maxCrossMergeCount = Long.MIN_VALUE;
    for (TsFileResource resource : tsFileResources) {
      // 根据本地TsFile文件的文件名获取对应的TsFileName对象
      TsFileName tsFileName = getTsFileName(resource.getTsFile().getName());
      minTime = Math.min(tsFileName.time, minTime);
      maxTime = Math.max(tsFileName.time, maxTime);
      minVersion = Math.min(tsFileName.version, minVersion);
      maxVersion = Math.max(tsFileName.version, maxVersion);
      maxInnerMergeCount = Math.max(tsFileName.innerCompactionCnt, maxInnerMergeCount);
      maxCrossMergeCount = Math.max(tsFileName.crossCompactionCnt, maxCrossMergeCount);
    }
    return sequence
        ? new File( // 若是顺序文件的空间内合并，则新生成的文件是“最小时间戳-最小版本号-空间内合并数+1-跨空间合并数”
            tsFileResources.get(0).getTsFile().getParent(),
            minTime
                + FILE_NAME_SEPARATOR
                + minVersion
                + FILE_NAME_SEPARATOR
                + (maxInnerMergeCount + 1)
                + FILE_NAME_SEPARATOR
                + maxCrossMergeCount
                + TSFILE_SUFFIX)
        : new File( // 若是乱序文件的空间内合并，则新生成的文件是“最大时间戳-最大版本号-空间内合并数+1-跨空间合并数”
            tsFileResources.get(0).getTsFile().getParent(),
            maxTime
                + FILE_NAME_SEPARATOR
                + maxVersion
                + FILE_NAME_SEPARATOR
                + (maxInnerMergeCount + 1)
                + FILE_NAME_SEPARATOR
                + maxCrossMergeCount
                + TSFILE_SUFFIX);
  }

  public static class TsFileName {
    private static final String FILE_NAME_PATTERN = "(\\d+)-(\\d+)-(\\d+)-(\\d+).tsfile$";
    private static final Pattern FILE_NAME_MATCHER = Pattern.compile(TsFileName.FILE_NAME_PATTERN);

    private long time; // 时间戳
    private long version; // 版本号
    private int innerCompactionCnt; // 空间内合并次数
    private int crossCompactionCnt; // 跨空间合并次数

    public TsFileName(long time, long version, int innerCompactionCnt, int crossCompactionCnt) {
      this.time = time;
      this.version = version;
      this.innerCompactionCnt = innerCompactionCnt;
      this.crossCompactionCnt = crossCompactionCnt;
    }

    public long getTime() {
      return time;
    }

    public long getVersion() {
      return version;
    }

    public int getInnerCompactionCnt() {
      return innerCompactionCnt;
    }

    public int getCrossCompactionCnt() {
      return crossCompactionCnt;
    }

    public void setTime(long time) {
      this.time = time;
    }

    public void setVersion(long version) {
      this.version = version;
    }

    public void setInnerCompactionCnt(int innerCompactionCnt) {
      this.innerCompactionCnt = innerCompactionCnt;
    }

    public void setCrossCompactionCnt(int crossCompactionCnt) {
      this.crossCompactionCnt = crossCompactionCnt;
    }
  }
}
