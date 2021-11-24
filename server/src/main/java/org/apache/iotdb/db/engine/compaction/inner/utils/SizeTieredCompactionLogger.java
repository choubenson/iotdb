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

package org.apache.iotdb.db.engine.compaction.inner.utils;

import org.apache.iotdb.db.engine.compaction.TsFileIdentifier;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SizeTieredCompactionLogger { // SizeTieredCompaction合并对应的日志类，该日志类可以向该合并任务对应的日志文件里写入日志

  public static final String COMPACTION_LOG_NAME = ".compaction.log";
  public static final String SOURCE_NAME = "source";
  public static final String TARGET_NAME = "target";
  public static final String SOURCE_INFO = "source_info";
  public static final String TARGET_INFO = "target_info";
  public static final String SEQUENCE_NAME = "sequence";
  public static final String UNSEQUENCE_NAME = "unsequence";

  private BufferedWriter logStream;

  public SizeTieredCompactionLogger(String storageGroupDir, String storageGroupName)
      throws IOException {
    logStream =
        new BufferedWriter(
            new FileWriter(
                SystemFileFactory.INSTANCE.getFile(
                    storageGroupDir, storageGroupName + COMPACTION_LOG_NAME),
                true));
  }

  public SizeTieredCompactionLogger(String logFile) throws IOException {
    logStream =
        new BufferedWriter(new FileWriter(SystemFileFactory.INSTANCE.getFile(logFile), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  // 往合并日志里先写入前缀prifix，再写入该TsFile的重要属性（物理存储组名、虚拟存储组名、时间分区、是否顺序、文件名）
  public void logFileInfo(String prefix, File file) throws IOException {
    logStream.write(prefix);
    logStream.newLine();
    logStream.write( // 写入该TsFile的文件识别器的重要属性
        TsFileIdentifier.getFileIdentifierFromFilePath(file.getAbsolutePath()).toString());
    logStream.newLine();
    logStream.flush();
  }

  public void logFile(String prefix, File file) throws IOException {
    logStream.write(prefix);
    logStream.newLine();
    logStream.write(file.getPath());
    logStream.newLine();
    logStream.flush();
  }

  public void logSequence(boolean isSeq) throws IOException {
    if (isSeq) {
      logStream.write(SEQUENCE_NAME);
    } else {
      logStream.write(UNSEQUENCE_NAME);
    }
    logStream.newLine();
    logStream.flush();
  }
}
