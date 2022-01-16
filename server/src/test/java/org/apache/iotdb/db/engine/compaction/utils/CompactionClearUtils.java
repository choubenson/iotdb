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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.recover.RewriteCrossSpaceCompactionLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import java.io.File;
import java.io.IOException;

public class CompactionClearUtils {

  /** Clear all generated and merged files in the test directory */
  public static void clearAllCompactionFiles() throws IOException {
    deleteAllFilesInOneDirBySuffix("target", ".tsfile");
    deleteAllFilesInOneDirBySuffix("target", ".resource");
    deleteAllFilesInOneDirBySuffix("target", ".mods");
    deleteAllFilesInOneDirBySuffix("target", ".target");
    deleteAllFilesInOneDirBySuffix("target", ".merge");
    deleteAllFilesInOneDirBySuffix("target", SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
    deleteAllFilesInOneDirBySuffix("target", RewriteCrossSpaceCompactionLogger.COMPACTION_LOG_NAME);
    // clean cache
    if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      BloomFilterCache.getInstance().clear();
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }

  private static void deleteAllFilesInOneDirBySuffix(String dirPath, String suffix)
      throws IOException {
    File dir = new File(dirPath);
    if (!dir.isDirectory()) {
      return;
    }
    if (!dir.exists()) {
      return;
    }
    for (File f : FSFactoryProducer.getFSFactory().listFilesBySuffix(dirPath, suffix)) {
      FileUtils.delete(f);
    }
    File[] tmpFiles = dir.listFiles();
    if (tmpFiles != null) {
      for (File f : tmpFiles) {
        if (f.isDirectory()) {
          deleteAllFilesInOneDirBySuffix(f.getAbsolutePath(), suffix);
        }
      }
    }
  }
}
