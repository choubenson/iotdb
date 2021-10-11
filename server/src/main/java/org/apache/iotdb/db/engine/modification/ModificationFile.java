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

package org.apache.iotdb.db.engine.modification;

import org.apache.iotdb.db.engine.modification.io.LocalTextModificationAccessor;
import org.apache.iotdb.db.engine.modification.io.ModificationReader;
import org.apache.iotdb.db.engine.modification.io.ModificationWriter;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * ModificationFile stores the Modifications of a TsFile or unseq file in another file in the same
 * directory. Methods in this class are highly synchronized for concurrency safety.
 */
public class ModificationFile implements AutoCloseable {  //mods文件类，该类对应着一个TSFile在本地的.mods文件，存储该.mods文件的相关信息（如文件路径，修改记录等），可用于向该.mods文件进行写入和读出数据
                                                            //每个TSFile文件都有着自己的一个.mods文件
  private static final Logger logger = LoggerFactory.getLogger(ModificationFile.class);
  public static final String FILE_SUFFIX = ".mods";

  private List<Modification> modifications; //修改操作列表，该列表记录了此本地mods文件里所有修改操作Modification对象的相关信息，
  private ModificationWriter writer;
  private ModificationReader reader;
  private String filePath;      //文件路径
  private Random random = new Random();

  /**
   * Construct a ModificationFile using a file as its storage.
   *
   * @param filePath the path of the storage file.
   */
  public ModificationFile(String filePath) {
    LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(filePath);
    this.writer = accessor;
    this.reader = accessor;
    this.filePath = filePath;
  }

  private void init() {
    synchronized (this) {
      modifications = (List<Modification>) reader.read(); //首先从对应的本地mods里读取所有的修改操作，存入需修改操作列表里
    }
  }

  private void checkInit() {
    if (modifications == null) {
      init();
    }
  }

  /** Release resources such as streams and caches. */
  @Override
  public void close() throws IOException {  //关闭此修改文件类对象，首先会把writer资源释放，并且把此mods文件类对象的修改操作列表清空
    synchronized (this) {
      writer.close();
      modifications = null;
    }
  }

  public void abort() throws IOException {
    synchronized (this) {
      if (!modifications.isEmpty()) {
        writer.abort();
        modifications.remove(modifications.size() - 1);
      }
    }
  }

  /**
   * Write a modification in this file. The modification will first be written to the persistent
   * store then the memory cache.
   *
   * @param mod the modification to be written.
   * @throws IOException if IOException is thrown when writing the modification to the store.
   */
  public void write(Modification mod) throws IOException {  //往该TSFile文件对应的本地.mods文件写入此次修改记录，并往修改操作列表属性里加入此次修改操作记录
    synchronized (this) {
      checkInit();
      writer.write(mod);  //往本地mods文件中写入修改记录
      modifications.add(mod);//往此mods修改文件的修改操作列表加入此次修改操作记录
    }
  }

  /**
   * Get all modifications stored in this file.
   *
   * @return an ArrayList of modifications.
   */
  public Collection<Modification> getModifications() {
    synchronized (this) {
      checkInit();
      return new ArrayList<>(modifications);
    }
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public void remove() throws IOException {
    close();
    FSFactoryProducer.getFSFactory().getFile(filePath).delete();
  }

  public boolean exists() {
    return new File(filePath).exists();
  }

  /**
   * Create a hardlink for the modification file. The hardlink with have a suffix like
   * ".{sysTime}_{randomLong}"
   *
   * @return a new ModificationFile with its path changed to the hardlink, or null if the origin
   *     file does not exist or the hardlink cannot be created.
   */
  public ModificationFile createHardlink() {
    if (!exists()) {
      return null;
    }

    while (true) {
      String hardlinkSuffix =
          TsFileConstant.PATH_SEPARATOR + System.currentTimeMillis() + "_" + random.nextLong();
      File hardlink = new File(filePath + hardlinkSuffix);

      try {
        Files.createLink(Paths.get(hardlink.getAbsolutePath()), Paths.get(filePath));
        return new ModificationFile(hardlink.getAbsolutePath());
      } catch (FileAlreadyExistsException e) {
        // retry a different name if the file is already created
      } catch (IOException e) {
        logger.error("Cannot create hardlink for {}", filePath, e);
        return null;
      }
    }
  }
}
