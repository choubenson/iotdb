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

package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An example of writing vector type timeseries with tablet */
public class TsFileWriteVectorWithTablet {

  private static final Logger logger = LoggerFactory.getLogger(TsFileWriteVectorWithTablet.class);

  public static void main(String[] args) throws IOException {
    try {
      String path = "test.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);

      Schema schema = new Schema(); // 目前在使用TSFile
      // API进行插入多元序列时，需要先新建一个Schema配置类对象，后续往该配置类对象里注册多元时间序列,然后用该schema创建TsFileWriter

      String device = Constant.DEVICE_PREFIX + 1;
      String sensorPrefix = "sensor_";
      String vectorName = "vector1";
      // the number of rows to include in the tablet
      int rowNum = 10000; // 该tablet里的行数
      // the number of vector values to include in the tablet
      int multiSensorNum = 10; // 该tablet里包含的传感器数量

      String[] measurementNames = new String[multiSensorNum];
      TSDataType[] dataTypes = new TSDataType[multiSensorNum];

      List<IMeasurementSchema> measurementSchemas =
          new ArrayList<>(); // 存放了该多元序列的多元传感器配置类对象，其实只有一个元素，但是为了创建Tablet需要构造ArrayList
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < multiSensorNum; i++) {
        measurementNames[i] = sensorPrefix + (i + 1);
        dataTypes[i] = TSDataType.INT64;
      }
      // vector schema
      IMeasurementSchema vectorMeasurementSchema = // 创建多元传感器配置类对象
          new VectorMeasurementSchema(vectorName, measurementNames, dataTypes);
      measurementSchemas.add(vectorMeasurementSchema);
      schema.registerTimeseries(
          new Path(device, vectorName),
          vectorMeasurementSchema); // 往TsFileWriter里的schema配置类对象里注册该多元时间序列
      // add measurements into TSFileWriter
      try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {

        // construct the
        // tablet//该构造函数里会遍历传感器配置类对象数组：（1）若是单元传感器，则把对应的传感器放入measurementIndex里（2）若是多元传感器，则把其下的每个子分量依次放入measurementIndex里
        Tablet tablet =
            new Tablet(
                device, measurementSchemas); // 该Tablet里存放了一个设备，该设备只有一个传感器配置类对象，且是多元传感器的配置类对象，通过

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        long timestamp = 1;
        long value = 1000000L;

        for (int r = 0;
            r < rowNum;
            r++, value++) { // 将该多元传感器的每个分量放入该Tablet里充当每列，然后依次对所有传感器插入一行行数据，此处插入数据是同一时间戳上所有传感器的值都相同
          int row = tablet.rowSize++; // 将tablet的行数+1
          timestamps[row] = timestamp++; // 往timestamps数组里的当前行加入时间戳
          for (int i = 0; i < measurementSchemas.size(); i++) { // 遍历多元时间传感器配置类对象数组，应该只有一个元素
            IMeasurementSchema measurementSchema = measurementSchemas.get(i); // 获取多元时间传感器配置类对象
            if (measurementSchema instanceof VectorMeasurementSchema) { // 若是多元时间传感器配置类对象
              for (String valueName :
                  measurementSchema.getSubMeasurementsList()) { // 遍历该多元传感器里的每个子分量的名称
                tablet.addValue(valueName, row, value); // 往该Tablet的指定valueName传感器的第row行放入value数据
              }
            }
          }
          // write Tablet to TsFile
          if (tablet.rowSize
              == tablet
                  .getMaxRowNumber()) { // 当Tablet的行数到达上限时，就使用TsFileWriter把tablet写入TsFile，并重置Tablet
            tsFileWriter.write(tablet);
            tablet.reset();
          }
        }
        // write Tablet to TsFile
        if (tablet.rowSize != 0) { // 使用TsFileWriter把tablet写入TsFile，并重置Tablet
          tsFileWriter.write(tablet);
          tablet.reset();
        }
      }

    } catch (Exception e) {
      logger.error("meet error in TsFileWrite with tablet", e);
    }
  }
}
