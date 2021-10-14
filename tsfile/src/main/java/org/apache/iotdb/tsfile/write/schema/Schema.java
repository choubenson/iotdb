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
package org.apache.iotdb.tsfile.write.schema;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The schema of timeseries that exist in this file. The schemaTemplates is a simplified manner to
 * batch create schema of timeseries.
 */
public class Schema
    implements Serializable { // 每个TsFile都有一个Schema配置类，该配置类里存放了该TsFile里所有的timeseries时间序列的相关信息

  /**
   * Path (device + measurement) -> measurementSchema By default, use the LinkedHashMap to store the
   * order of insertion
   */
  private Map<Path, IMeasurementSchema> registeredTimeseries; // 存放了该TsFile里已注册的时间序列，为（全路径，传感器配置类对象）

  /** template name -> (measurement -> MeasurementSchema) */
  private Map<String, Map<String, IMeasurementSchema>>
      schemaTemplates; // 配置模板，即每个模板下有自己的多个传感器ID和对应的传感器配置类对象

  public Schema() {
    this.registeredTimeseries = new LinkedHashMap<>();
  }

  public Schema(Map<Path, IMeasurementSchema> knownSchema) {
    this.registeredTimeseries = knownSchema;
  }

  public void registerTimeseries(Path path, IMeasurementSchema descriptor) {
    this.registeredTimeseries.put(path, descriptor); // 往该TsFile里注册一个新的时间序列，存放该序列的全路径和传感器配置类对象
  }

  public void registerSchemaTemplate( // 根据指定的模板名和其多个传感器ID对应各自的传感器配置类对象去注册配置模板
      String templateName, Map<String, IMeasurementSchema> template) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    this.schemaTemplates.put(templateName, template);
  }

  public void extendTemplate(
      String templateName, IMeasurementSchema descriptor) { // 扩展配置模板，即把指定的传感器配置类对象添加入指定的模板里
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    Map<String, IMeasurementSchema> template =
        this.schemaTemplates.getOrDefault(
            templateName, new HashMap<>()); // 根据此模板名获取对应的传感器及其对应配置类对象的map，若没有则新建一个
    template.put(descriptor.getMeasurementId(), descriptor); // 往此模板里添加该新的传感器配置类对象
    this.schemaTemplates.put(templateName, template); // 更新schemaTemplates变量里此模板名的对应模板
  }

  public void registerDevice(String deviceId, String templateName) {
    if (!schemaTemplates.containsKey(templateName)) { // 若注册模板里不存在此模板，则直接返回
      return;
    }
    Map<String, IMeasurementSchema> template =
        schemaTemplates.get(templateName); // 获取该模板下每个传感器对应的传感器配置对象
    for (Map.Entry<String, IMeasurementSchema> entry :
        template.entrySet()) { // 遍历该模板的每个传感器和其对应的配置对象
      Path path = new Path(deviceId, entry.getKey()); // 将设备ID和传感器ID拼接成时间序列的完整路径
      registerTimeseries(path, entry.getValue()); // 往该TsFile里注册一个新的时间序列，存放该序列的全路径和传感器配置类对象
    }
  }

  public IMeasurementSchema getSeriesSchema(Path path) { // 获取该时间序列的传感器配置类对象
    return registeredTimeseries.get(path);
  }

  public TSDataType getTimeseriesDataType(Path path) {
    if (!registeredTimeseries.containsKey(path)) {
      return null;
    }
    return registeredTimeseries.get(path).getType();
  }

  public Map<String, Map<String, IMeasurementSchema>> getSchemaTemplates() {
    return schemaTemplates;
  }

  /** check if this schema contains a measurement named measurementId. */
  public boolean containsTimeseries(Path path) { // 判断当前TsFile文件里是否已经存在、注册过该时间序列路径
    return registeredTimeseries.containsKey(path);
  }

  // for test
  public Map<Path, IMeasurementSchema> getRegisteredTimeseriesMap() {
    return registeredTimeseries;
  }
}
