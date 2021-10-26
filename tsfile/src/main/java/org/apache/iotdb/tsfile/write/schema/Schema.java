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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.DeviceInfo;
import org.apache.iotdb.tsfile.utils.TemplateInfo;

/**
 * The schema of timeseries that exist in this file. The schemaTemplates is a simplified manner to
 * batch create schema of timeseries.
 */
public class Schema implements Serializable {

  /**
   * Path (devicePath + DeviceInfo) -> measurementSchema By default, use the LinkedHashMap to store
   * the order of insertion
   */
  private Map<Path, DeviceInfo> registeredTimeseries;

  /** template name -> (measurement -> MeasurementSchema) */
  private Map<String, TemplateInfo> schemaTemplates;

  public Schema() {
    this.registeredTimeseries = new LinkedHashMap<>();
  }

  public Schema(Map<Path, DeviceInfo> knownSchema) {
    this.registeredTimeseries = knownSchema;
  }

  public void registerTimeseries(Path devicePath, DeviceInfo deviceInfo)
      throws WriteProcessException {
    if (this.registeredTimeseries.containsKey(devicePath)) {
      if (deviceInfo.isAligned()) {
        throw new WriteProcessException(
            "given aligned device has existed and should not be expanded! " + devicePath);
      } else {
        for (String measurementId : deviceInfo.getMeasurementSchemaMap().keySet()) {
          if (this.registeredTimeseries
              .get(devicePath)
              .getMeasurementSchemaMap()
              .containsKey(measurementId)) {
            throw new WriteProcessException(
                "given nonAligned timeseries has existed! " + (devicePath + measurementId));
          }
        }
      }
    }
    this.registeredTimeseries.put(devicePath, deviceInfo);
  }

  public void registerSchemaTemplate(String templateName, TemplateInfo template) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    this.schemaTemplates.put(templateName, template);
  }

  public void extendTemplate(String templateName, MeasurementSchema descriptor) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    Map<String, MeasurementSchema> template =
        this.schemaTemplates.getOrDefault(templateName, new HashMap<>());
    template.put(descriptor.getMeasurementId(), descriptor);
    this.schemaTemplates.put(templateName, template);
  }

  public void registerDevice(String deviceId, String templateName) {
    Map<String, MeasurementSchema> template =
        schemaTemplates.get(templateName).getMeasurementSchemaMap();
    boolean isAligned = schemaTemplates.get(templateName).isAligned();
    try {
      registerTimeseries(new Path(deviceId), new DeviceInfo(isAligned, template));
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  public DeviceInfo getSeriesSchema(Path devicePath) {
    return registeredTimeseries.get(devicePath);
  }

  public TSDataType getTimeseriesDataType(Path path) {
    if (!registeredTimeseries.containsKey(path)) {
      return null;
    }
    return registeredTimeseries.get(path).getType();
  }

  public Map<String, TemplateInfo> getSchemaTemplates() {
    return schemaTemplates;
  }

  /** check if this schema contains a measurement named measurementId. */
  public boolean containsTimeseries(Path devicePath) {
    return registeredTimeseries.containsKey(devicePath);
  }

  // for test
  public Map<Path, DeviceInfo> getRegisteredTimeseriesMap() {
    return registeredTimeseries;
  }
}
