package org.apache.iotdb.tsfile.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class DeviceInfo {
  private boolean isAligned = false;
  private Map<String, MeasurementSchema> measurementSchemaMap;

  public DeviceInfo(boolean isAligned) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
  }

  public DeviceInfo(boolean isAligned, Map<String, MeasurementSchema> measurementSchemaMap) {
    this.isAligned = isAligned;
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public Map<String, MeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }

  public void setMeasurementSchemaMap(Map<String, MeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }
}
