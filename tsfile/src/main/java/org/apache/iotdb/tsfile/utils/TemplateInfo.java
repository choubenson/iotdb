package org.apache.iotdb.tsfile.utils;

import java.util.Map;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TemplateInfo {
  Map<String, MeasurementSchema> measurementSchemaMap;
  boolean isAligned;

  public TemplateInfo(Map<String, MeasurementSchema> measurementSchemaMap, boolean isAligned) {
    this.measurementSchemaMap = measurementSchemaMap;
    this.isAligned = isAligned;
  }

  public Map<String, MeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }

  public void setMeasurementSchemaMap(Map<String, MeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }
}
