package org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl;

import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ISetDeviceTTLPlan;

public class SetDeviceTTLPlanImpl implements ISetDeviceTTLPlan {
  private String[] devicePathPattern;

  private long ttl;

  @Override
  public String[] getDevicePathPattern() {
    return devicePathPattern;
  }

  @Override
  public void setDevicePathPattern(String[] devicePathPattern) {
    this.devicePathPattern = devicePathPattern;
  }

  @Override
  public long getTTL() {
    return ttl;
  }

  @Override
  public void setTTL(long ttl) {
    this.ttl = ttl;
  }
}
