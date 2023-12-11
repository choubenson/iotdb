package org.apache.iotdb.db.schemaengine.schemaregion.write.req;

import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

public interface ISetDeviceTTLPlan extends ISchemaRegionPlan {
  @Override
  default SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.SET_DEVICE_TTL;
  }

  @Override
  default <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitSetDeviceTTL(this, context);
  }

  String[] getDevicePathPattern();

  void setDevicePathPattern(String[] devicePathPattern);

  long getTTL();

  void setTTL(long ttl);
}
