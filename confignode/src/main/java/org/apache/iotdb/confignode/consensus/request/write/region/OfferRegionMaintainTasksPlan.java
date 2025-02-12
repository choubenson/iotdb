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
package org.apache.iotdb.confignode.consensus.request.write.region;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Vector;

public class OfferRegionMaintainTasksPlan extends ConfigPhysicalPlan {

  protected List<RegionMaintainTask> regionMaintainTaskList;

  public OfferRegionMaintainTasksPlan() {
    super(ConfigPhysicalPlanType.OfferRegionMaintainTasks);
    this.regionMaintainTaskList = new Vector<>();
  }

  public void appendRegionMaintainTask(RegionMaintainTask task) {
    this.regionMaintainTaskList.add(task);
  }

  public List<RegionMaintainTask> getRegionMaintainTaskList() {
    return regionMaintainTaskList;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.OfferRegionMaintainTasks.ordinal());

    stream.writeInt(regionMaintainTaskList.size());
    for (RegionMaintainTask task : regionMaintainTaskList) {
      task.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      RegionMaintainTask task = RegionMaintainTask.Factory.create(buffer);
      regionMaintainTaskList.add(task);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OfferRegionMaintainTasksPlan that = (OfferRegionMaintainTasksPlan) o;
    return regionMaintainTaskList.equals(that.regionMaintainTaskList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionMaintainTaskList);
  }
}
