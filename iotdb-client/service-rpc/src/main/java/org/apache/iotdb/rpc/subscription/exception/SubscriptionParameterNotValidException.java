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

package org.apache.iotdb.rpc.subscription.exception;

import java.util.Objects;

public class SubscriptionParameterNotValidException extends SubscriptionException {

  public SubscriptionParameterNotValidException(String message) {
    super(message);
  }

  protected SubscriptionParameterNotValidException(String message, long timeStamp) {
    super(message, timeStamp);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SubscriptionParameterNotValidException
        && Objects.equals(getMessage(), ((SubscriptionParameterNotValidException) obj).getMessage())
        && Objects.equals(
            getTimeStamp(), ((SubscriptionParameterNotValidException) obj).getTimeStamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getTimeStamp());
  }
}
