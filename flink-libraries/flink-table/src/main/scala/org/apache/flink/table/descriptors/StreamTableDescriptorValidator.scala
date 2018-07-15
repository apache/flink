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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.table.descriptors.StreamTableDescriptorValidator._

/**
  * Validator for [[StreamTableDescriptor]].
  */
class StreamTableDescriptorValidator extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateEnumValues(
      UPDATE_MODE,
      isOptional = false,
      util.Arrays.asList(
        UPDATE_MODE_VALUE_APPEND,
        UPDATE_MODE_VALUE_RETRACT,
        UPDATE_MODE_VALUE_UPSERT)
    )
  }
}

object StreamTableDescriptorValidator {

  val UPDATE_MODE = "update-mode"
  val UPDATE_MODE_VALUE_APPEND = "append"
  val UPDATE_MODE_VALUE_RETRACT = "retract"
  val UPDATE_MODE_VALUE_UPSERT = "upsert"
}
