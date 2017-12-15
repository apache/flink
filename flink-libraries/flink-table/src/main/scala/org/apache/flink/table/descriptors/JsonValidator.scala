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

import org.apache.flink.table.descriptors.JsonValidator.{FORMAT_FAIL_ON_MISSING_FIELD, FORMAT_SCHEMA_STRING}

/**
  * Validator for [[Json]].
  */
class JsonValidator extends FormatDescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    properties.validateString(FORMAT_SCHEMA_STRING, isOptional = false, minLen = 1)
    properties.validateBoolean(FORMAT_FAIL_ON_MISSING_FIELD, isOptional = true)
  }
}

object JsonValidator {

  val FORMAT_TYPE_VALUE = "json"
  val FORMAT_SCHEMA_STRING = "format.schema-string"
  val FORMAT_FAIL_ON_MISSING_FIELD = "format.fail-on-missing-field"

}
