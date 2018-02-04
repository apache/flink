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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME
import org.apache.flink.table.descriptors.SchemaValidator._

/**
  * Validator for [[Schema]].
  */
class SchemaValidator(isStreamEnvironment: Boolean = true) extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateInt(SCHEMA_VERSION, isOptional = true, 0, Integer.MAX_VALUE)

    val names = properties.getIndexedProperty(SCHEMA, NAME)
    val types = properties.getIndexedProperty(SCHEMA, TYPE)

    if (names.isEmpty && types.isEmpty) {
      throw new ValidationException(s"Could not find the required schema for property '$SCHEMA'.")
    }

    for (i <- 0 until Math.max(names.size, types.size)) {
      properties.validateString(s"$SCHEMA.$i.$NAME", isOptional = false, minLen = 1)
      properties.validateType(s"$SCHEMA.$i.$TYPE", isOptional = false)
      properties.validateString(s"$SCHEMA.$i.$FROM", isOptional = true, minLen = 1)
      // either proctime or rowtime
      val proctime = s"$SCHEMA.$i.$PROCTIME"
      val rowtime = s"$SCHEMA.$i.$ROWTIME"
      if (properties.contains(proctime)) {
        if (!isStreamEnvironment) {
          throw new ValidationException(
            s"Property '$proctime' is not allowed in a batch environment.")
        }
        // check proctime
        properties.validateBoolean(proctime, isOptional = false)
        // no rowtime
        properties.validatePrefixExclusion(rowtime)
      } else if (properties.hasPrefix(rowtime)) {
        // check rowtime
        val rowtimeValidator = new RowtimeValidator(s"$SCHEMA.$i.")
        rowtimeValidator.validate(properties)
        // no proctime
        properties.validateExclusion(proctime)
      }
    }
  }
}

object SchemaValidator {

  val SCHEMA = "schema"
  val SCHEMA_VERSION = "schema.version"

  // per column properties

  val NAME = "name"
  val TYPE = "type"
  val PROCTIME = "proctime"
  val PROCTIME_VALUE_TRUE = "true"
  val FROM = "from"

}
