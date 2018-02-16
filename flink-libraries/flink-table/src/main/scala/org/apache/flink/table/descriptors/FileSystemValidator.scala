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

import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.FileSystemValidator.{CONNECTOR_PATH, CONNECTOR_TYPE_VALUE, PROCESSING_MODE, UPDATE_INTERVAL}

/**
  * Validator for [[FileSystem]].
  */
class FileSystemValidator extends ConnectorDescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, isOptional = false)
    properties.validateString(CONNECTOR_PATH, isOptional = false, minLen = 1)
    properties.validateLong(UPDATE_INTERVAL, isOptional = true, min = 500)

    val noValidation = () => {}

    properties.validateEnum(
      PROCESSING_MODE,
      isOptional = true,
      Map(
        FileProcessingMode.PROCESS_CONTINUOUSLY.toString -> noValidation,
        FileProcessingMode.PROCESS_ONCE.toString -> noValidation
      )
    )
  }
}

object FileSystemValidator {

  val CONNECTOR_TYPE_VALUE = "filesystem"
  val CONNECTOR_PATH = "connector.path"
  val PROCESSING_MODE = "connector.processing-mode"
  val UPDATE_INTERVAL = "connector.update-interval-ms"
}
