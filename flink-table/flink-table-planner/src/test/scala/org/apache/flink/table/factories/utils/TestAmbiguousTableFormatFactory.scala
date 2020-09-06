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

package org.apache.flink.table.factories.utils

import java.util

import org.apache.flink.table.descriptors.FormatDescriptorValidator
import org.apache.flink.table.factories.{TableFormatFactory, TableFormatFactoryServiceTest}
import org.apache.flink.types.Row

/**
  * Table format factory for testing.
  *
  * It does not support UNIQUE_PROPERTY compared to [[TestTableFormatFactory]] nor
  * schema derivation. Both formats have the same context and support COMMON_PATH.
  */
class TestAmbiguousTableFormatFactory extends TableFormatFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(
      FormatDescriptorValidator.FORMAT_TYPE,
      TableFormatFactoryServiceTest.TEST_FORMAT_TYPE)
    context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION, "1")
    context
  }

  override def supportsSchemaDerivation(): Boolean = false // no schema derivation

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add(TableFormatFactoryServiceTest.COMMON_PATH)
    properties.add(TableFormatFactoryServiceTest.SPECIAL_PATH)
    properties
  }
}
