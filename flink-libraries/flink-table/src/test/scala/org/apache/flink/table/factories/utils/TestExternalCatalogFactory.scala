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
import java.util.Collections

import org.apache.flink.table.catalog.ExternalCatalog
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.{CATALOG_PROPERTY_VERSION, CATALOG_TYPE}
import org.apache.flink.table.factories.utils.TestExternalCatalogFactory._
import org.apache.flink.table.factories.ExternalCatalogFactory
import org.apache.flink.table.runtime.utils.CommonTestData

/**
  * External catalog factory for testing.
  *
  * This factory provides the in-memory catalog from [[CommonTestData]] as a
  * catalog of type "test".
  *
  * The catalog produces tables intended for either a streaming or batch environment,
  * based on the descriptor property {{{ is-streaming }}}.
  */
class TestExternalCatalogFactory extends ExternalCatalogFactory {

  override def requiredContext: util.Map[String, String] = {
    val context = new util.HashMap[String, String]
    context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_TEST)
    context.put(CATALOG_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties: util.List[String] =
    Collections.singletonList(CATALOG_IS_STREAMING)

  override def createExternalCatalog(properties: util.Map[String, String]): ExternalCatalog = {
    val props = new DescriptorProperties()
    props.putProperties(properties)

    CommonTestData.getInMemoryTestCatalog(
      isStreaming = props.getOptionalBoolean(CATALOG_IS_STREAMING).orElse(false))
  }
}

object TestExternalCatalogFactory {
  val CATALOG_TYPE_VALUE_TEST = "test"
  val CATALOG_IS_STREAMING = "is-streaming"
}
