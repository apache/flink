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

package org.apache.flink.table.factories

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.flink.table.api.NoMatchingTableFactoryException
import org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.{CATALOG_PROPERTY_VERSION, CATALOG_TYPE}
import org.apache.flink.table.factories.utils.TestExternalCatalogFactory
import org.apache.flink.table.factories.utils.TestExternalCatalogFactory.CATALOG_TYPE_VALUE_TEST
import org.junit.Assert._
import org.junit.Test

/**
  * Tests for testing external catalog discovery using [[TableFactoryService]]. The tests assume the
  * external catalog factory [[TestExternalCatalogFactory]] is registered.
  */
class ExternalCatalogFactoryServiceTest {

  @Test
  def testValidProperties(): Unit = {
    val props = properties()
    assertTrue(TableFactoryService.find(classOf[ExternalCatalogFactory], props)
      .isInstanceOf[TestExternalCatalogFactory])
  }

  @Test(expected = classOf[NoMatchingTableFactoryException])
  def testInvalidContext(): Unit = {
    val props = properties()
    props.put(CATALOG_TYPE, "unknown-catalog-type")
    TableFactoryService.find(classOf[ExternalCatalogFactory], props)
  }

  @Test
  def testDifferentContextVersion(): Unit = {
    val props = properties()
    props.put(CATALOG_PROPERTY_VERSION, "2")
    // the external catalog should still be found
    assertTrue(TableFactoryService.find(classOf[ExternalCatalogFactory], props)
      .isInstanceOf[TestExternalCatalogFactory])
  }

  @Test(expected = classOf[NoMatchingTableFactoryException])
  def testUnsupportedProperty(): Unit = {
    val props = properties()
    props.put("unknown-property", "/new/path")
    TableFactoryService.find(classOf[ExternalCatalogFactory], props)
  }

  private def properties(): JMap[String, String] = {
    val properties = new JHashMap[String, String]()
    properties.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_TEST)
    properties.put(CATALOG_PROPERTY_VERSION, "1")
    properties
  }
}
