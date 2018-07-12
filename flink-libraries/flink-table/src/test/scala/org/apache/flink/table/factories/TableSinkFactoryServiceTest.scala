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
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator._
import org.apache.flink.table.factories.utils.TestTableSinkFactory
import org.apache.flink.table.factories.utils.TestTableSinkFactory._
import org.junit.Assert._
import org.junit.Test

/**
  * Tests for testing table sink discovery using [[TableFactoryService]]. The tests assume the
  * table sink factory [[TestTableSinkFactory]] is registered.
  */
class TableSinkFactoryServiceTest {

  @Test
  def testValidProperties(): Unit = {
    val props = properties()
    assertTrue(TableFactoryService.find(classOf[StreamTableSinkFactory[_]], props)
      .isInstanceOf[TestTableSinkFactory])
  }

  @Test(expected = classOf[NoMatchingTableFactoryException])
  def testInvalidContext(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, "unknown-connector-type")
    TableFactoryService.find(classOf[StreamTableSinkFactory[_]], props)
  }

  @Test
  def testDifferentContextVersion(): Unit = {
    val props = properties()
    props.put(CONNECTOR_PROPERTY_VERSION, "2")
    // the table source should still be found
    assertTrue(TableFactoryService.find(classOf[StreamTableSinkFactory[_]], props)
      .isInstanceOf[TestTableSinkFactory])
  }

  @Test(expected = classOf[NoMatchingTableFactoryException])
  def testUnsupportedProperty(): Unit = {
    val props = properties()
    props.put("format.path_new", "/new/path")
    TableFactoryService.find(classOf[StreamTableSinkFactory[_]], props)
  }

  private def properties(): JMap[String, String] = {
    val properties = new JHashMap[String, String]()
    properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_TEST)
    properties.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST)
    properties.put(CONNECTOR_PROPERTY_VERSION, "1")
    properties.put(FORMAT_PROPERTY_VERSION, "1")
    properties.put(FORMAT_PATH, "/path/to/target")
    properties.put("schema.0.name", "a")
    properties.put("schema.1.name", "b")
    properties.put("schema.2.name", "c")
    properties.put("schema.0.field.0.name", "a")
    properties.put("schema.0.field.1.name", "b")
    properties.put("schema.0.field.2.name", "c")
    properties
  }
}
