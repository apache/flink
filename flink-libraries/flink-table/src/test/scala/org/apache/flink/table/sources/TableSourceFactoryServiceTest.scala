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

package org.apache.flink.table.sources

import org.apache.flink.table.api.{NoMatchingTableSourceException, TableException, ValidationException}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.FormatDescriptorValidator.{FORMAT_PROPERTY_VERSION, FORMAT_TYPE}
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.collection.mutable

class TableSourceFactoryServiceTest {

  @Test
  def testValidProperties(): Unit = {
    val props = properties()
    assertTrue(TableSourceFactoryService.findAndCreateTableSource(props.toMap) != null)
  }

  @Test(expected = classOf[NoMatchingTableSourceException])
  def testInvalidContext(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, "FAIL")
    TableSourceFactoryService.findAndCreateTableSource(props.toMap)
  }

  @Test
  def testDifferentContextVersion(): Unit = {
    val props = properties()
    props.put(CONNECTOR_PROPERTY_VERSION, "2")
    // the table source should still be found
    assertTrue(TableSourceFactoryService.findAndCreateTableSource(props.toMap) != null)
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedProperty(): Unit = {
    val props = properties()
    props.put("format.path_new", "/new/path")
    TableSourceFactoryService.findAndCreateTableSource(props.toMap)
  }

  @Test(expected = classOf[TableException])
  def testFailingFactory(): Unit = {
    val props = properties()
    props.put("failing", "true")
    TableSourceFactoryService.findAndCreateTableSource(props.toMap)
  }

  private def properties(): mutable.Map[String, String] = {
    val properties = mutable.Map[String, String]()
    properties.put(CONNECTOR_TYPE, "test")
    properties.put(FORMAT_TYPE, "test")
    properties.put(CONNECTOR_PROPERTY_VERSION, "1")
    properties.put(FORMAT_PROPERTY_VERSION, "1")
    properties.put("format.path", "/path/to/target")
    properties.put("schema.0.name", "a")
    properties.put("schema.1.name", "b")
    properties.put("schema.2.name", "c")
    properties.put("schema.0.field.0.name", "a")
    properties.put("schema.0.field.1.name", "b")
    properties.put("schema.0.field.2.name", "c")
    properties.put("failing", "false")
    properties
  }
}
