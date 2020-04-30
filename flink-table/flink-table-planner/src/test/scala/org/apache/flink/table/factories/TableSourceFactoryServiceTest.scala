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
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.FormatDescriptorValidator.{FORMAT_PROPERTY_VERSION, FORMAT_TYPE}
import org.apache.flink.table.factories.utils.TestFixedFormatTableFactory.{CONNECTOR_TYPE_VALUE_FIXED, FORMAT_TYPE_VALUE_TEST}
import org.apache.flink.table.factories.utils.TestWildcardFormatTableSourceFactory.CONNECTOR_TYPE_VALUE_WILDCARD
import org.apache.flink.table.factories.utils.{TestFixedFormatTableFactory, TestWildcardFormatTableSourceFactory}
import org.junit.Assert.assertTrue
import org.junit.Test

/**
  * Tests for testing table source discovery using [[TableFactoryService]]. The tests assume the
  * two table source factories [[TestFixedFormatTableFactory]] and
  * [[TestWildcardFormatTableSourceFactory]] are registered.
  *
  * The first table source has a [[FORMAT_TYPE_VALUE_TEST]] type where as the second source uses
  * a wildcard to match arbitrary formats.
  */
class TableSourceFactoryServiceTest {

  @Test
  def testValidProperties(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_FIXED)
    props.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST)
    assertTrue(TableFactoryService.find(classOf[StreamTableSourceFactory[_]], props)
      .isInstanceOf[TestFixedFormatTableFactory])
  }

  @Test(expected = classOf[NoMatchingTableFactoryException])
  def testInvalidContext(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, "unknown-connector-type")
    props.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST)
    TableFactoryService.find(classOf[StreamTableSourceFactory[_]], props)
  }

  @Test
  def testDifferentContextVersion(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_FIXED)
    props.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST)
    props.put(CONNECTOR_PROPERTY_VERSION, "2")
    // the table source should still be found
    assertTrue(TableFactoryService.find(classOf[StreamTableSourceFactory[_]], props)
      .isInstanceOf[TestFixedFormatTableFactory])
  }

  @Test(expected = classOf[NoMatchingTableFactoryException])
  def testUnsupportedProperty(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_FIXED)
    props.put(FORMAT_TYPE, FORMAT_TYPE_VALUE_TEST)
    props.put("format.unknown-format-type-property", "/new/path")
    TableFactoryService.find(classOf[StreamTableSourceFactory[_]], props)
  }

  @Test
  def testWildcardFormat(): Unit = {
    val props = properties()
    props.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_WILDCARD)
    props.put("format.unknown-format-type-property", "wildcard-property")
    val actualTableSource = TableFactoryService.find(classOf[StreamTableSourceFactory[_]], props)
    assertTrue(actualTableSource.isInstanceOf[TestWildcardFormatTableSourceFactory])
  }

  private def properties(): JMap[String, String] = {
    val properties = new JHashMap[String, String]()
    properties.put(CONNECTOR_PROPERTY_VERSION, "1")
    properties.put(FORMAT_PROPERTY_VERSION, "1")
    properties.put("format.path", "/path/to/target")
    properties.put("schema.0.name", "a")
    properties.put("schema.1.name", "b")
    properties.put("schema.2.name", "c")
    properties.put("schema.0.field.0.name", "a")
    properties.put("schema.0.field.1.name", "b")
    properties.put("schema.0.field.2.name", "c")
    properties
  }
}
