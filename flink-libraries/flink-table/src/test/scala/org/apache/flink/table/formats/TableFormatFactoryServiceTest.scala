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

package org.apache.flink.table.formats

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.flink.table.api.{AmbiguousTableFormatException, NoMatchingTableFormatException}
import org.apache.flink.table.formats.utils.TestAmbiguousTableFormatFactory
import org.junit.Assert.{assertNotNull, assertTrue}
import org.junit.Test

/**
  * Tests for [[TableFormatFactoryService]].
  */
class TableFormatFactoryServiceTest {

  @Test
  def testValidProperties(): Unit = {
    val props = properties()
    assertNotNull(
      TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props))
  }

  @Test
  def testDifferentContextVersion(): Unit = {
    val props = properties()
    props.put("format.property-version", "2")
    // the format should still be found
    assertNotNull(TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props))
  }

  @Test
  def testAmbiguousMoreSupportSelection(): Unit = {
    val props = properties()
    props.remove("format.important")
    props.put("format.special_path", "/what/ever")
    assertTrue(
      TableFormatFactoryService
        .find(classOf[TableFormatFactory[_]], props)
        .isInstanceOf[TestAmbiguousTableFormatFactory])
  }

  @Test
  def testAmbiguousClassBasedSelection(): Unit = {
    val props = properties()
    props.remove("format.important")
    assertTrue(
      TableFormatFactoryService
        .find(classOf[TestAmbiguousTableFormatFactory], props)
        .isInstanceOf[TestAmbiguousTableFormatFactory])
  }

  @Test
  def testAmbiguousSchemaBasedSelection(): Unit = {
    val props = properties()
    props.remove("format.important")
    props.put("schema.weird_field", "unknown") // this is unknown to the schema derivation factory
    assertTrue(
      TableFormatFactoryService
        .find(classOf[TableFormatFactory[_]], props)
        .isInstanceOf[TestAmbiguousTableFormatFactory])
  }

  @Test(expected = classOf[NoMatchingTableFormatException])
  def testMissingClass(): Unit = {
    val props = properties()
    // this class is not a valid factory
    TableFormatFactoryService.find(classOf[TableFormatFactoryServiceTest], props)
  }

  @Test(expected = classOf[NoMatchingTableFormatException])
  def testInvalidContext(): Unit = {
    val props = properties()
    props.put("format.type", "FAIL") // no context specifies this
    TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
  }

  @Test(expected = classOf[NoMatchingTableFormatException])
  def testUnsupportedProperty(): Unit = {
    val props = properties()
    props.put("format.path_new", "/new/path") // no factory has this
    TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
  }

  @Test(expected = classOf[AmbiguousTableFormatException])
  def testAmbiguousFactory(): Unit = {
    val props = properties()
    props.remove("format.important") // now both factories match
    TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
  }

  private def properties(): JMap[String, String] = {
    val properties = new JHashMap[String, String]()
    properties.put("connector.type", "test")
    properties.put("format.type", "test-format")
    properties.put("format.important", "true")
    properties.put("connector.property-version", "1")
    properties.put("format.property-version", "1")
    properties.put("format.path", "/path/to/target")
    properties.put("schema.0.name", "a")
    properties.put("schema.1.name", "b")
    properties.put("schema.2.name", "c")
    properties.put("failing", "false")
    properties
  }
}
