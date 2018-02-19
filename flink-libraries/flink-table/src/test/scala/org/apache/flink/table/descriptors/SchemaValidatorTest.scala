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

import org.apache.flink.table.api.{TableSchema, Types, ValidationException}
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

/**
  * Tests for [[SchemaValidator]].
  */
class SchemaValidatorTest {

  @Test
  def testSchema(): Unit = {
     val desc1 = Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val inputSchema = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("abcField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .build()

    val expected = TableSchema.builder()
      .field("otherField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP)
      .field("r", Types.SQL_TIMESTAMP)
      .build()

    // test schema
    assertEquals(expected, SchemaValidator.deriveSchema(props, Some(inputSchema)))

    // test proctime
    assertEquals(Some("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[StreamRecordTimestamp])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map("otherField" -> "csvField")
    assertEquals(expectedMapping, SchemaValidator.deriveFieldMapping(props, Some(inputSchema)))
  }

  @Test
  def testSchemaAlphabetically(): Unit = {
     val desc1 = Schema()
      .deriveFieldsAlphabetically()
      .field("otherField", Types.STRING).from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val inputSchema = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("abcField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .build()

    val expected = TableSchema.builder()
      .field("abcField", Types.STRING) // field is here
      .field("csvField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .field("otherField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP)
      .field("r", Types.SQL_TIMESTAMP)
      .build()

    // test schema
    assertEquals(expected, SchemaValidator.deriveSchema(props, Some(inputSchema)))

    // test proctime
    assertEquals(Some("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[StreamRecordTimestamp])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map(
      "csvField" -> "csvField",
      "abcField" -> "abcField",
      "myField" -> "myField",
      "otherField" -> "csvField")
    assertEquals(expectedMapping, SchemaValidator.deriveFieldMapping(props, Some(inputSchema)))
  }

  @Test
  def testSchemaSequentially(): Unit = {
     val desc1 = Schema()
      .deriveFieldsSequentially()
      .field("otherField", Types.STRING).from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
      Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val inputSchema = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("abcField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .build()

    val expected = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("abcField", Types.STRING) // field is here
      .field("myField", Types.BOOLEAN)
      .field("otherField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP)
      .field("r", Types.SQL_TIMESTAMP)
      .build()

    // test schema
    assertEquals(expected, SchemaValidator.deriveSchema(props, Some(inputSchema)))

    // test proctime
    assertEquals(Some("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[StreamRecordTimestamp])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map(
      "csvField" -> "csvField",
      "abcField" -> "abcField",
      "myField" -> "myField",
      "otherField" -> "csvField")
    assertEquals(expectedMapping, SchemaValidator.deriveFieldMapping(props, Some(inputSchema)))
  }

  @Test(expected = classOf[ValidationException])
  def testDeriveSchemaWithFieldOverwriting(): Unit = {
     val desc1 = Schema()
      .deriveFieldsAlphabetically()
      .field("myField", Types.BOOLEAN)  // we do not allow this
      .field("otherField", Types.STRING).from("csvField")
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
      Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val inputSchema = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .build()

    val expected = TableSchema.builder()
      .field("myField", Types.BOOLEAN)
      .field("otherField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP)
      .field("r", Types.SQL_TIMESTAMP)
      .build()

    assertEquals(expected, SchemaValidator.deriveSchema(props, Some(inputSchema)))
  }
}
