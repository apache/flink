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

import org.apache.flink.table.api.{DataTypes, TableException, TableSchema, Types}
import org.apache.flink.table.descriptors.RowtimeTest.CustomExtractor
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp}
import org.apache.flink.table.sources.wmstrategies.{BoundedOutOfOrderTimestamps, PreserveWatermarks}

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.util.Optional

import scala.collection.JavaConverters._

/**
  * Tests for [[SchemaValidator]].
  */
class SchemaValidatorTest {

  @Test
  def testSchemaWithRowtimeFromSource(): Unit = {
     val desc1 = new Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
       new Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    props.putProperties(desc1.toProperties)

    val inputSchema = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("abcField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .build()

    // test proctime
    assertEquals(Optional.of("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[StreamRecordTimestamp])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map(
      "otherField" -> "csvField",
      "csvField" -> "csvField",
      "abcField" -> "abcField",
      "myField" -> "myField").asJava
    assertEquals(
      expectedMapping,
      SchemaValidator.deriveFieldMapping(props, Optional.of(inputSchema.toRowType)))
  }

  @Test(expected = classOf[TableException])
  def testDeriveTableSinkSchemaWithRowtimeFromSource(): Unit = {
    val desc1 = new Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
      new Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    props.putProperties(desc1.toProperties)

    SchemaValidator.deriveTableSinkSchema(props)
  }

  @Test
  def testDeriveTableSinkSchemaWithRowtimeFromField(): Unit = {
    // we have to use DataTypes here because TypeInformation -> properties -> DataType
    // loses information (conversion class)
    val desc1 = new Schema()
      .field("otherField", DataTypes.STRING()).from("csvField")
      .field("abcField", DataTypes.STRING())
      .field("p", DataTypes.TIMESTAMP(3)).proctime()
      .field("r", DataTypes.TIMESTAMP(3)).rowtime(
      new Rowtime().timestampsFromField("myTime").watermarksFromSource())
    val props = new DescriptorProperties()
    props.putProperties(desc1.toProperties)

    val expectedTableSinkSchema = TableSchema.builder()
      .field("csvField", DataTypes.STRING()) // aliased
      .field("abcField", DataTypes.STRING())
      .field("myTime", DataTypes.TIMESTAMP(3))
      .build()

    assertEquals(expectedTableSinkSchema, SchemaValidator.deriveTableSinkSchema(props))
  }

  @Test
  def testSchemaWithRowtimeFromField(): Unit = {
     val desc1 = new Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
       new Rowtime().timestampsFromField("myTime").watermarksFromSource())
    val props = new DescriptorProperties()
    props.putProperties(desc1.toProperties)

    val inputSchema = TableSchema.builder()
      .field("csvField", Types.STRING)
      .field("abcField", Types.STRING)
      .field("myField", Types.BOOLEAN)
      .field("myTime", Types.SQL_TIMESTAMP)
      .build()

    // test proctime
    assertEquals(Optional.of("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[ExistingField])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map(
      "otherField" -> "csvField",
      "csvField" -> "csvField",
      "abcField" -> "abcField",
      "myField" -> "myField",
      "myTime" -> "myTime").asJava
    assertEquals(
      expectedMapping,
      SchemaValidator.deriveFieldMapping(props, Optional.of(inputSchema.toRowType)))
  }

  @Test
  def testSchemaWithRowtimeCustomTimestampExtractor(): Unit = {
    val descriptor = new Schema()
      .field("f1", Types.STRING)
      .field("f2", Types.STRING)
      .field("f3", Types.SQL_TIMESTAMP)
      .field("rt", Types.SQL_TIMESTAMP).rowtime(
      new Rowtime().timestampsFromExtractor(new CustomExtractor("f3"))
          .watermarksPeriodicBounded(1000L))
    val properties = new DescriptorProperties()
    properties.putProperties(descriptor.toProperties)

    val rowtime = SchemaValidator.deriveRowtimeAttributes(properties).get(0)
    assertEquals("rt", rowtime.getAttributeName)
    val extractor = rowtime.getTimestampExtractor
    assertTrue(extractor.equals(new CustomExtractor("f3")))
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[BoundedOutOfOrderTimestamps])
  }

  @Test
  def testSchemaWithGeneratedColumnAndWatermark(): Unit = {
    val properties = new DescriptorProperties()
    properties.putString("schema.0.name", "f1")
    properties.putString("schema.0.data-type", DataTypes.STRING().toString)
    properties.putString("schema.1.name", "computed-column")
    properties.putString("schema.1.data-type", DataTypes.INT().toString)
    properties.putString("schema.1.expr", "f5 + 1")
    properties.putString("schema.2.name", "f2")
    properties.putString("schema.2.data-type", DataTypes.INT().toString)
    properties.putString("schema.3.name", "f3")
    properties.putString("schema.3.data-type", DataTypes.TIMESTAMP(3).toString)
    properties.putString("schema.4.name", "row-time")
    properties.putString("schema.4.data-type", DataTypes.TIMESTAMP(3).toString)
    properties.putString("schema.4.rowtime.timestamps.type", "from-field")
    properties.putString("schema.4.rowtime.timestamps.from", "f4")
    properties.putString("schema.4.rowtime.watermarks.type", "periodic-bounded")
    properties.putString("schema.4.rowtime.watermarks.delay", "5000")
    properties.putString("schema.5.name", "f5")
    properties.putString("schema.5.data-type", DataTypes.INT().toString)
    properties.putString("schema.watermark.0.rowtime", "row-time")
    properties.putString("schema.watermark.0.strategy.expr", "row-time - INTERVAL '5' SECOND")
    properties.putString("schema.watermark.0.strategy.data-type", DataTypes.TIMESTAMP(3).toString)

    new SchemaValidator(true, true, false).validate(properties)
    val expectd = TableSchema.builder()
      .field("f1", DataTypes.STRING())
      .field("f2", DataTypes.INT())
      .field("f3", DataTypes.TIMESTAMP(3))
      .field("f4", DataTypes.TIMESTAMP(3))
      .field("f5", DataTypes.INT())
      .build()
    val schema = SchemaValidator.deriveTableSinkSchema(properties)
    assertEquals(expectd, schema)
   }
}
