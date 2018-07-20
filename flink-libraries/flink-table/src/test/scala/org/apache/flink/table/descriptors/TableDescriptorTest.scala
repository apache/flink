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

import org.apache.flink.table.api.Types
import org.apache.flink.table.utils.TableTestBase
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Tests for [[TableDescriptor]].
  */
class TableDescriptorTest extends TableTestBase {

  @Test
  def testStreamTableSourceDescriptor(): Unit = {
    testTableSourceDescriptor(true)
  }

  @Test
  def testBatchTableSourceDescriptor(): Unit = {
    testTableSourceDescriptor(false)
  }

  private def testTableSourceDescriptor(isStreaming: Boolean): Unit = {

    val schema = Schema()
      .field("myfield", Types.STRING)
      .field("myfield2", Types.INT)
    // CSV table source and sink do not support proctime yet
    //if (isStreaming) {
    //  schema.field("proctime", Types.SQL_TIMESTAMP).proctime()
    //}

    val connector = FileSystem()
      .path("/path/to/csv")

    val format = Csv()
      .field("myfield", Types.STRING)
      .field("myfield2", Types.INT)
      .fieldDelimiter("#")

    val descriptor: RegistrableDescriptor = if (isStreaming) {
      streamTestUtil().tableEnv
        .connect(connector)
        .withFormat(format)
        .withSchema(schema)
        .inAppendMode()
    } else {
      batchTestUtil().tableEnv
        .connect(connector)
        .withFormat(format)
        .withSchema(schema)
    }

    // tests the table factory discovery and thus validates the result automatically
    descriptor.registerTableSourceAndSink("MyTable")

    val expectedCommonProperties = Seq(
      "connector.property-version" -> "1",
      "connector.type" -> "filesystem",
      "connector.path" -> "/path/to/csv",
      "format.property-version" -> "1",
      "format.type" -> "csv",
      "format.fields.0.name" -> "myfield",
      "format.fields.0.type" -> "VARCHAR",
      "format.fields.1.name" -> "myfield2",
      "format.fields.1.type" -> "INT",
      "format.field-delimiter" -> "#",
      "schema.0.name" -> "myfield",
      "schema.0.type" -> "VARCHAR",
      "schema.1.name" -> "myfield2",
      "schema.1.type" -> "INT"
    )

    val expectedProperties = if (isStreaming) {
      expectedCommonProperties ++ Seq(
        //"schema.2.name" -> "proctime",
        //"schema.2.type" -> "TIMESTAMP",
        //"schema.2.proctime" -> "true",
        "update-mode" -> "append"
      )
    } else {
      expectedCommonProperties
    }

    val actualProperties = new DescriptorProperties(true)
    descriptor.addProperties(actualProperties)

    assertEquals(expectedProperties.toMap.asJava, actualProperties.asMap)
  }
}
