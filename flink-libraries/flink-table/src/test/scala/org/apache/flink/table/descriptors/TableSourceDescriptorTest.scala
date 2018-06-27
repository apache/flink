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

class TableSourceDescriptorTest extends TableTestBase {

  @Test
  def testStreamTableSourceDescriptor(): Unit = {
    val util = streamTestUtil()
    val desc = util.tableEnv
      .from(
        FileSystem()
          .path("/path/to/csv"))
      .withFormat(
        Csv()
          .field("myfield", Types.STRING)
          .field("myfield2", Types.INT)
          .quoteCharacter(';')
          .fieldDelimiter("#")
          .lineDelimiter("\r\n")
          .commentPrefix("%%")
          .ignoreFirstLine()
          .ignoreParseErrors())
        .withSchema(
          Schema()
            .field("myfield", Types.STRING)
            .field("myfield2", Types.INT)
            .field("proctime", Types.SQL_TIMESTAMP).proctime()
        )
    val expected = Seq(
      "connector.property-version" -> "1",
      "connector.type" -> "filesystem",
      "connector.path" -> "/path/to/csv",
      "format.property-version" -> "1",
      "format.type" -> "csv",
      "format.fields.0.name" -> "myfield",
      "format.fields.0.type" -> "VARCHAR",
      "format.fields.1.name" -> "myfield2",
      "format.fields.1.type" -> "INT",
      "format.quote-character" -> ";",
      "format.field-delimiter" -> "#",
      "format.line-delimiter" -> "\r\n",
      "format.comment-prefix" -> "%%",
      "format.ignore-first-line" -> "true",
      "format.ignore-parse-errors" -> "true",
      "schema.0.name" -> "myfield",
      "schema.0.type" -> "VARCHAR",
      "schema.1.name" -> "myfield2",
      "schema.1.type" -> "INT",
      "schema.2.name" -> "proctime",
      "schema.2.type" -> "TIMESTAMP",
      "schema.2.proctime" -> "true"
    ).toMap

    val actual = new DescriptorProperties(true)
    desc.addProperties(actual)

    assertEquals(expected.asJava, actual.asMap)
  }

  @Test
  def testBatchTableSourceDescriptor(): Unit = {
    val util = batchTestUtil()
    val desc = util.tableEnv
      .from(
        FileSystem()
          .path("/path/to/csv"))
      .withFormat(
        Csv()
          .field("myfield", Types.STRING)
          .field("myfield2", Types.INT)
          .quoteCharacter(';')
          .fieldDelimiter("#")
          .lineDelimiter("\r\n")
          .commentPrefix("%%")
          .ignoreFirstLine()
          .ignoreParseErrors())
        .withSchema(
          Schema()
            .field("myfield", Types.STRING)
            .field("myfield2", Types.INT)
        )
    val expected = Seq(
      "connector.property-version" -> "1",
      "connector.type" -> "filesystem",
      "connector.path" -> "/path/to/csv",
      "format.property-version" -> "1",
      "format.type" -> "csv",
      "format.fields.0.name" -> "myfield",
      "format.fields.0.type" -> "VARCHAR",
      "format.fields.1.name" -> "myfield2",
      "format.fields.1.type" -> "INT",
      "format.quote-character" -> ";",
      "format.field-delimiter" -> "#",
      "format.line-delimiter" -> "\r\n",
      "format.comment-prefix" -> "%%",
      "format.ignore-first-line" -> "true",
      "format.ignore-parse-errors" -> "true",
      "schema.0.name" -> "myfield",
      "schema.0.type" -> "VARCHAR",
      "schema.1.name" -> "myfield2",
      "schema.1.type" -> "INT"
    ).toMap

    val actual = new DescriptorProperties(true)
    desc.addProperties(actual)

    assertEquals(expected.asJava, actual.asMap)
  }
}
