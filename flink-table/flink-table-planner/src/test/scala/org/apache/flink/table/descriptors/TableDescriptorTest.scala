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

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.Types
import org.apache.flink.table.runtime.utils.CommonTestData.Person
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

    val schema = new Schema()
      .field("myfield", Types.STRING)
      .field("myfield2", Types.INT)
      .field("myfield3", Types.MAP(Types.STRING, Types.INT))
      .field("myfield4", Types.MULTISET(Types.LONG))
      .field("myfield5", Types.PRIMITIVE_ARRAY(Types.SHORT))
      .field("myfield6", Types.OBJECT_ARRAY(TypeExtractor.createTypeInfo(classOf[Person])))
    // CSV table source and sink do not support proctime yet
    //if (isStreaming) {
    //  schema.field("proctime", Types.SQL_TIMESTAMP).proctime()
    //}

    val connector = new FileSystem()
      .path("/path/to/csv")

    val format = new OldCsv()
      .field("myfield", Types.STRING)
      .field("myfield2", Types.INT)
      .field("myfield3", Types.MAP(Types.STRING, Types.INT))
      .field("myfield4", Types.MULTISET(Types.LONG))
      .field("myfield5", Types.PRIMITIVE_ARRAY(Types.SHORT))
      .field("myfield6", Types.OBJECT_ARRAY(TypeExtractor.createTypeInfo(classOf[Person])))
      .fieldDelimiter("#")

    val descriptor = if (isStreaming) {
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
    descriptor.createTemporaryTable("MyTable")

    val personArrayString = "ARRAY<LEGACY('STRUCTURED_TYPE', " +
      "'POJO<org.apache.flink.table.runtime.utils.CommonTestData$Person>')>"

    val expectedCommonProperties = Seq(
      "connector.property-version" -> "1",
      "connector.type" -> "filesystem",
      "connector.path" -> "/path/to/csv",
      "format.property-version" -> "1",
      "format.type" -> "csv",
      "format.fields.0.name" -> "myfield",
      "format.fields.0.data-type" -> "VARCHAR(2147483647)",
      "format.fields.1.name" -> "myfield2",
      "format.fields.1.data-type" -> "INT",
      "format.fields.2.name" -> "myfield3",
      "format.fields.2.data-type" -> "MAP<VARCHAR(2147483647), INT>",
      "format.fields.3.name" -> "myfield4",
      "format.fields.3.data-type" -> "MULTISET<BIGINT>",
      "format.fields.4.name" -> "myfield5",
      "format.fields.4.data-type" -> "ARRAY<SMALLINT NOT NULL>",
      "format.fields.5.name" -> "myfield6",
      "format.fields.5.data-type" -> personArrayString,
      "format.field-delimiter" -> "#",
      "schema.0.name" -> "myfield",
      "schema.0.data-type" -> "VARCHAR(2147483647)",
      "schema.1.name" -> "myfield2",
      "schema.1.data-type" -> "INT",
      "schema.2.name" -> "myfield3",
      "schema.2.data-type" -> "MAP<VARCHAR(2147483647), INT>",
      "schema.3.name" -> "myfield4",
      "schema.3.data-type" -> "MULTISET<BIGINT>",
      "schema.4.name" -> "myfield5",
      "schema.4.data-type" -> "ARRAY<SMALLINT NOT NULL>",
      "schema.5.name" -> "myfield6",
      "schema.5.data-type" -> personArrayString
    )

    val expectedProperties = if (isStreaming) {
      expectedCommonProperties ++ Seq(
        //"schema.2.name" -> "proctime",
        //"schema.2.data-type" -> "TIMESTAMP",
        //"schema.2.proctime" -> "true",
        "update-mode" -> "append"
      )
    } else {
      expectedCommonProperties
    }

    assertEquals(expectedProperties.toMap.asJava, descriptor.toProperties)
  }
}
