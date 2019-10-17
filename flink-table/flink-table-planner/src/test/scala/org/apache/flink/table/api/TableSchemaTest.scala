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

package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class TableSchemaTest extends TableTestBase {

  @Test
  def testBatchTableSchema(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getFieldNames.apply(0))
    assertEquals("b", schema.getFieldNames.apply(1))

    assertEquals(Types.INT, schema.getFieldTypes.apply(0))
    assertEquals(Types.STRING, schema.getFieldTypes.apply(1))

    val expectedString = "root\n" +
      " |-- a: INT\n" +
      " |-- b: STRING\n"
    assertEquals(expectedString, schema.toString)

    assertTrue(!schema.getFieldName(3).isPresent)
    assertTrue(!schema.getFieldType(-1).isPresent)
    assertTrue(!schema.getFieldType("c").isPresent)
  }

  @Test
  def testStreamTableSchema(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getFieldNames.apply(0))
    assertEquals("b", schema.getFieldNames.apply(1))

    assertEquals(Types.INT, schema.getFieldTypes.apply(0))
    assertEquals(Types.STRING, schema.getFieldTypes.apply(1))

    val expectedString = "root\n" +
      " |-- a: INT\n" +
      " |-- b: STRING\n"
    assertEquals(expectedString, schema.toString)

    assertTrue(!schema.getFieldName(3).isPresent)
    assertTrue(!schema.getFieldType(-1).isPresent)
    assertTrue(!schema.getFieldType("c").isPresent)
  }

  @Test
  def testTableSchemaWithDifferentRowTypes(): Unit = {

    def createInnerRow(innerFieldName: String): TypeInformation[_] = {
      Types.ROW(
        Array[String](innerFieldName),
        Array[TypeInformation[_]](Types.INT()))
    }

    def createRow(innerFieldName: String): TypeInformation[_] = {
      Types.ROW(
        Array[String]("field"),
        Array[TypeInformation[_]](createInnerRow(innerFieldName))
      )
    }

    val util = streamTestUtil()
    util.addTable("MyTableA", 'field)(createRow("A"))
    util.addTable("MyTableB", 'field)(createRow("B"))

    val actualSchema = util.tableEnv
      .sqlQuery("SELECT MyTableA.field AS a, MyTableB.field AS b FROM MyTableA, MyTableB")
      .getSchema

    val expectedSchema = TableSchema.builder()
        .field("a", createInnerRow("A"))
        .field("b", createInnerRow("B"))
        .build()

    assertEquals(expectedSchema, actualSchema)
  }
}
