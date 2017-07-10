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

    assertEquals("a", schema.getColumnNames.apply(0))
    assertEquals("b", schema.getColumnNames.apply(1))

    assertEquals(Types.INT, schema.getTypes.apply(0))
    assertEquals(Types.STRING, schema.getTypes.apply(1))

    val expectedString = "root\n" +
      " |-- a: Integer\n" +
      " |-- b: String\n"
    assertEquals(expectedString, schema.toString)

    assertTrue(schema.getColumnName(3).isEmpty)
    assertTrue(schema.getType(-1).isEmpty)
    assertTrue(schema.getType("c").isEmpty)
  }

  @Test
  def testStreamTableSchema(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getColumnNames.apply(0))
    assertEquals("b", schema.getColumnNames.apply(1))

    assertEquals(Types.INT, schema.getTypes.apply(0))
    assertEquals(Types.STRING, schema.getTypes.apply(1))

    val expectedString = "root\n" +
      " |-- a: Integer\n" +
      " |-- b: String\n"
    assertEquals(expectedString, schema.toString)

    assertTrue(schema.getColumnName(3).isEmpty)
    assertTrue(schema.getType(-1).isEmpty)
    assertTrue(schema.getType("c").isEmpty)
  }
}
