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
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.TableTestBase
import org.junit.Assert.{assertEquals, assertFalse, fail}
import org.junit.Test

class TableSchemaTest extends TableTestBase {

  @Test
  def testStreamTableSchema(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getColumnNames.apply(0))
    assertEquals("b", schema.getColumnNames.apply(1))

    assertEquals(DataTypes.INT, schema.getTypes.apply(0).toInternalType)
    assertEquals(DataTypes.STRING, schema.getTypes.apply(1).toInternalType)

    val expectedString = "root\n" +
      " |-- name: a\n"      +
      " |-- type: IntType\n" +
      " |-- isNullable: true\n" +
      " |-- name: b\n" +
      " |-- type: StringType\n" +
      " |-- isNullable: true\n"
    assertEquals(expectedString, schema.toString)

    assertEquals("a", schema.getColumnName(0))

    try {
      schema.getColumnName(-1)
      fail("Should never reach here")
    } catch {
      case _ =>
    }

    try {
      schema.getType(-1)
      fail("Should never reach here")
    } catch {
      case _ =>
    }

    assertFalse(schema.getType("c").isPresent)
  }

}
