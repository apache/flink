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
package org.apache.flink.table.api.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Slide, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

/**
  * Tests to validate exceptions for column operations. This test can also cover the batch
  * scenarios.
  */
class ColumnsOperationValidationTest extends TableTestBase {

  val util = new StreamTableTestUtil()

  @Test
  def testIndexRangeInvalid(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(
      "The start:2 of columns() should not bigger than end:1")

    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)
    val tab = t.select(columns(2 to 1))
    util.verifyTable(tab, "")
  }

  @Test
  def testNameRangeInvalid(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(
      "The start name:b of columns() should not behind the end:a.")

    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)
    val tab = t.select(columns('b to 'a))
    util.verifyTable(tab, "")
  }

  @Test
  def testInvalidParameters(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "The parameters of columns() only accept column name or column index.")

    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)
    val tab = t.select(columns(concat('f)))
    util.verifyTable(tab, "")
  }

  @Test
  def testInvalidRenameColumns(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Invalid AS, parameters are: ['a, 'b, a].")

    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)
    val tab = t.renameColumns(columns(1 to 2) as 'a) // failed, invalid as
    util.verifyTable(tab, "")
  }

  @Test
  def testInvalidWindowTimeField(): Unit = {
    expectedException.expect(classOf[AssertionError])
    expectedException.expectMessage(
      "Group Window only supports a single time field column.")

    val t = util.addTable[(Int, Long, String, Int)]('a, 'b.rowtime, 'c, 'd)
    val tab = t
      // failed, time field only support one column
      .window(Slide over 3.milli every 10.milli on columns('b, 'a) as 'w)
      .groupBy(columns('a, 'b), 'w)
      .select(columns(1 to 2), columns('c).count as 'c)

    util.verifyTable(tab, "")
  }
}
