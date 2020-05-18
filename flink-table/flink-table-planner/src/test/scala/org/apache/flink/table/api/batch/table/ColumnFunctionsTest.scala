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

package org.apache.flink.table.api.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{BatchTableTestUtil, TableTestBase}

import org.junit.Test

/**
  * Tests for column functions.
  */
class ColumnFunctionsTest extends TableTestBase {

  val util = new BatchTableTestUtil()

  private def verifyAll(tab1: Table, tab2: Table, expected: String): Unit = {
    util.verifyTable(tab1, expected)
    this.verifyTableEquals(tab1, tab2)
  }

  @Test
  def testOrderBy(): Unit = {
    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t
      .orderBy(withColumns(1, 2 to 3))

    val tab2 = t
      .orderBy("withColumns(1, 2 to 3)")

    val expected =
      unaryNode(
        "DataSetSort",
        batchTableNode(t),
        term("orderBy", "a ASC", "b ASC", "c ASC")
      )

    verifyAll(tab1, tab2, expected)
  }
}

object TestFunc extends ScalarFunction {
  def eval(a: Double, b: Long): Double = {
    a
  }
}
