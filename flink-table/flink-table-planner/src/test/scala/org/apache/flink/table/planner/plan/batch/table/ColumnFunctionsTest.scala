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

package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
  * Tests for column functions.
  */
class ColumnFunctionsTest extends TableTestBase {

  private val util = batchTestUtil()

  @Test
  def testOrderBy(): Unit = {
    val t = util.addTableSource[(Int, Long, String, Int, Long, String)](
      'a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t.orderBy(withColumns(1, 2 to 3))
    val tab2 = t.orderBy("withColumns(1, 2 to 3)")
    verifyTableEquals(tab1, tab2)
    util.verifyExecPlan(tab1)
  }
}

@SerialVersionUID(1L)
object TestFunc extends ScalarFunction {
  def eval(a: Double, b: Long): Double = {
    a
  }
}
