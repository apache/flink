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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{TableFunc0, TableFunc1, TableTestBase}
import org.junit.{Ignore, Test}

class CorrelateTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result1 = table.join(function('c) as 's).select('c, 's)

    util.verifyPlan(result1)
  }

  @Test
  def testCrossJoin2(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result2 = table.join(function('c, "$") as 's).select('c, 's)

    util.verifyPlan(result2)
  }

  @Test
  def testLeftOuterJoinWithoutJoinPredicates(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result = table.leftOuterJoin(function('c) as 's).select('c, 's).where('s > "")

    util.verifyPlan(result)
  }

  // TODO to FLINK-7853.
  @Ignore
  @Test
  def testLeftOuterJoinWithLiteralTrue(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result = table.leftOuterJoin(function('c) as 's, true).select('c, 's)

    util.verifyPlan(result)
  }

  @Test
  def testCorrelateWithMultiFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc0)

    val result = sourceTable.select('a, 'b, 'c)
      .join(function('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    util.verifyPlan(result)
  }

  @Test
  def testCorrelateAfterConcatAggWithConstantParam(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(String)]("MyTable1", 'a)
    val function = util.addFunction("func1", new TableFunc0)
    val left = sourceTable.groupBy("5").select('a.concat_agg("#") as 'a)
    val result = left.join(function('a))

    util.verifyPlan(result)
  }



}
