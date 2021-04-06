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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SetOperatorsTest extends TableTestBase {

  @Test
  def testFilterUnionTranspose(): Unit = {
    val util = streamTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.unionAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)
    util.verifyExecPlan(result)
  }

  @Test
  def testProjectUnionTranspose(): Unit = {
    val util = streamTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
      .unionAll(right.select('a, 'b, 'c))
      .select('b, 'c)

    util.verifyExecPlan(result)
  }

  @Test
  def testInUncorrelated(): Unit = {
    val util = streamTestUtil()
    val tableA = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)
    val tableB = util.addTableSource[(Int, String)]('x, 'y)

    val result = tableA.where('a.in(tableB.select('x)))
    util.verifyExecPlan(result)
  }

  @Test
  def testInUncorrelatedWithConditionAndAgg(): Unit = {
    val util = streamTestUtil()
    val tableA = util.addTableSource[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val tableB = util.addTableSource[(Int, String)]("tableB", 'x, 'y)

    val result = tableA
      .where('a.in(tableB.where('y.like("%Hanoi%")).groupBy('y).select('x.sum)))
    util.verifyExecPlan(result)
  }

  @Test
  def testInWithMultiUncorrelatedCondition(): Unit = {
    val util = streamTestUtil()
    val tableA = util.addTableSource[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val tableB = util.addTableSource[(Int, String)]("tableB", 'x, 'y)
    val tableC = util.addTableSource[(Long, Int)]("tableC", 'w, 'z)

    val result = tableA
      .where('a.in(tableB.select('x)) && 'b.in(tableC.select('w)))
    util.verifyExecPlan(result)
  }
}
