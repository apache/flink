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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.NonPojo
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

import java.sql.Timestamp

class SetOperatorsTest extends TableTestBase {

  @Test
  def testInWithFilter(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[((Int, Int), String, (Int, Int))]("A", 'a, 'b, 'c)

    val elements = t.where('b === "two").select('a).as("a1")
    val in = t.select($"*").where('c.in(elements))

    util.verifyExecPlan(in)
  }

  @Test
  def testInWithProject(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Timestamp, String)]("A", 'a, 'b, 'c)

    val in = t.select('b.in(Timestamp.valueOf("1972-02-22 07:12:00.333"))).as("b2")

    util.verifyExecPlan(in)
  }

  @Test
  def testUnionNullableTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[((Int, String), (Int, String), Int)]("A", 'a, 'b, 'c)

    val in = t.select('a)
      .unionAll(
        t.select(('c > 0) ? ('b, nullOf(createTypeInformation[(Int, String)]))))
    util.verifyExecPlan(in)
  }

  @Test
  def testUnionAnyType(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource("A",
      Array[TypeInformation[_]](
        new GenericTypeInfo(classOf[NonPojo]),
        new GenericTypeInfo(classOf[NonPojo])),
      Array("a", "b"))
    val in = t.select('a).unionAll(t.select('b))
    util.verifyExecPlan(in)
  }

  @Test
  def testFilterUnionTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.unionAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)

    util.verifyExecPlan(result)
  }

  @Test
  def testFilterMinusTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.minusAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)

    util.verifyExecPlan(result)
  }

  @Test
  def testProjectUnionTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
                 .unionAll(right.select('a, 'b, 'c))
                 .select('b, 'c)

    util.verifyExecPlan(result)

  }

  @Test
  def testProjectMinusTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
                 .minusAll(right.select('a, 'b, 'c))
                 .select('b, 'c)

    util.verifyExecPlan(result)

  }
}
