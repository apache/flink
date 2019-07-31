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

package org.apache.flink.table.planner.plan.batch.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Ignore, Test}

import java.sql.Timestamp

class SetOperatorsTest extends TableTestBase {

  @Ignore("Support in subQuery in RexNodeConverter")
  @Test
  def testInWithFilter(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[((Int, Int), String, (Int, Int))]("A", 'a, 'b, 'c)

    val elements = t.where("b === 'two'").select("a").as("a1")
    val in = t.select("*").where('c.in(elements))

    util.verifyPlan(in)
  }

  @Test
  def testInWithProject(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Timestamp, String)]("A", 'a, 'b, 'c)

    val in = t.select("b.in('1972-02-22 07:12:00.333'.toTimestamp)").as("b2")

    util.verifyPlan(in)
  }
}
