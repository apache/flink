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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{OverAgg0, WeightedAvgWithRetract}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class OverWindowValidationTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  val table: Table = streamUtil.addTable[(Int, String, Long)]("MyTable",
    'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  /**
    * Perform optimization for the input Table.
    */
  def optimizeTable(table: Table, updatesAsRetraction: Boolean): Unit = {
    val planner = streamUtil.tableEnv.getPlanner.asInstanceOf[StreamPlanner]
    planner.optimizer
      .optimize(streamUtil.toRelNode(table), updatesAsRetraction = true, planner.getRelBuilder)
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val overAgg = new OverAgg0
    table.select(overAgg('a, 'b))
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val overAgg = new OverAgg0
    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select(overAgg('long, 'int))
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows as 'w)
      .select('c, 'b.count over 'x)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testOrderBy(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'abc preceding 2.rows as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testPrecedingAndFollowingUsingIsLiteral(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2 following "xx" as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testPrecedingAndFollowingUsingSameType(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_RANGE as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testPartitionByWithUnresolved(): Unit = {
    val result = table
      .window(Over partitionBy 'a + 'b orderBy 'rowtime preceding 2.rows as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testPartitionByWithNotKeyType(): Unit = {
    val table2 = streamUtil.addTable[(Int, String, Either[Long, String])]("MyTable2", 'a, 'b, 'c)

    val result = table2
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testPrecedingValue(): Unit = {
    val result = table
      .window(Over orderBy 'rowtime preceding -1.rows as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testFollowingValue(): Unit = {
    val result = table
      .window(Over orderBy 'rowtime preceding 1.rows following -2.rows as 'w)
      .select('c, 'b.count over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test(expected = classOf[ValidationException])
  def testUdAggWithInvalidArgs(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over orderBy 'rowtime preceding 1.minutes as 'w)
      .select('c, weightedAvg('b, 'a) over 'w)
    optimizeTable(result, updatesAsRetraction = true)
  }

  @Test
  def testAccessesWindowProperties(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Window start and end properties are not available for Over windows.")

    table
    .window(Over orderBy 'rowtime preceding 1.minutes as 'w)
    .select('c, 'a.count over 'w, 'w.start + 1, 'w.end)
  }
}
