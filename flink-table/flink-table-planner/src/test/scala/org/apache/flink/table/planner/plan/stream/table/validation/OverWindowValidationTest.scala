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
package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithRetract
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.OverAgg0
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase, TableTestUtil}

import org.apache.calcite.rel.RelNode
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class OverWindowValidationTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  val table: Table = streamUtil
    .addDataStream[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  /** OVER clause is necessary for [[OverAgg0]] window function. */
  @Test
  def testInvalidOverAggregation(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'a, 'b, 'c)
    val overAgg = new OverAgg0
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => table.select(overAgg('a, 'b)))
  }

  /** OVER clause is necessary for [[OverAgg0]] window function. */
  @Test
  def testInvalidOverAggregation2(): Unit = {
    val util = streamTestUtil()

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val table = util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'string, 'proctime)
          val overAgg = new OverAgg0
          table
            .window(Tumble.over(2.rows).on('proctime).as('w))
            .groupBy('w, 'string)
            .select(overAgg('long, 'int))
        })
  }

  @Test
  def testInvalidWindowAlias(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(Over.partitionBy('c).orderBy('rowtime).preceding(2.rows).as('w))
            .select('c, 'b.count.over('x))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testOrderBy(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(Over.partitionBy('c).orderBy('abc).preceding(2.rows).as('w))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testPrecedingAndFollowingUsingIsLiteral(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(
              Over.partitionBy($"c").orderBy($"rowtime").preceding(2).following($"xx").as($"w"))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testPrecedingAndFollowingUsingSameType(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(
              Over
                .partitionBy('c)
                .orderBy('rowtime)
                .preceding(2.rows)
                .following(CURRENT_RANGE)
                .as('w))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testPartitionByWithUnresolved(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(Over.partitionBy('a + 'b).orderBy('rowtime).preceding(2.rows).as('w))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testPartitionByWithNotKeyType(): Unit = {
    val table2 =
      streamUtil.addTableSource[(Int, String, Either[Long, String])]("MyTable2", 'a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table2
            .window(Over.partitionBy('c).orderBy('rowtime).preceding(2.rows).as('w))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testPrecedingValue(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(Over.orderBy('rowtime).preceding(-1.rows).as('w))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testFollowingValue(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(Over.orderBy('rowtime).preceding(1.rows).following(-2.rows).as('w))
            .select('c, 'b.count.over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testUdAggWithInvalidArgs(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .window(Over.orderBy('rowtime).preceding(1.minutes).as('w))
            .select('c, weightedAvg('b, 'a).over('w))
          optimize(TableTestUtil.toRelNode(result))
        })
  }

  @Test
  def testAccessesWindowProperties(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Over.orderBy('rowtime).preceding(1.minutes).as('w))
            .select('c, 'a.count.over('w), 'w.start + 1, 'w.end))
      .withMessageContaining("Window start and end properties are not available for Over windows.")
  }

  private def optimize(rel: RelNode): RelNode = {
    val planner = streamUtil.tableEnv.asInstanceOf[TableEnvironmentImpl].getPlanner
    planner.asInstanceOf[PlannerBase].optimize(Seq(rel)).head
  }
}
