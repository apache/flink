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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalRank, FlinkLogicalTableAggregate}
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}

import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.`type`.RelDataTypeFieldImpl
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.validate.SqlMonotonicity._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdModifiedMonotonicityTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetRelMonotonicityOnTableScan(): Unit = {
    assertEquals(
      new RelModifiedMonotonicity(Array.fill(7)(CONSTANT)),
      mq.getRelModifiedMonotonicity(studentLogicalScan))
  }

  @Test
  def testGetRelMonotonicityOnProject(): Unit = {
    // test monotonicity pass on
    // select max_c, b from
    //   (select a, b, max(c) as max_c, sum(d) as sum_d from MyTable4 group by a, b) t
    val projectWithMaxAgg = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(INCREASING, CONSTANT)),
      mq.getRelModifiedMonotonicity(projectWithMaxAgg)
    )

    // test monotonicity pass on
    // select b, max(max_c) as max_max_c) from(
    // select max_c, b from
    //   (select a, b, max(c) as max_c, sum(d) as sum_d from MyTable4 group by a, b) t
    // ) group by b
    val aggWithMaxMax = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("b")),
        relBuilder.max("max_max_c", relBuilder.field("max_c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)),
      mq.getRelModifiedMonotonicity(aggWithMaxMax)
    )

    // test monotonicity lost
    // select b, min(max_c) as min_max_c) from(
    // select max_c, b from
    //   (select a, b, max(c) as max_c, sum(d) as sum_d from MyTable4 group by a, b) t
    // ) group by b
    val aggWithMaxMin = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("b")),
        relBuilder.min("min_max_c", relBuilder.field("max_c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC)),
      mq.getRelModifiedMonotonicity(aggWithMaxMin)
    )
  }

  @Test
  def testGetRelMonotonicityOnRank(): Unit = {
    // test input monotonicity is null.
    val rank = new FlinkLogicalRank(
      cluster,
      logicalTraits,
      testRel,
      ImmutableBitSet.of(), // without partition columns
      RelCollations.of(1),
      RankType.ROW_NUMBER,
      new ConstantRankRange(1, 3),
      new RelDataTypeFieldImpl("rk", 7, longType),
      outputRankNumber = false
    )
    assertEquals(null, mq.getRelModifiedMonotonicity(rank))
  }

  @Test
  def testGetRelMonotonicityOnTableAggregateAfterScan(): Unit = {
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC, NOT_MONOTONIC)),
      mq.getRelModifiedMonotonicity(logicalTableAgg))
  }

  @Test
  def testGetRelMonotonicityOnTableAggregateAfterAggregate(): Unit = {
    val projectWithMaxAgg = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    val tableAggregate = new FlinkLogicalTableAggregate(
      cluster,
      logicalTraits,
      projectWithMaxAgg,
      ImmutableBitSet.of(0),
      null,
      Seq(tableAggCall)
    )
    assertEquals(null, mq.getRelModifiedMonotonicity(tableAggregate))
  }

  @Test
  def testGetRelMonotonicityOnWindowTableAggregate(): Unit = {
    Array(logicalWindowTableAgg, flinkLogicalWindowTableAgg, streamWindowTableAgg).foreach {
      agg =>
        assertEquals(
          new RelModifiedMonotonicity(Array.fill(agg.getRowType.getFieldCount)(CONSTANT)),
          mq.getRelModifiedMonotonicity(agg))
    }
  }

  @Test
  def testGetRelMonotonicityOnAggregate(): Unit = {
    // select b, sum(a) from (select a + 10 as a, b from MyTable3) t group by b
    val aggWithSum = relBuilder.scan("MyTable3")
      .project(
        relBuilder.alias(relBuilder.call(PLUS, relBuilder.field(0), relBuilder.literal(10)), "a"),
        relBuilder.field(1))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("b")),
        relBuilder.sum(false, "sum_a", relBuilder.field("a"))).build()
    // sum increasing
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)),
      mq.getRelModifiedMonotonicity(aggWithSum)
    )

    // select b, count(a) from MyTable3 group by b
    val aggWithCount = relBuilder.scan("MyTable3").aggregate(
      relBuilder.groupKey(relBuilder.field("b")),
      relBuilder.count(false, "count_a", relBuilder.field("a"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)),
      mq.getRelModifiedMonotonicity(aggWithCount)
    )

    // select b, max(a) from MyTable3 group by b
    val aggWithMax = relBuilder.scan("MyTable3").aggregate(
      relBuilder.groupKey(relBuilder.field("b")),
      relBuilder.max("max_a", relBuilder.field("a"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)),
      mq.getRelModifiedMonotonicity(aggWithMax)
    )

    // select b, min(a) from MyTable3 group by b
    val aggWithMin = relBuilder.scan("MyTable3").aggregate(
      relBuilder.groupKey(relBuilder.field("b")),
      relBuilder.min("min_a", relBuilder.field("a"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, DECREASING)),
      mq.getRelModifiedMonotonicity(aggWithMin)
    )

    // select a, avg(b) from MyTable3 group by a
    val aggWithAvg = relBuilder.scan("MyTable3").aggregate(
      relBuilder.groupKey(relBuilder.field("a")),
      relBuilder.avg(false, "avg_b", relBuilder.field("b"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC)),
      mq.getRelModifiedMonotonicity(aggWithAvg)
    )

    // test monotonicity lost because group by a agg field
    // select max_c, max(sum_d) as max_sum_d from (
    //   select a, b, max(c) as max_c, sum(d) as sum_d from MyTable4 group by a, b
    // ) group by max_c
    val aggWithMaxSum = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("max_c")),
        relBuilder.max("max_sum_d", relBuilder.field("sum_d")))
      .build()

    assertEquals(null, mq.getRelModifiedMonotonicity(aggWithMaxSum))

    // test monotonicity lost because min after max
    // select b, min(max_c) as min_max_c from (
    //   select a, b, max(c) as max_c, sum(d) as sum_d from MyTable4 group by a, b
    // ) group by b
    val aggWithMaxSumMin = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("b")),
        relBuilder.min("min_max_c", relBuilder.field("max_c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC)),
      mq.getRelModifiedMonotonicity(aggWithMaxSumMin)
    )

    // test monotonicity pass on
    // select b, max(max_c) as max_max_c from (
    //   select a, b, max(c) as max_c, sum(d) as sum_d from MyTable4 group by a, b
    // ) group by b
    val aggWithMaxSumMax = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "max_d", relBuilder.field("d")))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("b")),
        relBuilder.max("max_max_c", relBuilder.field("max_c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)),
      mq.getRelModifiedMonotonicity(aggWithMaxSumMax)
    )
  }

  @Test
  def testGetRelMonotonicityOnJoin(): Unit = {
    // both input is CONSTANT
    val left1 = relBuilder.scan("MyTable4")
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()
    val right1 = relBuilder.scan("MyTable4")
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()
    val join1 = relBuilder.push(left1).push(right1).join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT, CONSTANT, CONSTANT)),
      mq.getRelModifiedMonotonicity(join1)
    )

    // both input is update
    val left = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.max("max_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    val right = relBuilder.scan("MyTable4")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("a"), relBuilder.field("b")),
        relBuilder.min("min_c", relBuilder.field("c")),
        relBuilder.sum(false, "sum_d", relBuilder.field("d")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    // join condition is left.b=right.b
    val join2 = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(INCREASING, CONSTANT, DECREASING, CONSTANT)),
      mq.getRelModifiedMonotonicity(join2)
    )

    // input contains delete
    val join3 = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 1))).build()
    assertEquals(null, mq.getRelModifiedMonotonicity(join3))

    assertNull(mq.getRelModifiedMonotonicity(logicalAntiJoinNotOnUniqueKeys))
    assertNull(mq.getRelModifiedMonotonicity(logicalAntiJoinOnUniqueKeys))
  }

}

