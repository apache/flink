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

package org.apache.flink.table.plan.metadata

import org.apache.calcite.rel.RelCollations
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.functions.sql.AggSqlFunctions
import org.apache.flink.table.plan.`trait`.RelModifiedMonotonicity
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.validate.SqlMonotonicity._
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.plan.util.ConstantRankRange
import org.junit.Assert._
import org.junit.Test

class FlinkRelMdModifiedMonotonicityTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetRelMonotonicityOnTableScan(): Unit = {
    val ts = relBuilder.scan("student").build()
    assertEquals(
      new RelModifiedMonotonicity(Array.fill(4)(CONSTANT)),
      mq.getRelModifiedMonotonicity(ts))
  }

  @Test
  def testGetRelMonotonicityOnSingleAggregate(): Unit = {
    // sum increasing
    val sumagg = relBuilder.scan("t1")
      .project(
        relBuilder.call(
          PLUS,
          relBuilder.field(0),
          relBuilder.literal(10)), relBuilder.field(1))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("score")),
        relBuilder.sum(false, "s", relBuilder.field("$f0"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)).toString,
      mq.getRelModifiedMonotonicity(sumagg).toString
    )

    // count
    val agg = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("score")),
      relBuilder.count(false, "c", relBuilder.field("id"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)).toString,
      mq.getRelModifiedMonotonicity(agg).toString
    )

    // max
    val maxagg = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("score")),
      relBuilder.max("c", relBuilder.field("id"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)).toString,
      mq.getRelModifiedMonotonicity(maxagg).toString
    )

    // min
    val minagg = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("score")),
      relBuilder.min("c", relBuilder.field("id"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, DECREASING)).toString,
      mq.getRelModifiedMonotonicity(minagg).toString
    )

    // avg
    val agg2 = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("id")),
      relBuilder.avg(false, "avg_score", relBuilder.field("score"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC)).toString,
      mq.getRelModifiedMonotonicity(agg2).toString
    )

    // incr_sum agg
    val incr_sumagg = relBuilder.scan("t1").aggregate(
      relBuilder.groupKey(relBuilder.field("id")),
      relBuilder.aggregateCall(AggSqlFunctions.INCR_SUM, false, null,
        "avg_score", relBuilder.field("score"))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)).toString,
      mq.getRelModifiedMonotonicity(incr_sumagg).toString
    )
  }

  @Test
  def testGetRelMonotonicityOnTwoAggregate(): Unit = {

    // test monotonicity lost because group by a agg field
    val maxagg1 = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("c")),
        relBuilder.max("d", relBuilder.field("d")))
      .build()

    assertEquals(
      null,
      mq.getRelModifiedMonotonicity(maxagg1)
    )

    // test monotonicity lost because min after max
    val maxagg2 = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("score")),
        relBuilder.min("c", relBuilder.field("c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC)).toString,
      mq.getRelModifiedMonotonicity(maxagg2).toString
    )

    // test monotonicity pass on
    val maxagg3 = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)).toString,
      mq.getRelModifiedMonotonicity(maxagg3).toString
    )
  }

  @Test
  def testGetRelMonotonicityOnProject(): Unit = {

    // test monotonicity pass on
    val maxagg1 = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(INCREASING, CONSTANT)).toString,
      mq.getRelModifiedMonotonicity(maxagg1).toString
    )

    // test monotonicity pass on
    val maxagg2 = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)).toString,
      mq.getRelModifiedMonotonicity(maxagg2).toString
    )

    // test monotonicity lost
    val maxagg3 = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("c", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .aggregate(
        relBuilder.groupKey(relBuilder.field("score")),
        relBuilder.min("c", relBuilder.field("c")))
      .build()

    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC)).toString,
      mq.getRelModifiedMonotonicity(maxagg3).toString
    )
  }

  @Test
  def testGetRelMonotonicityOnJoin(): Unit = {

    // both input is CONSTANT
    val left0 = relBuilder.scan("student")
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    val right0 = relBuilder.scan("student")
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()
    val join0 = relBuilder.push(left0).push(right0).join(JoinRelType.LEFT,
       relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT, CONSTANT, CONSTANT)).toString,
      mq.getRelModifiedMonotonicity(join0).toString
    )

    // both input is update
    val left = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.max("lmax", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    val right = relBuilder.scan("student")
      .aggregate(
        relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("score")),
        relBuilder.min("rmin", relBuilder.field("age")),
        relBuilder.sum(false, "d", relBuilder.field("height")))
      .project(relBuilder.field(2), relBuilder.field(1))
      .build()

    // join condition is left.$1=right.$1
    val join1 = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 1))).build()
    assertEquals(
      new RelModifiedMonotonicity(Array(INCREASING, CONSTANT, DECREASING, CONSTANT))
        .toString,
      mq.getRelModifiedMonotonicity(join1).toString
    )

    //input contains delete
    val join2 = relBuilder.push(left).push(right).join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 1))).build()
    assertEquals(null, mq.getRelModifiedMonotonicity(join2))
  }

  @Test
  def testGetRelMonotonicityOnRank(): Unit = {
    // test input monotonicity is null.
    val logicalRank = new FlinkLogicalRank(
      cluster,
      logicalTraits,
      flinkLogicalWindowAgg,
      SqlStdOperatorTable.ROW_NUMBER,
      ImmutableBitSet.of(), // without partition columns
      RelCollations.of(1),
      ConstantRankRange(1, 3),
      outputRankFunColumn = false
    )
    assertEquals(null, mq.getRelModifiedMonotonicity(logicalRank))
  }
}

class Func0 extends ScalarFunction {

  def eval(index: Int): Int = {
    index
  }
}
