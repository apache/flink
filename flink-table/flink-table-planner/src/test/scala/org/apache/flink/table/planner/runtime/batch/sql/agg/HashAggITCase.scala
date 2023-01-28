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
package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/** AggregateITCase using HashAgg Operator. */
@RunWith(classOf[Parameterized])
class HashAggITCase(adaptiveLocalHashAggEnable: Boolean)
  extends AggregateITCaseBase("HashAggregate") {

  override def prepareAggOp(): Unit = {
    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    if (adaptiveLocalHashAggEnable) {
      tEnv.getConfig
        .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
      tEnv.getConfig.set(
        HashAggCodeGenerator.TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED,
        Boolean.box(true))
      tEnv.getConfig.set(
        HashAggCodeGenerator.TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD,
        Long.box(5L))
    }
  }

  @Test
  def testAdaptiveLocalHashAggWithHighAggregationDegree(): Unit = {
    checkQuery(
      Seq(
        (1, 1, 1, 1, 1L, 1.1d),
        (1, 1, 1, 2, 1L, 1.2d),
        (1, 1, 2, 3, 2L, 2.2d),
        (1, 1, 2, 2, 2L, 1d),
        (1, 1, 3, 3, 3L, 3d),
        (1, 1, 2, 2, 2L, 4d),
        (1, 2, 1, 1, 1L, 1.1d),
        (1, 2, 1, 2, 1L, 2.3d),
        (1, 3, 1, 1, 1L, 3.3d),
        (1, 4, 1, 1, 1L, 1.1d),
        (2, 1, 2, 2, 2L, 2.2d),
        (2, 2, 3, 3, 3L, 3.3d)
      ),
      """
        | SELECT f0, f1, sum(f2), avg(f2), max(f3), min(f3), count(f3), count(*), sum(f4), sum(f5), avg(f4), avg(f5)
        | FROM TableName GROUP BY f0, f1
        |""".stripMargin,
      Seq(
        (1, 1, 11, 1, 3, 1, 6, 6, 11, 12.5, 1, 2.0833333333333335),
        (1, 2, 2, 1, 2, 1, 2, 2, 2, 3.4, 1, 1.7),
        (1, 3, 1, 1, 1, 1, 1, 1, 1, 3.3, 1, 3.3),
        (1, 4, 1, 1, 1, 1, 1, 1, 1, 1.1, 1, 1.1),
        (2, 1, 2, 2, 2, 2, 1, 1, 2, 2.2, 2, 2.2),
        (2, 2, 3, 3, 3, 3, 1, 1, 3, 3.3, 3, 3.3)
      )
    )
  }

  @Test
  def testAdaptiveLocalHashAggWithLowAggregationDegree(): Unit = {
    checkQuery(
      Seq(
        (1, 1, 1, 1, 1L, 1.1d),
        (1, 1, 1, 2, 1L, 1.2d),
        (1, 2, 2, 3, 2L, 2.2d),
        (1, 3, 2, 2, 2L, 1d),
        (1, 4, 3, 3, 3L, 3d),
        (1, 5, 2, 2, 2L, 4d),
        (1, 6, 1, 1, 1L, 3.3d),
        (2, 1, 1, 2, 1L, 2.3d),
        (2, 2, 1, 1, 1L, 3.3d),
        (2, 3, 1, 1, 1L, 3.3d),
        (2, 3, 2, 2, 2L, 2.2d),
        (2, 3, 3, 3, 3L, 3.3d)
      ),
      """
        | SELECT f0, f1, sum(f2), avg(f2), max(f3), min(f3), count(f3), count(*), sum(f4), sum(f5), avg(f4), avg(f5)
        | FROM TableName GROUP BY f0, f1
        |""".stripMargin,
      Seq(
        (1, 1, 2, 1, 2, 1, 2, 2, 2, 2.3, 1, 1.15),
        (1, 2, 2, 2, 3, 3, 1, 1, 2, 2.2, 2, 2.2),
        (1, 3, 2, 2, 2, 2, 1, 1, 2, 1.0, 2, 1.0),
        (1, 4, 3, 3, 3, 3, 1, 1, 3, 3.0, 3, 3.0),
        (1, 5, 2, 2, 2, 2, 1, 1, 2, 4.0, 2, 4.0),
        (1, 6, 1, 1, 1, 1, 1, 1, 1, 3.3, 1, 3.3),
        (2, 1, 1, 1, 2, 2, 1, 1, 1, 2.3, 1, 2.3),
        (2, 2, 1, 1, 1, 1, 1, 1, 1, 3.3, 1, 3.3),
        (2, 3, 6, 2, 3, 1, 3, 3, 6, 8.8, 2, 2.9333333333333336)
      )
    )
  }

  @Test
  def testAdaptiveLocalHashAggWithRowLessThanSamplePoint(): Unit = {
    checkQuery(
      Seq((1, 1, 1, 1, 1L, 1.1d), (1, 1, 1, 2, 1L, 1.2d), (1, 2, 2, 3, 2L, 2.2d)),
      """
        | SELECT f0, f1, sum(f2), avg(f2), max(f3), min(f3), count(f3), count(*), sum(f4), sum(f5), avg(f4), avg(f5)
        | FROM TableName GROUP BY f0, f1
        |""".stripMargin,
      Seq((1, 1, 2, 1, 2, 1, 2, 2, 2, 2.3, 1, 1.15), (1, 2, 2, 2, 3, 3, 1, 1, 2, 2.2, 2, 2.2))
    )
  }

  @Test
  def testAdaptiveLocalHashAggWithNullValue(): Unit = {
    tEnv.getConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
    val testDataWithNullValue = tEnv.fromValues(
      DataTypes.ROW(
        DataTypes.FIELD("f0", DataTypes.INT()),
        DataTypes.FIELD("f1", DataTypes.INT()),
        DataTypes.FIELD("f2", DataTypes.INT()),
        DataTypes.FIELD("f3", DataTypes.INT()),
        DataTypes.FIELD("f4", DataTypes.BIGINT()),
        DataTypes.FIELD("f5", DataTypes.DOUBLE())
      ),
      row(1, 1, 1, 1, null, 1.1d),
      row(1, 1, null, null, 1L, null),
      row(1, 1, 2, 3, 2L, 2.2d),
      row(1, 1, 2, 2, null, 1d),
      row(1, 1, 2, 2, 2L, 4d),
      row(1, 1, null, 3, 3L, null),
      row(1, 2, 1, 1, 1L, 1.1d),
      row(1, 2, null, 2, null, null),
      row(1, 3, 1, 1, 1L, null),
      row(1, 4, 1, 1, 1L, 1.1d),
      row(2, 1, null, 2, 2L, 2.2d),
      row(2, 2, 3, null, 3L, 3.3d)
    )

    checkResult(
      s"""
         | SELECT f0, f1, sum(f2), avg(f2), max(f3), min(f3), count(f3), count(*), sum(f4), sum(f5), avg(f4), avg(f5)
         | FROM $testDataWithNullValue GROUP BY f0, f1
         |""".stripMargin,
      Seq(
        row(1, 1, 7, 1, 3, 1, 5, 6, 8, 8.3, 2, 2.075),
        row(1, 2, 1, 1, 2, 1, 2, 2, 1, 1.1, 1, 1.1),
        row(1, 3, 1, 1, 1, 1, 1, 1, 1, null, 1, null),
        row(1, 4, 1, 1, 1, 1, 1, 1, 1, 1.1, 1, 1.1),
        row(2, 1, null, null, 2, 2, 1, 1, 2, 2.2, 2, 2.2),
        row(2, 2, 3, 3, null, null, 0, 1, 3, 3.3, 3, 3.3)
      )
    )

  }

}

object HashAggITCase {
  @Parameterized.Parameters(name = "adaptiveLocalHashAggEnable={0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
