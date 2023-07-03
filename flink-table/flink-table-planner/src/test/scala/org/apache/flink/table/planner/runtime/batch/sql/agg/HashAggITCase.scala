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
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData.{data1, nullablesOfData1, type1}
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith

import java.math.BigDecimal

import scala.collection.JavaConverters._

/** AggregateITCase using HashAgg Operator. */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class HashAggITCase extends AggregateITCaseBase("HashAggregate") {

  @Parameter var adaptiveLocalHashAggEnable: Boolean = _

  override def prepareAggOp(): Unit = {
    registerCollection(
      "AuxGroupingTable",
      data1,
      type1,
      "a, b, c",
      nullablesOfData1,
      FlinkStatistic.builder().uniqueKeys(Set(Set("a").asJava).asJava).build())

    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    if (adaptiveLocalHashAggEnable) {
      tEnv.getConfig
        .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
      tEnv.getConfig.set(
        HashAggCodeGenerator.TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_ENABLED,
        Boolean.box(true))
      tEnv.getConfig.set(
        HashAggCodeGenerator.TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD,
        Long.box(3L))
    }
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testAdaptiveLocalHashAggWithRowLessThanSamplingThreshold(): Unit = {
    checkQuery(
      Seq((1, 1, 1, 1, 1L, 1.1d), (1, 1, 1, 2, 1L, 1.2d), (1, 2, 2, 3, 2L, 2.2d)),
      """
        | SELECT f0, f1, sum(f2), avg(f2), max(f3), min(f3), count(f3), count(*), sum(f4), sum(f5), avg(f4), avg(f5)
        | FROM TableName GROUP BY f0, f1
        |""".stripMargin,
      Seq((1, 1, 2, 1, 2, 1, 2, 2, 2, 2.3, 1, 1.15), (1, 2, 2, 2, 3, 3, 1, 1, 2, 2.2, 2, 2.2))
    )
  }

  @TestTemplate
  def testAdaptiveLocalHashAggWithNullValue(): Unit = {
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

  @TestTemplate
  def testAdaptiveHashAggWithSumAndAvgFunctionForNumericalType(): Unit = {
    val testDataWithAllTypes = tEnv.fromValues(
      DataTypes.ROW(
        DataTypes.FIELD("f0", DataTypes.INT()),
        DataTypes.FIELD("f1", DataTypes.TINYINT()),
        DataTypes.FIELD("f2", DataTypes.SMALLINT()),
        DataTypes.FIELD("f3", DataTypes.BIGINT()),
        DataTypes.FIELD("f4", DataTypes.FLOAT()),
        DataTypes.FIELD("f5", DataTypes.DOUBLE()),
        DataTypes.FIELD("f6", DataTypes.DECIMAL(5, 2)),
        DataTypes.FIELD("f7", DataTypes.DECIMAL(14, 3)),
        DataTypes.FIELD("f8", DataTypes.DECIMAL(38, 18))
      ),
      row(
        1,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      ),
      row(
        1,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      ),
      row(
        1,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      ),
      row(
        1,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      ),
      row(
        2,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      ),
      row(
        2,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      ),
      row(
        3,
        1,
        1,
        1000L,
        -1.1f,
        1.1d,
        new BigDecimal("111.11"),
        new BigDecimal("11111111111.111"),
        new BigDecimal("11111111111111111111.111111111111111111")
      )
    )

    checkResult(
      s"""
         | SELECT f0, sum(f1), avg(f1), sum(f2), avg(f2), sum(f3), avg(f3),
         | sum(f4), avg(f4), sum(f5), avg(f5), sum(f6), avg(f6),
         | sum(f7), avg(f7), sum(f8), avg(f8)
         | FROM $testDataWithAllTypes GROUP BY f0
         |""".stripMargin,
      Seq(
        row(
          1,
          4.toByte,
          1.toByte,
          4.toShort,
          1.toShort,
          4000L,
          1000L,
          -4.4f,
          -1.1f,
          4.4d,
          1.1d,
          new BigDecimal("444.44"),
          new BigDecimal("111.110000"),
          new BigDecimal("44444444444.444"),
          new BigDecimal("11111111111.111000"),
          new BigDecimal("44444444444444444444.444444444444444444"),
          new BigDecimal("11111111111111111111.111111111111111111")
        ),
        row(
          2,
          2.toByte,
          1.toByte,
          2.toShort,
          1.toShort,
          2000L,
          1000L,
          -2.2f,
          -1.1f,
          2.2d,
          1.1d,
          new BigDecimal("222.22"),
          new BigDecimal("111.110000"),
          new BigDecimal("22222222222.222"),
          new BigDecimal("11111111111.111000"),
          new BigDecimal("22222222222222222222.222222222222222222"),
          new BigDecimal("11111111111111111111.111111111111111111")
        ),
        row(
          3,
          1.toByte,
          1.toByte,
          1.toShort,
          1.toShort,
          1000L,
          1000L,
          -1.1f,
          -1.1f,
          1.1d,
          1.1d,
          new BigDecimal("111.11"),
          new BigDecimal("111.110000"),
          new BigDecimal("11111111111.111"),
          new BigDecimal("11111111111.111000"),
          new BigDecimal("11111111111111111111.111111111111111111"),
          new BigDecimal("11111111111111111111.111111111111111111")
        )
      )
    )
  }

  @TestTemplate
  def testAdaptiveHashAggWithAuxGrouping(): Unit = {
    checkResult(
      "SELECT a, b, COUNT(c) FROM AuxGroupingTable GROUP BY a, b",
      Seq(
        row(1, "a", 1),
        row(2, "a", 1),
        row(3, "b", 1),
        row(4, "b", 1),
        row(5, "c", 1),
        row(6, "c", 1)
      )
    )
  }
}

object HashAggITCase {
  @Parameters(name = "adaptiveLocalHashAggEnable={0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
