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
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator

import org.junit.Test

/** AggregateITCase using HashAgg Operator. */
class HashAggITCase extends AggregateITCaseBase("HashAggregate") {

  override def prepareAggOp(): Unit = {
    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
  }

  @Test
  def testAdaptiveHashAggWithHighAggregationDegree(): Unit = {
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_ENABLED,
      Boolean.box(true))
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_SAMPLE_POINT,
      Long.box(5L))

    checkQuery(
      Seq(
        (1, 1, 1, 1),
        (1, 1, 1, 2),
        (1, 1, 2, 3),
        (1, 1, 2, 2),
        (1, 1, 3, 3),
        (1, 2, 1, 1),
        (1, 2, 1, 2),
        (1, 3, 1, 1),
        (1, 4, 1, 1),
        (2, 1, 2, 2),
        (2, 2, 3, 3)),
      "SELECT f0, f1, sum(f2), max(f3), count(f3), count(*) FROM TableName GROUP BY f0, f1",
      Seq(
        (1, 1, 9, 3, 5, 5),
        (1, 2, 2, 2, 2, 2),
        (1, 3, 1, 1, 1, 1),
        (1, 4, 1, 1, 1, 1),
        (2, 1, 2, 2, 1, 1),
        (2, 2, 3, 3, 1, 1))
    )
  }

  @Test
  def testAdaptiveHashAggWithLowAggregationDegree(): Unit = {
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_ENABLED,
      Boolean.box(true))
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_SAMPLE_POINT,
      Long.box(5L))

    checkQuery(
      Seq(
        (1, 1, 1, 1),
        (1, 1, 1, 2),
        (1, 2, 2, 3),
        (1, 3, 2, 2),
        (1, 4, 3, 3),
        (1, 5, 1, 1),
        (2, 1, 1, 2),
        (2, 2, 1, 1),
        (2, 3, 1, 1),
        (2, 3, 2, 2),
        (2, 3, 3, 3)),
      "SELECT f0, f1, sum(f2), max(f3) FROM TableName GROUP BY f0, f1",
      Seq(
        (1, 1, 2, 2),
        (1, 2, 2, 3),
        (1, 3, 2, 2),
        (1, 4, 3, 3),
        (1, 5, 1, 1),
        (2, 1, 1, 2),
        (2, 2, 1, 1),
        (2, 3, 6, 3))
    )
  }

  @Test
  def testAdaptiveHashAggWithRowLessThanSamplePoint(): Unit = {
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_ENABLED,
      Boolean.box(true))
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_SAMPLE_POINT,
      Long.box(5L))

    checkQuery(
      Seq((1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 2, 3)),
      "SELECT f0, f1, sum(f2), max(f3) FROM TableName GROUP BY f0, f1",
      Seq((1, 1, 2, 2), (1, 2, 2, 3))
    )
  }

  @Test
  def testAdaptiveHashAggWithDifferentSumColType(): Unit = {
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_ENABLED,
      Boolean.box(true))
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_ADAPTIVE_LOCAL_HASH_AGG_SAMPLE_POINT,
      Long.box(2L))

    checkQuery(
      Seq(
        (1, 1, 1L, 1, 1.1d),
        (1, 1, 1L, 2, 1.2d),
        (1, 1, 2L, 3, 2.2d),
        (1, 1, 2L, 2, 3.1d),
        (1, 1, 3L, 3, 3d),
        (1, 2, 1L, 1, 1.1d),
        (1, 2, 1L, 2, 2.3d),
        (1, 3, 1L, 1, 3.3d),
        (1, 4, 1L, 1, 1.1d),
        (2, 1, 2L, 2, 2.2d),
        (2, 1, 3L, 3, 3.3d)
      ),
      "SELECT f0, f1, sum(f2), sum(f3), sum(f4) FROM TableName GROUP BY f0, f1",
      Seq(
        (1, 1, 9, 11, 10.6),
        (1, 2, 2, 3, 3.4),
        (1, 3, 1, 1, 3.3),
        (1, 4, 1, 1, 1.1),
        (2, 1, 5, 5, 5.5))
    )
  }

}
