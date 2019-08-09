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
import org.apache.flink.table.planner.plan.utils.OperatorType
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.table.planner.utils.{CountAggFunction, IntSumAggFunction}

import org.junit.Test

import scala.collection.Seq

/**
  * DistinctAggregateITCase using SortAgg Operator.
  */
class SortDistinctAggregateITCase extends DistinctAggregateITCaseBase {

  override def prepareAggOp(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,  OperatorType.HashAgg.toString)

    registerFunction("countFun", new CountAggFunction())
    registerFunction("intSumFun", new IntSumAggFunction())
    registerFunction("weightedAvg", new WeightedAvgWithMergeAndReset())
  }

  @Test
  def testDistinctUDAGGWithoutGroupBy(): Unit = {
    checkResult(
      "SELECT countFun(DISTINCT c), SUM(a) FROM NullTable3",
      Seq(row(18, 231))
    )

    checkResult(
      "SELECT countFun(DISTINCT c), SUM(a) FROM EmptyTable3",
      Seq(row(0, null))
    )

    checkResult(
      "SELECT countFun(DISTINCT b), intSumFun(DISTINCT a), countFun(c) FROM NullTable3",
      Seq(row(6, 231, 18))
    )
  }

  @Test
  def testDistinctUDAGGWithGroupBy(): Unit = {
    checkResult(
      "SELECT b, countFun(b), intSumFun(DISTINCT a), countFun(DISTINCT c) " +
        "FROM SmallTable3 GROUP BY b",
      Seq(row(1, 1, 1, 1), row(2, 2, 5, 2))
    )
  }

  @Test
  def testUDAGGNullGroupKeyAggregation(): Unit = {
    checkResult(
      "SELECT c, countFun(b), intSumFun(DISTINCT a) FROM NullTable3 WHERE a < 6 GROUP BY c",
      Seq(row(null, 3, 9), row("Hi", 1, 1), row("I am fine.", 1, 5))
    )
  }

  @Test
  def testComplexUDAGGWithGroupBy(): Unit = {
    checkResult(
      "SELECT e, countFun(d), weightedAvg(DISTINCT c, a) FROM Table5 GROUP BY e",
      Seq(row(1, 5, 7), row(2, 7, 8), row(3, 3, 10))
    )
  }

}
