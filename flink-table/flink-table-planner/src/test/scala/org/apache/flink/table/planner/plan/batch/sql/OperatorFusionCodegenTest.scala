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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.TableTestBase

import com.google.common.collect.ImmutableSet
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

/** Tests for operator fusion codegen. */
@RunWith(classOf[Parameterized])
class OperatorFusionCodegenTest(fusionCodegenEnabled: Boolean) extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource(
      "T1",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING, Types.STRING),
      Array("a1", "b1", "c1", "d1"),
      FlinkStatistic
        .builder()
        .tableStats(new TableStats(100000000))
        .uniqueKeys(ImmutableSet.of(ImmutableSet.of("a1")))
        .build()
    )
    util.addTableSource(
      "T2",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING),
      Array("a2", "b2", "c2"),
      FlinkStatistic
        .builder()
        .tableStats(new TableStats(100000000))
        .uniqueKeys(ImmutableSet.of(ImmutableSet.of("b2"), ImmutableSet.of("a2", "b2")))
        .build()
    )
    util.addTableSource(
      "T3",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING, Types.LONG),
      Array("a3", "b3", "c3", "d3"),
      FlinkStatistic
        .builder()
        .tableStats(new TableStats(1000))
        .build()
    )

    util.tableEnv.getConfig
      .set(
        ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED,
        Boolean.box(fusionCodegenEnabled))
  }

  @Test
  def testHashAggAsMutltipleInputRoot(): Unit = {
    util.verifyExecPlan(
      "SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
        "(SELECT * FROM T1, T2 WHERE a1 = b2) t GROUP BY a1, b1, a2, b2")
  }

  @Test
  def testLocalHashAggAsMutltipleInputRoot(): Unit = {
    util.verifyExecPlan(
      "SELECT a2, b2, a3, b3, COUNT(c2), AVG(d3) FROM " +
        "(SELECT * FROM T2, T3 WHERE b2 = a3) t GROUP BY a2, b2, a3, b3")
  }

  @Test
  def testCalcAsMutltipleInputRoot(): Unit = {
    util.verifyExecPlan(
      "SELECT a1, b1, a2, b2, a3, b3, COUNT(c1) FROM " +
        "(SELECT * FROM T1, T2, T3 WHERE a1 = b2 AND a1 = a3) t GROUP BY a1, b1, a2, b2, a3, b3")
  }
}

object OperatorFusionCodegenTest {
  @Parameterized.Parameters(name = "fusionCodegenEnabled={0}")
  def parameters(): util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
