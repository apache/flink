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
package org.apache.flink.table.planner.plan.batch.sql.agg

import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.plan.utils.OperatorType
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy

import org.junit.Before
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class SortAggregateTest(aggStrategy: AggregatePhaseStrategy) extends AggregateTestBase {

  @Before
  def before(): Unit = {
    // disable hash agg
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, OperatorType.HashAgg.toString)
    util.tableEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, aggStrategy.toString)
  }
}

object SortAggregateTest {

  @Parameterized.Parameters(name = "aggStrategy={0}")
  def parameters(): util.Collection[AggregatePhaseStrategy] = {
    Seq[AggregatePhaseStrategy](
      AggregatePhaseStrategy.AUTO,
      AggregatePhaseStrategy.ONE_PHASE,
      AggregatePhaseStrategy.TWO_PHASE
    )
  }
}
