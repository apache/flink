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
package org.apache.flink.table.plan.batch.sql.agg

import org.apache.flink.table.api.AggPhaseEnforcer.AggPhaseEnforcer
import org.apache.flink.table.api.{AggPhaseEnforcer, OperatorType, PlannerConfigOptions, TableConfigOptions, TableException, ValidationException}

import org.junit.Before
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class HashAggregateTest(aggStrategy: AggPhaseEnforcer) extends AggregateTestBase {

  @Before
  def before(): Unit = {
    // disable sort agg
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, OperatorType.SortAgg.toString)
    util.tableEnv.getConfig.getConf.setString(
      PlannerConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, aggStrategy.toString)
  }

  override def testMinWithVariableLengthType(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMinWithVariableLengthType()
  }

  override def testMaxWithVariableLengthType(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMaxWithVariableLengthType()
  }

  override def testPojoAccumulator(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testPojoAccumulator()
  }
}

object HashAggregateTest {

  @Parameterized.Parameters(name = "aggStrategy={0}")
  def parameters(): util.Collection[AggPhaseEnforcer] = {
    Seq[AggPhaseEnforcer](
      AggPhaseEnforcer.NONE,
      AggPhaseEnforcer.ONE_PHASE,
      AggPhaseEnforcer.TWO_PHASE
    )
  }
}
