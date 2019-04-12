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

package org.apache.flink.table.plan.stream.sql.agg

import org.apache.flink.table.api.AggPhaseEnforcer.AggPhaseEnforcer
import org.apache.flink.table.api.{AggPhaseEnforcer, PlannerConfigOptions}

import org.junit.Before
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

@RunWith(classOf[Parameterized])
class IncrementalAggregateTest(
    splitDistinctAggEnabled: Boolean,
    aggPhaseEnforcer: AggPhaseEnforcer)
  extends DistinctAggregateTest(splitDistinctAggEnabled, aggPhaseEnforcer) {

  @Before
  override def before(): Unit = {
    super.before()
    // enable incremental agg
    util.tableEnv.getConfig.getConf.setBoolean(
      PlannerConfigOptions.SQL_OPTIMIZER_INCREMENTAL_AGG_ENABLED, true)
  }

}

object IncrementalAggregateTest {
  @Parameterized.Parameters(name = "splitDistinctAggEnabled={0}, aggPhaseEnforcer={1}")
  def parameters(): util.Collection[Array[Any]] = {
    util.Arrays.asList(
      Array(true, AggPhaseEnforcer.TWO_PHASE)
    )
  }
}
