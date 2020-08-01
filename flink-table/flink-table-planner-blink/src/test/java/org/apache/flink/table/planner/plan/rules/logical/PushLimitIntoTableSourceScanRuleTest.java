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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.tools.RuleSets;

/**
 * Test for {@link PushLimitIntoTableSourceScanRule}.
 */
public class PushLimitIntoTableSourceScanRuleTest extends PushLimitIntoLegacyTableSourceScanRuleTest {
	@Override
	public void setup() {
		util().buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
		CalciteConfig calciteConfig = TableConfigUtils.getCalciteConfig(util().tableEnv().getConfig());
		calciteConfig.getBatchProgram().get().addLast(
			"rules",
			FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
				.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
				.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
				.add(RuleSets.ofList(PushLimitIntoTableSourceScanRule.INSTANCE,
					SortProjectTransposeRule.INSTANCE,
					// converts calcite rel(RelNode) to flink rel(FlinkRelNode)
					FlinkLogicalSort.BATCH_CONVERTER(),
					FlinkLogicalTableSourceScan.CONVERTER()))
				.build()
		);

		String ddl =
			"CREATE TABLE LimitTable (\n" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c string\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'true'\n" +
				")";
		util().tableEnv().executeSql(ddl);
	}
}
