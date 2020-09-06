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
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.tools.RuleSets;

/**
 * Test for {@link PushFilterIntoTableSourceScanRule}.
 */
public class PushFilterIntoTableSourceScanRuleTest extends PushFilterIntoLegacyTableSourceScanRuleTest {

	@Override
	public void setup() {
		util().buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
		CalciteConfig calciteConfig = TableConfigUtils.getCalciteConfig(util().tableEnv().getConfig());
		calciteConfig.getBatchProgram().get().addLast(
			"rules",
			FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
				.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
				.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
				.add(RuleSets.ofList(PushFilterIntoTableSourceScanRule.INSTANCE,
					FilterProjectTransposeRule.INSTANCE))
				.build()
		);
		// name: STRING, id: LONG, amount: INT, price: DOUBLE
		String ddl1 =
			"CREATE TABLE MyTable (\n" +
				"  name STRING,\n" +
				"  id bigint,\n" +
				"  amount int,\n" +
				"  price double\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'filterable-fields' = 'amount',\n" +
				" 'bounded' = 'true'\n" +
				")";
		util().tableEnv().executeSql(ddl1);

		String ddl2 =
			"CREATE TABLE VirtualTable (\n" +
				"  name STRING,\n" +
				"  id bigint,\n" +
				"  amount int,\n" +
				"  virtualField as amount + 1,\n" +
				"  price double\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'filterable-fields' = 'amount',\n" +
				" 'bounded' = 'true'\n" +
				")";

		util().tableEnv().executeSql(ddl2);
	}

	@Override
	public void testLowerUpperPushdown() {
		String ddl =
			"CREATE TABLE MTable (\n" +
				"  a STRING,\n" +
				"  b STRING\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'filterable-fields' = 'a;b',\n" +
				" 'bounded' = 'true'\n" +
				")";
		util().tableEnv().executeSql(ddl);
		util().verifyPlan("SELECT * FROM MTable WHERE LOWER(a) = 'foo' AND UPPER(b) = 'bar'");
	}
}
