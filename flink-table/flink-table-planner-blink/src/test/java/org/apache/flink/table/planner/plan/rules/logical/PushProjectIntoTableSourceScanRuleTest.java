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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;

/**
 * Test for {@link PushProjectIntoTableSourceScanRule}.
 */
public class PushProjectIntoTableSourceScanRuleTest extends PushProjectIntoLegacyTableSourceScanRuleTest {

	@Override
	public void setup() {
		util().buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
		CalciteConfig calciteConfig = TableConfigUtils.getCalciteConfig(util().tableEnv().getConfig());
		calciteConfig.getBatchProgram().get().addLast(
				"rules",
				FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(RuleSets.ofList(PushProjectIntoTableSourceScanRule.INSTANCE))
						.build()
		);

		String ddl1 =
				"CREATE TABLE MyTable (\n" +
						"  a int,\n" +
						"  b bigint,\n" +
						"  c string\n" +
						") WITH (\n" +
						" 'connector' = 'values',\n" +
						" 'bounded' = 'true'\n" +
						")";
		util().tableEnv().executeSql(ddl1);

		String ddl2 =
				"CREATE TABLE VirtualTable (\n" +
						"  a int,\n" +
						"  b bigint,\n" +
						"  c string,\n" +
						"  d as a + 1\n" +
						") WITH (\n" +
						" 'connector' = 'values',\n" +
						" 'bounded' = 'true'\n" +
						")";
		util().tableEnv().executeSql(ddl2);
	}

	@Override
	public void testNestedProject() {
		expectedException().expect(TableException.class);
		expectedException().expectMessage("Nested projection push down is unsupported now.");
		testNestedProject(true);
	}

	@Test
	public void testNestedProjectDisabled() {
		testNestedProject(false);
	}

	private void testNestedProject(boolean nestedProjectionSupported) {
		String ddl =
				"CREATE TABLE NestedTable (\n" +
						"  id int,\n" +
						"  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n" +
						"  nested row<name string, `value` int>,\n" +
						"  name string\n" +
						") WITH (\n" +
						" 'connector' = 'values',\n" +
						" 'nested-projection-supported' = '" + nestedProjectionSupported + "',\n" +
						"  'bounded' = 'true'\n" +
						")";
		util().tableEnv().executeSql(ddl);

		String sqlQuery = "SELECT id,\n" +
				"    deepNested.nested1.name AS nestedName,\n" +
				"    nested.`value` AS nestedValue,\n" +
				"    deepNested.nested2.flag AS nestedFlag,\n" +
				"    deepNested.nested2.num AS nestedNum\n" +
				"FROM NestedTable";
		util().verifyPlan(sqlQuery);
	}

}
