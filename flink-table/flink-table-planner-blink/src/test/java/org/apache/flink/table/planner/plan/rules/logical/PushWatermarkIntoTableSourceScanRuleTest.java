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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/**
 * Test rule {@link PushWatermarkIntoTableSourceScanAcrossCalcRule} and {@link PushWatermarkIntoTableSourceScanRule}.
 * */
public class PushWatermarkIntoTableSourceScanRuleTest extends TableTestBase {
	private StreamTableTestUtil util = streamTestUtil(new TableConfig());

	@Before
	public void setup() {
		FlinkChainedProgram<StreamOptimizeContext> program = new FlinkChainedProgram<>();
		program.addLast(
				"Converters",
				FlinkVolcanoProgramBuilder.<StreamOptimizeContext>newBuilder()
						.add(RuleSets.ofList(
								CoreRules.PROJECT_TO_CALC,
								CoreRules.FILTER_TO_CALC,
								FlinkLogicalCalc.CONVERTER(),
								FlinkLogicalTableSourceScan.CONVERTER(),
								FlinkLogicalWatermarkAssigner.CONVERTER()
								))
						.setRequiredOutputTraits(new Convention[] {FlinkConventions.LOGICAL()})
						.build()
		);
		program.addLast(
				"PushWatermarkIntoTableSourceScanRule",
				FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(RuleSets.ofList(
								PushWatermarkIntoTableSourceScanRule.INSTANCE,
								PushWatermarkIntoTableSourceScanAcrossCalcRule.INSTANCE))
						.build()
		);
		util.replaceStreamProgram(program);
	}

	@Test
	public void testSimpleWatermark() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c TIMESTAMP(3),\n" +
						"  WATERMARK FOR c AS c - INTERVAL '5' SECOND\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("select a, c from MyTable");
	}

	@Test
	public void testWatermarkOnComputedColumn() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c TIMESTAMP(3),\n" +
						"  d AS c + INTERVAL '5' SECOND,\n" +
						"  WATERMARK FOR d AS d - INTERVAL '5' SECOND\n" +
						") WITH (\n" +
						" 'connector' = 'values',\n" +
						" 'enable-watermark-push-down' = 'true',\n" +
						" 'bounded' = 'false',\n" +
						" 'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT * from MyTable");
	}

	@Test
	public void testWatermarkOnComputedColumnWithQuery() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c TIMESTAMP(3) NOT NULL,\n" +
						"  d AS c + INTERVAL '5' SECOND,\n" +
						"  WATERMARK FOR d AS d - INTERVAL '5' SECOND\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT a, b FROM MyTable WHERE d > TO_TIMESTAMP('2020-10-09 12:12:12')");
	}

	@Test
	public void testWatermarkOnComputedColumnWithMultipleInputs() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  a STRING,\n" +
						"  b STRING,\n" +
						"  c as TO_TIMESTAMP(a, b),\n" +
						"  WATERMARK FOR c AS c - INTERVAL '5' SECOND\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT * FROM MyTable");
	}

	@Test
	public void testWatermarkOnRow() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c ROW<name STRING, d TIMESTAMP(3)>," +
						"  e AS c.d," +
						"  WATERMARK FOR e AS e - INTERVAL '5' SECOND\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT * FROM MyTable");
	}

	@Test
	public void testWatermarkOnNestedRow() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c ROW<name STRING, d row<e STRING, f TIMESTAMP(3)>>," +
						"  g as c.d.f," +
						"  WATERMARK for g as g - INTERVAL '5' SECOND\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT * FROM MyTable");
	}

	@Test
	public void testWatermarkWithMultiInputUdf() {
		JavaFunc5.closeCalled = false;
		JavaFunc5.openCalled = false;
		util.addFunction("func", new JavaFunc5());

		String ddl =
				"CREATE TABLE MyTable(\n" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c TIMESTAMP(3),\n" +
						"  d AS func(c, a),\n" +
						"  WATERMARK FOR d AS func(func(d, a), a)\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT * FROM MyTable");
	}

	@Test
	public void testWatermarkOnMetadata() {
		String ddl =
				"CREATE TABLE MyTable(" +
						"  `a` INT,\n" +
						"  `b` BIGINT,\n" +
						"  `c` TIMESTAMP(3),\n" +
						"  `metadata` BIGINT METADATA FROM 'metadata_2' VIRTUAL,\n" +
						"  `computed` AS `metadata` + `b`,\n" +
						"  WATERMARK for `c` as c - CAST(`metadata` + `computed` AS INTERVAL SECOND)\n" +
						") WITH (\n" +
						"  'connector' = 'values',\n" +
						"  'readable-metadata' = 'metadata_1:STRING,metadata_2:INT',\n" +
						"  'enable-watermark-push-down' = 'true',\n" +
						"  'bounded' = 'false',\n" +
						"  'disable-lookup' = 'true'" +
						")";
		util.tableEnv().executeSql(ddl);
		util.verifyRelPlan("SELECT * FROM MyTable");
	}
}
