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
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link ProjectWatermarkAssignerTransposeRule}.
 */
public class ProjectWatermarkAssignerTransposeRuleTest extends TableTestBase {
	private final StreamTableTestUtil util = streamTestUtil(new TableConfig());

	@Before
	public void setup() {
		FlinkChainedProgram<StreamOptimizeContext> program = new FlinkChainedProgram<>();

		program.addLast(
				"ProjectWatermarkAssignerTransposeRule",
				FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(RuleSets.ofList(ProjectWatermarkAssignerTransposeRule.INSTANCE))
						.build()
		);
		util.replaceStreamProgram(program);

		String ddl1 =
				"CREATE TABLE SimpleTable(\n" +
						"  a INT,\n" +
						"  b BIGINT,\n" +
						"  c TIMESTAMP(3)," +
						"  d ROW<d1 INT, d2 INT, d3 INT>,\n" +
						"WATERMARK FOR c AS c" +
						") WITH (" +
						"  'connector' = 'values',\n" +
						"  'bounded' = 'false'\n" +
						")";
		util.tableEnv().executeSql(ddl1);

		String ddl2 =
				"CREATE TABLE VirtualTable(" +
						"  a int,\n" +
						"  b BIGINT,\n" +
						"  c ROW<c1 TIMESTAMP(3)>,\n" +
						"  d AS c.c1 + INTERVAL '5' SECOND,\n" +
						"WATERMARK FOR d as d - INTERVAL '5' second" +
						") WITH (" +
						"  'connector' = 'values',\n" +
						"  'bounded' = 'false'\n" +
						")";
		util.tableEnv().executeSql(ddl2);

		JavaFunc5.openCalled = false;
		JavaFunc5.closeCalled = false;
		util.addFunction("myFunc", new JavaFunc5());
		String ddl3 =
				"CREATE TABLE UdfTable(" +
						"  a INT,\n" +
						"  b TIMESTAMP(3),\n" +
						"  c INT,\n" +
						"  d STRING,\n" +
						"  WATERMARK FOR b AS myFunc(b, c)" +
						") WITH (" +
						"  'connector' = 'values',\n" +
						"  'bounded' = 'false'\n" +
						")";
		util.tableEnv().executeSql(ddl3);

		String ddl4 =
				"CREATE TABLE NestedTable(\n" +
						"  a int,\n" +
						"  b BIGINT,\n" +
						"  c ROW<c1 TIMESTAMP(3), c2 INT>,\n" +
						"  d AS c.c1 + INTERVAL '5' SECOND,\n" +
						"WATERMARK FOR d as myFunc(d, c.c2)\n" +
						") WITH (" +
						"  'connector' = 'values',\n" +
						"  'bounded' = 'false'\n" +
						")";
		util.tableEnv().executeSql(ddl4);
	}

	@Test
	public void simpleTranspose() {
		util.verifyRelPlan("SELECT a, c FROM SimpleTable");
	}

	@Test
	public void transposeWithReorder() {
		util.verifyRelPlan("SELECT b, a FROM SimpleTable");
	}

	@Test
	public void transposeWithNestedField() {
		util.verifyRelPlan("SELECT b, d.d1, d.d2 FROM SimpleTable");
	}

	@Test
	public void complicatedTranspose() {
		util.verifyRelPlan("SELECT d.d1, d.d2 + b FROM SimpleTable");
	}

	@Test
	public void transposeExcludeRowTime() {
		util.verifyRelPlan("SELECT SECOND(c) FROM SimpleTable");
	}

	@Test
	public void transposeWithIncludeComputedRowTime() {
		util.verifyRelPlan("SELECT a, b, d FROM VirtualTable");
	}

	@Test
	public void transposeWithExcludeComputedRowTime() {
		util.verifyRelPlan("SELECT a, b FROM VirtualTable");
	}

	@Test
	public void transposeWithExcludeComputedRowTime2() {
		util.verifyRelPlan("SELECT a, b, SECOND(d) FROM VirtualTable");
	}

	@Test
	public void transposeWithExcludeComputedRowTime3() {
		util.verifyRelPlan("SELECT a, SECOND(d) FROM NestedTable");
	}

	@Test
	public void transposeWithDuplicateColumns() {
		util.verifyRelPlan("SELECT a, b, b as e FROM VirtualTable");
	}

	@Test
	public void transposeWithWatermarkWithMultipleInput() {
		util.verifyRelPlan("SELECT a FROM UdfTable");
	}
}
