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

import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link PushPartitionIntoTableSourceScanRule}.
 */
public class PushPartitionIntoTableSourceScanRuleTest extends PushPartitionIntoLegacyTableSourceScanRuleTest{
	public PushPartitionIntoTableSourceScanRuleTest(boolean sourceFetchPartitions, boolean useFilter) {
		super(sourceFetchPartitions, useFilter);
	}

	@Override
	public void setup() throws Exception {
		util().buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
		CalciteConfig calciteConfig = TableConfigUtils.getCalciteConfig(util().tableEnv().getConfig());
		calciteConfig.getBatchProgram().get().addLast(
			"rules",
			FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
				.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
				.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
				.add(RuleSets.ofList(FilterProjectTransposeRule.INSTANCE,
					PushPartitionIntoTableSourceScanRule.INSTANCE))
				.build());

		// define ddl
		String ddlTemp =
			"CREATE TABLE MyTable (\n" +
				"  id int,\n" +
				"  name string,\n" +
				"  part1 string,\n" +
				"  part2 int)\n" +
				"  partitioned by (part1, part2)\n" +
				"  WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'true',\n" +
				" 'partition-list' = '%s'" +
				")";

		String ddlTempWithVirtualColumn =
			"CREATE TABLE VirtualTable (\n" +
				"  id int,\n" +
				"  name string,\n" +
				"  part1 string,\n" +
				"  part2 int,\n" +
				"  virtualField AS part2 + 1)\n" +
				"  partitioned by (part1, part2)\n" +
				"  WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'true',\n" +
				" 'partition-list' = '%s'" +
				")";

		if (sourceFetchPartitions()) {
			String partitionString = "part1:A,part2:1;part1:A,part2:2;part1:B,part2:3;part1:C,part2:1";
			util().tableEnv().executeSql(String.format(ddlTemp, partitionString));
			util().tableEnv().executeSql(String.format(ddlTempWithVirtualColumn, partitionString));
		} else {
			TestValuesCatalog catalog =
				new TestValuesCatalog("test_catalog", "test_database", useCatalogFilter());
			util().tableEnv().registerCatalog("test_catalog", catalog);
			util().tableEnv().useCatalog("test_catalog");
			// register table without partitions
			util().tableEnv().executeSql(String.format(ddlTemp, ""));
			util().tableEnv().executeSql(String.format(ddlTempWithVirtualColumn, ""));
			ObjectPath mytablePath = ObjectPath.fromString("test_database.MyTable");
			ObjectPath virtualTablePath = ObjectPath.fromString("test_database.VirtualTable");
			// partition map
			List<Map<String, String>> partitions = Arrays.asList(
				new HashMap<String, String>(){{
					put("part1", "A");
					put("part2", "1");
				}},
				new HashMap<String, String>(){{
					put("part1", "A");
					put("part2", "2");
				}},
				new HashMap<String, String>(){{
					put("part1", "B");
					put("part2", "3");
				}},
				new HashMap<String, String>(){{
					put("part1", "C");
					put("part2", "1");
				}}
			);
			for (Map<String, String> partition : partitions) {
				CatalogPartitionSpec catalogPartitionSpec = new CatalogPartitionSpec(partition);
				CatalogPartition catalogPartition = new CatalogPartitionImpl(new HashMap<>(), "");
				catalog.createPartition(mytablePath, catalogPartitionSpec, catalogPartition, true);
				catalog.createPartition(virtualTablePath, catalogPartitionSpec, catalogPartition, true);
			}
		}
	}

	@Test
	public void testUnconvertedExpression() {
		String sql =
			"select * from MyTable where trim(part1) = 'A' and part2 > 1";
		util().verifyPlan(sql);
	}
}
