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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.functions.FunctionTestBase;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for catalog and system in stream table environment.
 */
public class FunctionITCase extends FunctionTestBase {

	@BeforeClass
	public static void setup() {
		EnvironmentSettings environmentSettings =
			EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		tableEnv = TestingTableEnvironment.create(environmentSettings);
	}

	@Test
	public void testUseDefinedRegularCatalogFunction() throws Exception {
		String functionDDL = "create function addOne as " +
			"'org.apache.flink.table.planner.functions.FunctionTestBase$TestUDF'";

		String dropFunctionDDL = "drop function addOne";
		testUseDefinedCatalogFunction(functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUseDefinedTemporaryCatalogFunction() throws Exception {
		String functionDDL = "create temporary function addOne as " +
			"'org.apache.flink.table.planner.functions.FunctionTestBase$TestUDF'";

		String dropFunctionDDL = "drop temporary function addOne";
		testUseDefinedCatalogFunction(functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	@Test
	public void testUseDefinedTemporarySystemFunction() throws Exception {
		String functionDDL = "create temporary system function addOne as " +
			"'org.apache.flink.table.planner.functions.FunctionTestBase$TestUDF'";

		String dropFunctionDDL = "drop temporary system function addOne";
		testUseDefinedCatalogFunction(functionDDL);
		// delete the function
		tableEnv.sqlUpdate(dropFunctionDDL);
	}

	// This test case only works for stream mode
	private void testUseDefinedCatalogFunction(String createFunctionDDL) throws Exception {
		List<Row> sourceData = Arrays.asList(
			toRow(1, "1000", 2),
			toRow(2, "1", 3),
			toRow(3, "2000", 4),
			toRow(1, "2", 2),
			toRow(2, "3000", 3)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData, new ArrayList<Row>(), -1);

		String sourceDDL = "create table t1(a int, b varchar, c int) with ('connector' = 'COLLECTION')";
		String sinkDDL = "create table t2(a int, b varchar, c int) with ('connector' = 'COLLECTION')";

		String query = "select t1.a, t1.b, addOne(t1.a, 1) as c from t1";

		tableEnv.sqlUpdate(sourceDDL);
		tableEnv.sqlUpdate(sinkDDL);
		tableEnv.sqlUpdate(createFunctionDDL);
		Table t2 = tableEnv.sqlQuery(query);
		tableEnv.insertInto("t2", t2);
		tableEnv.execute("job1");

		Row[] result = TestCollectionTableFactory.RESULT().toArray(new Row[0]);
		Row[] expected = sourceData.toArray(new Row[0]);
		assertArrayEquals(expected, result);

		tableEnv.sqlUpdate("drop table t1");
		tableEnv.sqlUpdate("drop table t2");
	}
}
