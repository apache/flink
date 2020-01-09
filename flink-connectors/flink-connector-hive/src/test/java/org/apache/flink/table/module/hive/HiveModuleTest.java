/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.module.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.HiveVersionTestUtil.HIVE_120_OR_LATER;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_2_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_0_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_1_1;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_2_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_3_4;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V3_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Test for {@link HiveModule}.
 */
public class HiveModuleTest {
	@BeforeClass
	public static void init() {
		assumeTrue(HIVE_120_OR_LATER);
	}

	@Test
	public void testNumberOfBuiltinFunctions() {
		String hiveVersion = HiveShimLoader.getHiveVersion();

		if (hiveVersion.equals(HIVE_VERSION_V1_2_0)) {
			assertEquals(232, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());
		} else if (hiveVersion.equals(HIVE_VERSION_V2_0_0)) {
			assertEquals(243, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());
		} else if (hiveVersion.equals(HIVE_VERSION_V2_1_1) || hiveVersion.equals(HIVE_VERSION_V2_2_0)) {
			assertEquals(253, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());
		} else if (hiveVersion.equals(HIVE_VERSION_V2_3_4)) {
			assertEquals(287, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());
		} else if (hiveVersion.equals(HIVE_VERSION_V3_1_1)) {
			assertEquals(306, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());
		}
	}

	@Test
	public void testHiveBuiltInFunction() {
		FunctionDefinition fd = new HiveModule(HiveShimLoader.getHiveVersion()).getFunctionDefinition("reverse").get();

		ScalarFunction func = ((ScalarFunctionDefinition) fd).getScalarFunction();
		HiveSimpleUDF udf = (HiveSimpleUDF) func;

		DataType[] inputType = new DataType[] {
			DataTypes.STRING()
		};

		udf.setArgumentTypesAndConstants(new Object[0], inputType);
		udf.getHiveResultType(new Object[0], inputType);

		udf.open(null);

		assertEquals("cba", udf.eval("abc"));
	}

	@Test
	public void testNonExistFunction() {
		assertFalse(new HiveModule(HiveShimLoader.getHiveVersion()).getFunctionDefinition("nonexist").isPresent());
	}

	@Test
	public void testConstantArguments() throws Exception {
		TableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

		tEnv.unloadModule("core");
		tEnv.loadModule("hive", new HiveModule(HiveShimLoader.getHiveVersion()));

		List<Row> results = TableUtils.collectToList(tEnv.sqlQuery("select concat('an', 'bn')"));
		assertEquals("[anbn]", results.toString());

		results = TableUtils.collectToList(tEnv.sqlQuery("select concat('ab', cast('cdefghi' as varchar(5)))"));
		assertEquals("[abcdefg]", results.toString());

		results = TableUtils.collectToList(tEnv.sqlQuery("select concat('ab',cast(12.34 as decimal(10,5)))"));
		assertEquals("[ab12.34]", results.toString());

		results = TableUtils.collectToList(tEnv.sqlQuery("select concat(cast('2018-01-19' as date),cast('2019-12-27 17:58:23.385' as timestamp))"));
		assertEquals("[2018-01-192019-12-27 17:58:23.385]", results.toString());

		// TODO: null cannot be a constant argument at the moment. This test will make more sense when that changes.
		results = TableUtils.collectToList(tEnv.sqlQuery("select concat('ab',cast(null as int))"));
		assertEquals("[null]", results.toString());
	}

	@Test
	public void testDecimalReturnType() throws Exception {
		TableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

		tEnv.unloadModule("core");
		tEnv.loadModule("hive", new HiveModule(HiveShimLoader.getHiveVersion()));

		List<Row> results = TableUtils.collectToList(tEnv.sqlQuery("select negative(5.1)"));

		assertEquals("[-5.1]", results.toString());
	}
}
