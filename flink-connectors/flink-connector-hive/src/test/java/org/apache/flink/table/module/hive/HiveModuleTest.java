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
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import static org.apache.flink.table.HiveVersionTestUtil.HIVE_120_OR_LATER;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V1_2_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_0_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_1_1;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_2_0;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V2_3_4;
import static org.apache.flink.table.catalog.hive.client.HiveShimLoader.HIVE_VERSION_V3_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Test for {@link HiveModule}.
 */
public class HiveModuleTest {
	@Test
	public void testNumberOfBuiltinFunctions() {
		String hiveVersion = HiveShimLoader.getHiveVersion();

		assumeTrue(hiveVersion.equals(HIVE_VERSION_V1_2_0));
		assertEquals(232, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());

		assumeTrue(hiveVersion.equals(HIVE_VERSION_V2_0_0));
		assertEquals(243, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());

		assumeTrue(hiveVersion.equals(HIVE_VERSION_V2_1_1)
			|| hiveVersion.equals(HIVE_VERSION_V2_2_0));
		assertEquals(253, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());

		assumeTrue(hiveVersion.equals(HIVE_VERSION_V2_3_4) || hiveVersion.equals(HIVE_VERSION_V3_1_1));
		assertEquals(287, new HiveModule(HiveShimLoader.getHiveVersion()).listFunctions().size());
	}

	@Test
	public void testHiveBuiltInFunction() {
		assumeTrue(HIVE_120_OR_LATER);
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
}
