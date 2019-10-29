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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link HiveModule}.
 */
public class HiveModuleTest {
	@Test
	public void testNumberOfBuiltinFunctions() {
		assertEquals(287, new HiveModule("1.2.0").listFunctions().size());
		assertEquals(287, new HiveModule("2.1.1").listFunctions().size());
		assertEquals(287, new HiveModule("3.1.2").listFunctions().size());
	}

	@Test
	public void testHiveBuiltInFunction() {
		FunctionDefinition fd = new HiveModule("2.1.1").getFunctionDefinition("reverse").get();

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
