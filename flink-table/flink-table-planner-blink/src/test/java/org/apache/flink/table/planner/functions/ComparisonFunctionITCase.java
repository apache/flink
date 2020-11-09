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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.ARRAY;

/**
 * Tests for comparison functions in {@link BuiltInFunctionDefinitions}.
 */
public class ComparisonFunctionITCase extends BuiltInFunctionTestBase {

	@Parameterized.Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			// equal comparison between arrays
			TestSpec
				.forFunction(BuiltInFunctionDefinitions.EQUALS, "compare between arrays")
				.onFieldsWithData(Arrays.asList(1L, 2L, 3L))
				.andDataTypes(ARRAY(DataTypes.BIGINT()))
				.testSqlResult(
					"f0 = Array[1, 2, 3]",
					true,
					DataTypes.BOOLEAN()),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.EQUALS, "compare between nested arrays")
				.onFieldsWithData(Arrays.asList(new Integer[]{1, 2, 3}, new Integer[]{4, 5, 6}))
				.andDataTypes(ARRAY(ARRAY(DataTypes.INT())))
				.testSqlResult(
					"f0 = Array[Array[1.0, 2.0, 3.0], Array[4.0, 5.0, 6.0]]",
					true,
					DataTypes.BOOLEAN())
		);
	}
}
