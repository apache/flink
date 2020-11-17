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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;

/**
 * Tests for different combinations around {@link BuiltInFunctionDefinitions#ROW}.
 */
public class RowFunctionITCase extends BuiltInFunctionTestBase {

	@Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.forFunction(BuiltInFunctionDefinitions.ROW, "with field access")
				.onFieldsWithData(12, "Hello world")
				.andDataTypes(DataTypes.INT(), DataTypes.STRING())
				.testTableApiResult(
					row($("f0"), $("f1")),
					Row.of(12, "Hello world"),
					DataTypes.ROW(
						DataTypes.FIELD("f0", DataTypes.INT()),
						DataTypes.FIELD("f1", DataTypes.STRING())).notNull())
				.testSqlResult(
					"ROW(f0, f1)",
					Row.of(12, "Hello world"),
					DataTypes.ROW(
						DataTypes.FIELD("EXPR$0", DataTypes.INT()),
						DataTypes.FIELD("EXPR$1", DataTypes.STRING())).notNull()),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.ROW, "within function call")
				.onFieldsWithData(12, "Hello world")
				.andDataTypes(DataTypes.INT(), DataTypes.STRING())
				.withFunction(TakesRow.class)
				.testResult(
					call("TakesRow", row($("f0"), $("f1")), 1),
					"TakesRow(ROW(f0, f1), 1)",
					Row.of(13, "Hello world"),
					DataTypes.ROW(
						DataTypes.FIELD("i", DataTypes.INT()),
						DataTypes.FIELD("s", DataTypes.STRING()))),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.ROW, "within cast")
				.onFieldsWithData(1)
				.testResult(
					row($("f0").plus(12), "Hello world")
						.cast(
							DataTypes.ROW(
								DataTypes.FIELD("i", DataTypes.INT()),
								DataTypes.FIELD("s", DataTypes.STRING())).notNull()
						),
					"CAST((f0 + 12, 'Hello world') AS ROW<i INT, s STRING>)",
					Row.of(13, "Hello world"),
					DataTypes.ROW(
						DataTypes.FIELD("i", DataTypes.INT()),
						DataTypes.FIELD("s", DataTypes.STRING())).notNull())
		);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Identity function for a row.
	 */
	public static class TakesRow extends ScalarFunction {
		public @DataTypeHint("ROW<i INT, s STRING>") Row eval(@DataTypeHint("ROW<i INT, s STRING>") Row row, Integer i) {
			row.setField(0, (int) row.getField(0) + i);
			return row;
		}
	}
}
