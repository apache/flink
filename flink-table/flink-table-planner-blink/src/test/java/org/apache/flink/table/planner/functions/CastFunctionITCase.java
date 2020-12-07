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

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Tests for {@link BuiltInFunctionDefinitions#CAST}.
 */
public class CastFunctionITCase extends BuiltInFunctionTestBase {

	@Parameterized.Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.forFunction(BuiltInFunctionDefinitions.CAST, "implicit with different field names")
				.onFieldsWithData(Row.of(12, "Hello"))
				.andDataTypes(DataTypes.of("ROW<otherNameInt INT, otherNameString STRING>"))
				.withFunction(RowToFirstField.class)
				.testResult(
					call("RowToFirstField", $("f0")),
					"RowToFirstField(f0)",
					12,
					DataTypes.INT()),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.CAST, "implicit with type widening")
				.onFieldsWithData(Row.of((byte) 12, "Hello"))
				.andDataTypes(DataTypes.of("ROW<i TINYINT, s STRING>"))
				.withFunction(RowToFirstField.class)
				.testResult(
					call("RowToFirstField", $("f0")),
					"RowToFirstField(f0)",
					12,
					DataTypes.INT()),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.CAST, "implicit with nested type widening")
				.onFieldsWithData(Row.of(Row.of(12, 42), "Hello"))
				.andDataTypes(DataTypes.of("ROW<r ROW<i1 INT, i2 INT>, s STRING>"))
				.withFunction(NestedRowToFirstField.class)
				.testResult(
					call("NestedRowToFirstField", $("f0")),
					"NestedRowToFirstField(f0)",
					Row.of(12, 42.0),
					DataTypes.of("ROW<i INT, d DOUBLE>")),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.CAST, "explicit with nested rows and implicit nullability change")
				.onFieldsWithData(Row.of(Row.of(12, 42, null), "Hello"))
				.andDataTypes(DataTypes.of("ROW<r ROW<i1 INT, i2 INT, i3 INT>, s STRING>"))
				.testResult(
					$("f0").cast(
						DataTypes.ROW(
							DataTypes.FIELD(
								"r",
								DataTypes.ROW(
									DataTypes.FIELD("s", DataTypes.STRING()),
									DataTypes.FIELD("b", DataTypes.BOOLEAN()),
									DataTypes.FIELD("i", DataTypes.INT()))),
							DataTypes.FIELD("s", DataTypes.STRING())
						)
					),
					"CAST(f0 AS ROW<r ROW<s STRING NOT NULL, b BOOLEAN, i INT>, s STRING>)",
					Row.of(Row.of("12", true, null), "Hello"),
					// the inner NOT NULL is ignored in SQL because the outer ROW is nullable and
					// the cast does not allow setting the outer nullability but derives it from
					// the source operand
					DataTypes.of("ROW<r ROW<s STRING, b BOOLEAN, i INT>, s STRING>")),

			TestSpec
				.forFunction(BuiltInFunctionDefinitions.CAST, "explicit with nested rows and explicit nullability change")
				.onFieldsWithData(Row.of(Row.of(12, 42, null), "Hello"))
				.andDataTypes(DataTypes.of("ROW<r ROW<i1 INT, i2 INT, i3 INT>, s STRING>"))
				.testTableApiResult(
					$("f0").cast(
						DataTypes.ROW(
							DataTypes.FIELD(
								"r",
								DataTypes.ROW(
									DataTypes.FIELD("s", DataTypes.STRING().notNull()),
									DataTypes.FIELD("b", DataTypes.BOOLEAN()),
									DataTypes.FIELD("i", DataTypes.INT()))),
							DataTypes.FIELD("s", DataTypes.STRING())
						)
					),
					Row.of(Row.of("12", true, null), "Hello"),
					DataTypes.of("ROW<r ROW<s STRING NOT NULL, b BOOLEAN, i INT>, s STRING>"))
		);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Function that return the first field of the input row.
	 */
	public static class RowToFirstField extends ScalarFunction {
		public Integer eval(@DataTypeHint("ROW<i INT, s STRING>") Row row) {
			assert row.getField(0) instanceof Integer;
			assert row.getField(1) instanceof String;
			return (Integer) row.getField(0);
		}
	}

	/**
	 * Function that return the first field of the nested input row.
	 */
	public static class NestedRowToFirstField extends ScalarFunction {
		public @DataTypeHint("ROW<i INT, d DOUBLE>") Row eval(
				@DataTypeHint("ROW<r ROW<i INT, d DOUBLE>, s STRING>") Row row) {
			assert row.getField(0) instanceof Row;
			assert row.getField(1) instanceof String;
			return (Row) row.getField(0);
		}
	}
}
