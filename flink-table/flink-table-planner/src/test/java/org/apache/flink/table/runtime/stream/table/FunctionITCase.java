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

package org.apache.flink.table.runtime.stream.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Tests for user defined functions in the Table API.
 */
public class FunctionITCase extends AbstractTestBase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDataTypeBasedTypeInferenceNotSupported() throws Exception {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("The new type inference for functions is only supported in the Blink planner.");

		StreamExecutionEnvironment streamExecEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
				streamExecEnvironment, settings);

		Table table = tableEnvironment
			.sqlQuery("SELECT * FROM (VALUES (1)) AS TableName(f0)")
			.select(call(new SimpleScalarFunction(), $("f0")));
		tableEnvironment.toAppendStream(table, Row.class).print();

		streamExecEnvironment.execute();
	}

	@Test
	public void testDataTypeBasedTypeInferenceNotSupportedInLateralJoin() throws Exception {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("The new type inference for functions is only supported in the Blink planner.");

		StreamExecutionEnvironment streamExecEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
				streamExecEnvironment, settings);

		Table table = tableEnvironment
			.sqlQuery("SELECT * FROM (VALUES ('A,B,C')) AS TableName(f0)")
			.joinLateral(call(new SimpleTableFunction(), $("f0")).as("a", "b"))
			.select($("a"), $("b"));
		tableEnvironment.toAppendStream(table, Row.class).print();

		streamExecEnvironment.execute();
	}

	/**
	 * Simple scalar function.
	 */
	public static class SimpleScalarFunction extends ScalarFunction {
		public long eval(Integer i) {
			return i;
		}
	}

	/**
	 * Simple table function.
	 */
	@FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING>>"))
	public static class SimpleTableFunction extends TableFunction<Row> {
		public void eval(String s) {
			// no-op
		}
	}
}
