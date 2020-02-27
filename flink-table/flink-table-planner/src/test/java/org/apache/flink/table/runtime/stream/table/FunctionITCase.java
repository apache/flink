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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Tests for both catalog and system function.
 */
public class FunctionITCase extends AbstractTestBase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testPrimitiveScalarFunction() throws Exception {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("The new type inference for functions is only supported in the Blink planner.");

		StreamExecutionEnvironment streamExecEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecEnvironment);

		Table table = tableEnvironment
			.sqlQuery("SELECT * FROM (VALUES (1)) AS TableName(f0)")
			.select(call(new SimpleScalarFunction(), $("f0")));
		tableEnvironment.toAppendStream(table, Row.class).print();

		streamExecEnvironment.execute();
	}

	/**
	 * Scalar function that uses new type inference stack.
	 */
	public static class SimpleScalarFunction extends ScalarFunction {
		public long eval(Integer i) {
			return i;
		}
	}
}
