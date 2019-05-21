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

package org.apache.flink.table.expressions;

import org.apache.flink.table.expressions.FunctionDefinition.Type;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import org.junit.Test;

import static org.apache.flink.table.expressions.FunctionDefinition.getFunctionType;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link FunctionDefinition}.
 */
public class FunctionDefinitionTest {

	private static final UserDefinedFunction DUMMY_USER_DEFINED_FUNCTION = new UserDefinedFunction() {
		// dummy
	};

	private static final ScalarFunction DUMMY_SCALAR_FUNCTION = new ScalarFunction() {
		// dummy
	};

	private static final TableFunction DUMMY_TABLE_FUNCTION = new TableFunction() {
		// dummy
	};

	private static final AggregateFunction DUMMY_AGGREGATE_FUNCTION = new AggregateFunction() {
		@Override
		public Object getValue(Object accumulator) {
			return null;
		}

		@Override
		public Object createAccumulator() {
			return null;
		}
	};

	private static final TableAggregateFunction DUMMY_TABLE_AGGREGATE_FUNCTION = new TableAggregateFunction() {
		@Override
		public Object createAccumulator() {
			return null;
		}
	};

	@Test
	public void testGetFunctionType() {
		assertEquals(Type.SCALAR_FUNCTION, getFunctionType(DUMMY_SCALAR_FUNCTION));
		assertEquals(Type.TABLE_FUNCTION, getFunctionType(DUMMY_TABLE_FUNCTION));
		assertEquals(Type.AGGREGATE_FUNCTION, getFunctionType(DUMMY_AGGREGATE_FUNCTION));
		assertEquals(Type.TABLE_AGGREGATE_FUNCTION, getFunctionType(DUMMY_TABLE_AGGREGATE_FUNCTION));
		assertEquals(Type.OTHER_FUNCTION, getFunctionType(DUMMY_USER_DEFINED_FUNCTION));
	}
}
