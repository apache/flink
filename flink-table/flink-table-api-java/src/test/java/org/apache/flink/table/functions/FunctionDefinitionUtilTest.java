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

package org.apache.flink.table.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for {@link FunctionDefinitionUtil}.
 */
public class FunctionDefinitionUtilTest {
	@Test
	public void testScalarFunction() {
		FunctionDefinition fd = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
				TestScalarFunction.class.getName()
		);

		assertTrue(((ScalarFunctionDefinition) fd).getScalarFunction() instanceof TestScalarFunction);
	}

	@Test
	public void testTableFunction() {
		FunctionDefinition fd = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
			TestTableFunction.class.getName()
		);

		assertTrue(((TableFunctionDefinition) fd).getTableFunction() instanceof TestTableFunction);
	}

	@Test
	public void testAggregateFunction() {
		FunctionDefinition fd = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
			TestAggFunction.class.getName()
		);

		assertTrue(((AggregateFunctionDefinition) fd).getAggregateFunction() instanceof TestAggFunction);
	}

	@Test
	public void testTableAggregateFunction() {
		FunctionDefinition fd = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
			TestTableAggFunction.class.getName()
		);

		assertTrue(((TableAggregateFunctionDefinition) fd).getTableAggregateFunction() instanceof TestTableAggFunction);
	}

	/**
	 * Test function.
	 */
	public static class TestScalarFunction extends ScalarFunction {

	}

	/**
	 * Test function.
	 */
	public static class TestTableFunction extends TableFunction {
		@Override
		public TypeInformation getResultType() {
			return TypeInformation.of(Object.class);
		}
	}

	/**
	 * Test function.
	 */
	public static class TestAggFunction extends AggregateFunction {
		@Override
		public Object createAccumulator() {
			return null;
		}

		@Override
		public TypeInformation getResultType() {
			return TypeInformation.of(Object.class);
		}

		@Override
		public TypeInformation getAccumulatorType() {
			return TypeInformation.of(Object.class);
		}

		@Override
		public Object getValue(Object accumulator) {
			return null;
		}
	}

	/**
	 * Test function.
	 */
	public static class TestTableAggFunction extends TableAggregateFunction {
		@Override
		public Object createAccumulator() {
			return null;
		}

		@Override
		public TypeInformation getResultType() {
			return TypeInformation.of(Object.class);
		}

		@Override
		public TypeInformation getAccumulatorType() {
			return TypeInformation.of(Object.class);
		}
	}
}
