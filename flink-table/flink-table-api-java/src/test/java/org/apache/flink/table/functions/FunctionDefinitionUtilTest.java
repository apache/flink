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
import org.apache.flink.api.common.typeinfo.Types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
		FunctionDefinition fd1 = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
			TestTableFunction.class.getName()
		);

		assertTrue(((TableFunctionDefinition) fd1).getTableFunction() instanceof TestTableFunction);

		FunctionDefinition fd2 = FunctionDefinitionUtil.createFunctionDefinition(
				"test",
				TestTableFunctionWithoutResultType.class.getName()
		);

		assertTrue(((TableFunctionDefinition) fd2).getTableFunction() instanceof TestTableFunctionWithoutResultType);
	}

	@Test
	public void testAggregateFunction() {
		FunctionDefinition fd1 = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
			TestAggFunction.class.getName()
		);

		assertTrue(((AggregateFunctionDefinition) fd1).getAggregateFunction() instanceof TestAggFunction);

		FunctionDefinition fd2 = FunctionDefinitionUtil.createFunctionDefinition(
				"test",
				TestAggFunctionWithoutResultType.class.getName()
		);

		assertTrue(((AggregateFunctionDefinition) fd2).getAggregateFunction()
				instanceof TestAggFunctionWithoutResultType);
		assertEquals(((AggregateFunctionDefinition) fd2).getResultTypeInfo(), Types.LONG);
		assertEquals(((AggregateFunctionDefinition) fd2).getAccumulatorTypeInfo(), Types.STRING);

	}

	@Test
	public void testTableAggregateFunction() {
		FunctionDefinition fd1 = FunctionDefinitionUtil.createFunctionDefinition(
			"test",
			TestTableAggFunction.class.getName()
		);

		assertTrue(((TableAggregateFunctionDefinition) fd1).getTableAggregateFunction() instanceof TestTableAggFunction);

		FunctionDefinition fd2 = FunctionDefinitionUtil.createFunctionDefinition(
				"test",
				TestTableAggFunctionWithoutResultType.class.getName()
		);

		assertTrue(((TableAggregateFunctionDefinition) fd2).getTableAggregateFunction()
				instanceof TestTableAggFunctionWithoutResultType);
		assertEquals(((TableAggregateFunctionDefinition) fd2).getResultTypeInfo(), Types.LONG);
		assertEquals(((TableAggregateFunctionDefinition) fd2).getAccumulatorTypeInfo(), Types.STRING);
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
	public static class TestTableFunctionWithoutResultType extends TableFunction<String> {
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
	public static class TestAggFunctionWithoutResultType extends AggregateFunction<Long, String> {
		@Override
		public String createAccumulator() {
			return null;
		}

		@Override
		public Long getValue(String accumulator) {
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

	/**
	 * Test function.
	 */
	public static class TestTableAggFunctionWithoutResultType extends TableAggregateFunction<Long, String> {
		@Override
		public String createAccumulator() {
			return null;
		}
	}
}
