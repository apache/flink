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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ClassInstance;
import org.apache.flink.table.descriptors.FunctionDescriptor;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link FunctionService}.
 */
public class FunctionServiceTest {

	@Test(expected = ValidationException.class)
	public void testWrongArgsFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(new ClassInstance()
						.of(NoArgClass.class.getName())
						.parameterString("12"));

		FunctionService.createFunction(descriptor);
	}

	@Test(expected = ValidationException.class)
	public void testPrivateFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(new ClassInstance().of(PrivateClass.class.getName()));

		FunctionService.createFunction(descriptor);
	}

	@Test(expected = ValidationException.class)
	public void testInvalidClassFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(new ClassInstance().of("this.class.does.not.exist"));

		FunctionService.createFunction(descriptor);
	}

	@Test(expected = ValidationException.class)
	public void testNotFunctionClassFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(new ClassInstance()
						.of(String.class.getName())
						.parameterString("hello"));

		FunctionService.createFunction(descriptor);
	}

	@Test(expected = ValidationException.class)
	public void testErrorConstructorClass() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(new ClassInstance()
						.of(ErrorConstructorClass.class.getName())
						.parameterString("arg"));

		FunctionService.createFunction(descriptor);
	}

	@Test
	public void testNoArgFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(new ClassInstance().of(NoArgClass.class.getName()));

		assertEquals(NoArgClass.class, FunctionService.createFunction(descriptor).getClass());
	}

	@Test
	public void testOneArgFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(
						new ClassInstance()
								.of(OneArgClass.class.getName())
								.parameterString("false"));

		UserDefinedFunction actualFunction = FunctionService.createFunction(descriptor);

		assertEquals(OneArgClass.class, actualFunction.getClass());
		assertFalse(((OneArgClass) actualFunction).field);
	}

	@Test
	public void testMultiArgFunctionCreation() {
		FunctionDescriptor descriptor = new FunctionDescriptor()
				.fromClass(
						new ClassInstance()
								.of(MultiArgClass.class.getName())
								.parameter(new java.math.BigDecimal("12.0003"))
								.parameter(new ClassInstance()
										.of(BigInteger.class.getName())
										.parameter("111111111111111111111111111111111")));

		UserDefinedFunction actualFunction = FunctionService.createFunction(descriptor);

		assertEquals(MultiArgClass.class, actualFunction.getClass());
		assertEquals(
				new java.math.BigDecimal("12.0003"),
				((MultiArgClass) actualFunction).field1);
		assertEquals(
				new java.math.BigInteger("111111111111111111111111111111111"),
				((MultiArgClass) actualFunction).field2);
	}

	/**
	 * Test no argument.
	 */
	public static class NoArgClass extends ScalarFunction {}

	/**
	 * Test one argument.
	 */
	public static class OneArgClass extends ScalarFunction {
		public Boolean field;

		public OneArgClass(Boolean field) {
			this.field = field;
		}
	}

	/**
	 * Test multi arguments.
	 */
	public static class MultiArgClass extends ScalarFunction {
		public final BigDecimal field1;
		public final BigInteger field2;

		public MultiArgClass(BigDecimal field1, BigInteger field2) {
			this.field1 = field1;
			this.field2 = field2;
		}
	}

	/**
	 * Test private constructor.
	 */
	public static class PrivateClass extends ScalarFunction {
		private PrivateClass() {}
	}

	/**
	 * Test error constructor.
	 */
	public static class ErrorConstructorClass extends ScalarFunction {
		public ErrorConstructorClass(String arg) {
			throw new RuntimeException(arg);
		}
	}
}
