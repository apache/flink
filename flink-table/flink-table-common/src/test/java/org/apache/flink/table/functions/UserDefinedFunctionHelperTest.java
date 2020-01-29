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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link UserDefinedFunctionHelper}.
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unused")
public class UserDefinedFunctionHelperTest {

	@Parameterized.Parameters
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.forClass(ValidScalarFunction.class)
				.expectSuccess(),

			TestSpec
				.forInstance(new ValidScalarFunction())
				.expectSuccess(),

			TestSpec
				.forClass(PrivateScalarFunction.class)
				.expectErrorMessage(
					"Function class '" + PrivateScalarFunction.class.getName() + "' is not public."),

			TestSpec
				.forClass(MissingImplementationScalarFunction.class)
				.expectErrorMessage(
					"Function class '" + MissingImplementationScalarFunction.class.getName() +
						"' does not implement a method named 'eval'."),

			TestSpec
				.forClass(PrivateMethodScalarFunction.class)
				.expectErrorMessage(
					"Method 'eval' of function class '" + PrivateMethodScalarFunction.class.getName() +
						"' is not public."),

			TestSpec
				.forInstance(new ValidTableAggregateFunction())
				.expectSuccess(),

			TestSpec
				.forInstance(new MissingEmitTableAggregateFunction())
				.expectErrorMessage(
					"Function class '" + MissingEmitTableAggregateFunction.class.getName() +
						"' does not implement a method named 'emitUpdateWithRetract' or 'emitValue'."),

			TestSpec
				.forInstance(new ValidTableFunction())
				.expectSuccess(),

			TestSpec
				.forInstance(new ParameterizedTableFunction(12))
				.expectSuccess(),

			TestSpec
				.forClass(ParameterizedTableFunction.class)
				.expectErrorMessage(
					"Function class '" + ParameterizedTableFunction.class.getName() +
						"' must have a public default constructor."),

			TestSpec
				.forClass(HierarchicalTableAggregateFunction.class)
				.expectSuccess()
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInstantiation() {
		if (testSpec.functionClass != null) {
			if (testSpec.expectedErrorMessage != null) {
				thrown.expect(ValidationException.class);
				thrown.expectMessage(testSpec.expectedErrorMessage);
			}
			assertThat(
				UserDefinedFunctionHelper.instantiateFunction(testSpec.functionClass),
				notNullValue());
		}
	}

	@Test
	public void testValidation() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectMessage(testSpec.expectedErrorMessage);
		}
		if (testSpec.functionClass != null) {
			UserDefinedFunctionHelper.validateClass(testSpec.functionClass);
		} else if (testSpec.functionInstance != null) {
			UserDefinedFunctionHelper.prepareInstance(new Configuration(), testSpec.functionInstance);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test utilities
	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		final @Nullable Class<? extends UserDefinedFunction> functionClass;

		final @Nullable UserDefinedFunction functionInstance;

		@Nullable String expectedErrorMessage;

		TestSpec(Class<? extends UserDefinedFunction> functionClass) {
			this.functionClass = functionClass;
			this.functionInstance = null;
		}

		TestSpec(UserDefinedFunction functionInstance) {
			this.functionClass = null;
			this.functionInstance = functionInstance;
		}

		static TestSpec forClass(Class<? extends UserDefinedFunction> function) {
			return new TestSpec(function);
		}

		static TestSpec forInstance(UserDefinedFunction function) {
			return new TestSpec(function);
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}

		TestSpec expectSuccess() {
			this.expectedErrorMessage = null;
			return this;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test classes for validation
	// --------------------------------------------------------------------------------------------

	/**
	 * Valid scalar function.
	 */
	public static class ValidScalarFunction extends ScalarFunction {
		public String eval(int i) {
			return null;
		}
	}

	private static class PrivateScalarFunction extends ScalarFunction {
		public String eval(int i) {
			return null;
		}
	}

	/**
	 * No implementation method.
	 */
	public static class MissingImplementationScalarFunction extends ScalarFunction {
		// nothing to do
	}

	/**
	 * Implementation method is private.
	 */
	public static class PrivateMethodScalarFunction extends ScalarFunction {
		private String eval(int i) {
			return null;
		}
	}

	/**
	 * Valid table aggregate function.
	 */
	public static class ValidTableAggregateFunction extends TableAggregateFunction<String, String> {

		public void accumulate(String acc, String in) {
			// nothing to do
		}

		public void emitValue(String acc, Collector<String> out) {
			// nothing to do
		}

		@Override
		public String createAccumulator() {
			return null;
		}
	}

	/**
	 * No emitting method implementation.
	 */
	public static class MissingEmitTableAggregateFunction extends TableAggregateFunction<String, String> {

		public void accumulate(String acc, String in) {
			// nothing to do
		}

		@Override
		public String createAccumulator() {
			return null;
		}
	}

	/**
	 * Valid table function.
	 */
	public static class ValidTableFunction extends TableFunction<String> {
		public void eval(String i) {
			// nothing to do
		}
	}

	/**
	 * Table function with parameters in constructor.
	 */
	public static class ParameterizedTableFunction extends TableFunction<String> {

		public ParameterizedTableFunction(int param) {
			// nothing to do
		}

		public void eval(String i) {
			// nothing to do
		}
	}

	private abstract static class AbstractTableAggregateFunction extends TableAggregateFunction<String, String> {
		public void accumulate(String acc, String in) {
			// nothing to do
		}
	}

	/**
	 * Hierarchy that is implementing different methods.
	 */
	public static class HierarchicalTableAggregateFunction extends AbstractTableAggregateFunction {

		public void emitValue(String acc, Collector<String> out) {
			// nothing to do
		}

		@Override
		public String createAccumulator() {
			return null;
		}
	}
}
