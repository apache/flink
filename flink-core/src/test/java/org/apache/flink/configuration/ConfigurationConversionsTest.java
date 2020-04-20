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

package org.apache.flink.configuration;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Configuration} conversion between types. Extracted from {@link ConfigurationTest}.
 */
@RunWith(Parameterized.class)
public class ConfigurationConversionsTest {

	private static final byte[] EMPTY_BYTES = new byte[0];
	private static final long TOO_LONG = Integer.MAX_VALUE + 10L;
	private static final double TOO_LONG_DOUBLE = Double.MAX_VALUE;

	private Configuration pc;

	@Before
	public void init() {
		pc = new Configuration();

		pc.setInteger("int", 5);
		pc.setLong("long", 15);
		pc.setLong("too_long", TOO_LONG);
		pc.setFloat("float", 2.1456775f);
		pc.setDouble("double", Math.PI);
		pc.setDouble("negative_double", -1.0);
		pc.setDouble("zero", 0.0);
		pc.setDouble("too_long_double", TOO_LONG_DOUBLE);
		pc.setString("string", "42");
		pc.setString("non_convertible_string", "bcdefg&&");
		pc.setBoolean("boolean", true);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Parameterized.Parameters
	public static Collection<TestSpec> getSpecs() {
		return Arrays.asList(
			// from integer
			TestSpec.whenAccessed(conf -> conf.getInteger("int", 0)).expect(5),
			TestSpec.whenAccessed(conf -> conf.getLong("int", 0)).expect(5L),
			TestSpec.whenAccessed(conf -> conf.getFloat("int", 0)).expect(5f),
			TestSpec.whenAccessed(conf -> conf.getDouble("int", 0)).expect(5.0),
			TestSpec.whenAccessed(conf -> conf.getBoolean("int", true))
				.expectException("Unrecognized option for boolean: 5. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("int", "0")).expect("5"),
			TestSpec.whenAccessed(conf -> conf.getBytes("int", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 5 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"int",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Integer as a class name"),

			// from long
			TestSpec.whenAccessed(conf -> conf.getInteger("long", 0)).expect(15),
			TestSpec.whenAccessed(conf -> conf.getLong("long", 0)).expect(15L),
			TestSpec.whenAccessed(conf -> conf.getFloat("long", 0)).expect(15f),
			TestSpec.whenAccessed(conf -> conf.getDouble("long", 0)).expect(15.0),
			TestSpec.whenAccessed(conf -> conf.getBoolean("long", true))
				.expectException("Unrecognized option for boolean: 15. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("long", "0")).expect("15"),
			TestSpec.whenAccessed(conf -> conf.getBytes("long", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 15 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"long",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Long as a class name"),

			// from too long
			TestSpec.whenAccessed(conf -> conf.getInteger("too_long", 0))
				.expectException("Configuration value 2147483657 overflows/underflows the integer type"),
			TestSpec.whenAccessed(conf -> conf.getLong("too_long", 0)).expect(TOO_LONG),
			TestSpec.whenAccessed(conf -> conf.getFloat("too_long", 0)).expect((float) TOO_LONG),
			TestSpec.whenAccessed(conf -> conf.getDouble("too_long", 0)).expect((double) TOO_LONG),
			TestSpec.whenAccessed(conf -> conf.getBoolean("too_long", true))
				.expectException(
					"Unrecognized option for boolean: 2147483657. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("too_long", "0")).expect(String.valueOf(TOO_LONG)),
			TestSpec.whenAccessed(conf -> conf.getBytes("too_long", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 2147483657 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"too_long",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Long as a class name"),

			// from float
			TestSpec.whenAccessed(conf -> conf.getInteger("float", 0))
				.expectException("For input string: \"2.1456776\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getLong("float", 0))
				.expectException("For input string: \"2.1456776\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getFloat("float", 0))
				.expect(2.1456775f),
			TestSpec.whenAccessed(conf -> conf.getDouble("float", 0)).expect(closeTo(2.1456775, 0.0000001)),
			TestSpec.whenAccessed(conf -> conf.getBoolean("float", true))
				.expectException(
					"Unrecognized option for boolean: 2.1456776. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("float", "0")).expect(startsWith("2.145677")),
			TestSpec.whenAccessed(conf -> conf.getBytes("float", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 2.1456776 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"float",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("onfiguration cannot evaluate object of class class java.lang.Float as a class name"),

			// from double
			TestSpec.whenAccessed(conf -> conf.getInteger("double", 0))
				.expectException("For input string: \"3.141592653589793\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getLong("double", 0))
				.expectException("For input string: \"3.141592653589793\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getFloat("double", 0)).expect(new IsCloseTo(3.141592f, 0.000001f)),
			TestSpec.whenAccessed(conf -> conf.getDouble("double", 0)).expect(Math.PI),
			TestSpec.whenAccessed(conf -> conf.getBoolean("double", true))
				.expectException(
					"Unrecognized option for boolean: 3.141592653589793. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("double", "0")).expect(startsWith("3.1415926535")),
			TestSpec.whenAccessed(conf -> conf.getBytes("double", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 3.141592653589793 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"double",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("onfiguration cannot evaluate object of class class java.lang.Double as a class name"),

			// from negative double
			TestSpec.whenAccessed(conf -> conf.getInteger("negative_double", 0))
				.expectException("For input string: \"-1.0\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getLong("negative_double", 0))
				.expectException("For input string: \"-1.0\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getFloat("negative_double", 0))
				.expect(new IsCloseTo(-1f, 0.000001f)),
			TestSpec.whenAccessed(conf -> conf.getDouble("negative_double", 0)).expect(-1D),
			TestSpec.whenAccessed(conf -> conf.getBoolean("negative_double", true))
				.expectException("Unrecognized option for boolean: -1.0. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("negative_double", "0")).expect(startsWith("-1")),
			TestSpec.whenAccessed(conf -> conf.getBytes("negative_double", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value -1.0 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"negative_double",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Double as a class name"),

			// from zero
			TestSpec.whenAccessed(conf -> conf.getInteger("zero", 0))
				.expectException("For input string: \"0.0\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getLong("zero", 0))
				.expectException("For input string: \"0.0\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getFloat("zero", 0)).expect(new IsCloseTo(0f, 0.000001f)),
			TestSpec.whenAccessed(conf -> conf.getDouble("zero", 0)).expect(0D),
			TestSpec.whenAccessed(conf -> conf.getBoolean("zero", true))
				.expectException("Unrecognized option for boolean: 0.0. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("zero", "0")).expect(startsWith("0")),
			TestSpec.whenAccessed(conf -> conf.getBytes("zero", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 0.0 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"zero",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Double as a class name"),

			// from too long double
			TestSpec.whenAccessed(conf -> conf.getInteger("too_long_double", 0))
				.expectException("For input string: \"1.7976931348623157E308\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getLong("too_long_double", 0))
				.expectException("For input string: \"1.7976931348623157E308\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getFloat("too_long_double", 0))
				.expectException("Configuration value 1.7976931348623157E308 overflows/underflows the float type."),
			TestSpec.whenAccessed(conf -> conf.getDouble("too_long_double", 0)).expect(TOO_LONG_DOUBLE),
			TestSpec.whenAccessed(conf -> conf.getBoolean("too_long_double", true))
				.expectException("Unrecognized option for boolean: 1.7976931348623157E308. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("too_long_double", "0"))
				.expect(String.valueOf(TOO_LONG_DOUBLE)),
			TestSpec.whenAccessed(conf -> conf.getBytes("too_long_double", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 1.7976931348623157E308 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"too_long_double",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Double as a class name"),

			// from string
			TestSpec.whenAccessed(conf -> conf.getInteger("string", 0)).expect(42),
			TestSpec.whenAccessed(conf -> conf.getLong("string", 0)).expect(42L),
			TestSpec.whenAccessed(conf -> conf.getFloat("string", 0)).expect(42f),
			TestSpec.whenAccessed(conf -> conf.getDouble("string", 0)).expect(42.0),
			TestSpec.whenAccessed(conf -> conf.getBoolean("string", true))
				.expectException("Unrecognized option for boolean: 42. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("string", "0")).expect("42"),
			TestSpec.whenAccessed(conf -> conf.getBytes("string", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value 42 as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"string",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("42", ClassNotFoundException.class),

			// from non convertible string
			TestSpec.whenAccessed(conf -> conf.getInteger("non_convertible_string", 0))
				.expectException("For input string: \"bcdefg&&\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getLong("non_convertible_string", 0))
				.expectException("For input string: \"bcdefg&&\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getFloat("non_convertible_string", 0))
				.expectException("For input string: \"bcdefg&&\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getDouble("non_convertible_string", 0))
				.expectException("For input string: \"bcdefg&&\"", NumberFormatException.class),
			TestSpec.whenAccessed(conf -> conf.getBoolean("non_convertible_string", true))
				.expectException("Unrecognized option for boolean: bcdefg&&. Expected either true or false(case insensitive)"),
			TestSpec.whenAccessed(conf -> conf.getString("non_convertible_string", "0")).expect("bcdefg&&"),
			TestSpec.whenAccessed(conf -> conf.getBytes("non_convertible_string", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value bcdefg&& as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"non_convertible_string",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("bcdefg&&", ClassNotFoundException.class),

			// from boolean
			TestSpec.whenAccessed(conf -> conf.getInteger("boolean", 0))
				.expectException("For input string: \"true\""),
			TestSpec.whenAccessed(conf -> conf.getLong("boolean", 0))
				.expectException("For input string: \"true\""),
			TestSpec.whenAccessed(conf -> conf.getFloat("boolean", 0))
				.expectException("For input string: \"true\""),
			TestSpec.whenAccessed(conf -> conf.getDouble("boolean", 0))
				.expectException("For input string: \"true\""),
			TestSpec.whenAccessed(conf -> conf.getBoolean("boolean", false)).expect(true),
			TestSpec.whenAccessed(conf -> conf.getString("boolean", "0")).expect("true"),
			TestSpec.whenAccessed(conf -> conf.getBytes("boolean", EMPTY_BYTES))
				.expectException("Configuration cannot evaluate value true as a byte[] value"),
			TestSpec.whenAccessed(conf -> conf.getClass(
				"boolean",
				ConfigurationConversionsTest.class,
				ConfigurationConversionsTest.class.getClassLoader()))
				.expectException("Configuration cannot evaluate object of class class java.lang.Boolean as a class name")
		);
	}

	@Parameterized.Parameter
	public TestSpec<?> testSpec;

	@Test
	public void testConversions() throws Exception {
		testSpec.getExpectedException().ifPresent(exception -> {
				thrown.expect(testSpec.getExceptionClass());
				thrown.expectMessage(exception);
			}
		);

		// workaround for type erasure
		testSpec.assertConfiguration(pc);
	}

	private static class IsCloseTo extends TypeSafeMatcher<Float> {
		private final float delta;
		private final float value;

		public IsCloseTo(float value, float error) {
			this.delta = error;
			this.value = value;
		}

		public boolean matchesSafely(Float item) {
			return this.actualDelta(item) <= 0.0D;
		}

		public void describeMismatchSafely(Float item, Description mismatchDescription) {
			mismatchDescription.appendValue(item).appendText(" differed by ").appendValue(this.actualDelta(item));
		}

		public void describeTo(Description description) {
			description.appendText("a numeric value within ")
				.appendValue(this.delta)
				.appendText(" of ")
				.appendValue(this.value);
		}

		private double actualDelta(Float item) {
			return Math.abs(item - this.value) - this.delta;
		}
	}

	private static class TestSpec<T> {
		private final ConfigurationAccessor<T> configurationAccessor;
		private Matcher<T> matcher;
		@Nullable private String expectedException = null;
		@Nullable private Class<? extends Exception> exceptionClass;

		@FunctionalInterface
		private interface ConfigurationAccessor<T> {
			T access(Configuration configuration) throws Exception;
		}

		private TestSpec(ConfigurationAccessor<T> configurationAccessor) {
			this.configurationAccessor = configurationAccessor;
		}

		public static <T> TestSpec<T> whenAccessed(ConfigurationAccessor<T> configurationAccessor) {
			return new TestSpec<T>(configurationAccessor);
		}

		public TestSpec<T> expect(Matcher<T> expected) {
			this.matcher = expected;
			return this;
		}

		public TestSpec<T> expect(T expected) {
			this.matcher = equalTo(expected);
			return this;
		}

		public TestSpec<T> expectException(String message) {
			this.expectedException = message;
			this.exceptionClass = IllegalArgumentException.class;
			return this;
		}

		public TestSpec<T> expectException(String message, Class<? extends Exception> exceptionClass) {
			this.expectedException = message;
			this.exceptionClass = exceptionClass;
			return this;
		}

		public Optional<String> getExpectedException() {
			return Optional.ofNullable(expectedException);
		}

		@Nullable
		public Class<? extends Exception> getExceptionClass() {
			return exceptionClass;
		}

		void assertConfiguration(Configuration conf) throws Exception {
			assertThat(configurationAccessor.access(conf), matcher);
		}
	}
}
