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

package org.apache.flink.graph.drivers;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.graph.Runner;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.FlinkRuntimeException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

public abstract class DriverBaseITCase
extends MultipleProgramsTestBase {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	protected DriverBaseITCase(TestExecutionMode mode) {
		super(mode);
	}

	// extend MultipleProgramsTestBase default to include object reuse mode
	@Parameterized.Parameters(name = "Execution mode = {0}")
	public static Collection<Object[]> executionModes() {
		return Arrays.asList(
			new Object[] { TestExecutionMode.CLUSTER },
			new Object[] { TestExecutionMode.CLUSTER_OBJECT_REUSE },
			new Object[] { TestExecutionMode.COLLECTION });
	}

	/**
	 * Simpler variant of {@link #expectedOutput(String[], String)}
	 * that only compares the count of the number of records in standard output.
	 * This is intended for use for algorithms where the result cannot be
	 * hashed due to approximate results (typically floating point arithmetic).
	 *
	 * @param parameters algorithm, input, and output arguments
	 * @param records expected number of records in standard output
	 * @throws Exception on error
	 */
	protected void expectedCount(String[] parameters, int records) throws Exception {
		String output = getSystemOutput(parameters);

		// subtract the extra newline
		int numberOfRecords = output.split(System.getProperty("line.separator")).length - 1;
		Assert.assertEquals(records, numberOfRecords);
	}

	/**
	 * Executes the driver with the provided arguments and compares the
	 * standard output with the given regular expression.
	 *
	 * @param parameters algorithm, input, and output arguments
	 * @param expected expected standard output
	 * @throws Exception on error
	 */
	protected void expectedOutput(String[] parameters, String expected) throws Exception {
		String output = getSystemOutput(parameters);

		Assert.assertThat(output, RegexMatcher.matchesRegex(expected));
	}

	/**
	 * Executes the driver with the provided arguments and compares the
	 * exception and exception method with the given class and regular
	 * expression.
	 *
	 * @param parameters algorithm, input, and output arguments
	 * @param expected expected standard output
	 * @param exception expected exception
	 * @throws Exception on error when not matching exception
	 */
	protected void expectedOutputFromException(String[] parameters, String expected,Class<? extends Throwable> exception) throws Exception {
		expectedException.expect(exception);
		expectedException.expectMessage(RegexMatcher.matchesRegex(expected));

		getSystemOutput(parameters);
	}

	/**
	 * Generate a regular expression string by quoting the input string and
	 * adding wildcard matchers to the beginning and end.
	 *
	 * @param input source string
	 * @return regex string
	 */
	protected String regexSubstring(String input) {
		// Pattern.quote disables regex interpretation of the input string and
		// flag expression "(?s)" (Pattern.DOTALL) matches "." against any
		// character including line terminators
		return "(?s).*" + Pattern.quote(input) + ".*";
	}

	/**
	 * Capture the command-line standard output from the driver execution.
	 *
	 * @param args driver command-line arguments
	 * @return standard output from driver execution
	 * @throws Exception on error
	 */
	private String getSystemOutput(String[] args) throws Exception {
		ByteArrayOutputStream output = new ByteArrayOutputStream();

		// Configure object reuse mode
		switch (mode) {
			case CLUSTER:
			case COLLECTION:
				args = (String[])ArrayUtils.add(args, "--__disable_object_reuse");
				break;

			case CLUSTER_OBJECT_REUSE:
				// object reuse is enabled by default when executing drivers
				break;

			default:
				throw new FlinkRuntimeException("Unknown execution mode " + mode);
		}

		// Redirect stdout
		PrintStream stdout = System.out;
		System.setOut(new PrintStream(output));

		Runner.main(args);

		// Restore stdout
		System.setOut(stdout);

		return output.toString();
	}

	/**
	 * Implements a Hamcrest regex matcher. Hamcrest 2.0 provides
	 * Matchers.matchesPattern(String) but Flink depends on Hamcrest 1.3.
	 *
	 * see http://stackoverflow.com/a/25021229
	 */
	private static class RegexMatcher
	extends TypeSafeMatcher<String> {
		private final String regex;

		private RegexMatcher(final String regex) {
			this.regex = regex;
		}

		@Override
		public void describeTo(final Description description) {
			description.appendText("matches regex=`" + regex + "`");
		}

		@Override
		public boolean matchesSafely(final String string) {
			return string.matches(regex);
		}

		public static RegexMatcher matchesRegex(final String regex) {
			return new RegexMatcher(regex);
		}
	}
}
