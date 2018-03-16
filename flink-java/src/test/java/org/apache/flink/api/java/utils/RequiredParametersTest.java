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

package org.apache.flink.api.java.utils;

import org.apache.flink.util.TestLogger;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.fail;

/**
 * Tests for RequiredParameter class and its interactions with ParameterTool.
 */
public class RequiredParametersTest extends TestLogger {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testAddWithAlreadyExistingParameter() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage("Option with key berlin already exists.");

		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin"));
		required.add(new Option("berlin"));
	}

	@Test
	public void testStringBasedAddWithAlreadyExistingParameter() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage("Option with key berlin already exists.");

		RequiredParameters required = new RequiredParameters();
		required.add("berlin");
		required.add("berlin");
	}

	@Test
	public void testApplyToWithMissingParameters() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage(CoreMatchers.allOf(
				containsString("Missing arguments for:"),
				containsString("munich ")));

		ParameterTool parameter = ParameterTool.fromArgs(new String[]{});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("munich"));

		required.applyTo(parameter);
	}

	@Test
	public void testApplyToWithMissingDefaultValues() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage("No default value for undefined parameter berlin");

		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin"));

		required.applyTo(parameter);
	}

	@Test
	public void testApplyToWithInvalidParameterValueBasedOnOptionChoices() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage("Value river is not in the list of valid choices for key berlin");

		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin", "river"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin").choices("city", "metropolis"));

		required.applyTo(parameter);
	}

	@Test
	public void testApplyToWithParameterDefinedOnShortAndLongName() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage("Value passed for parameter berlin is ambiguous. " +
				"Value passed for short and long name.");

		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin", "value", "--b", "another"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin").alt("b"));

		required.applyTo(parameter);
	}

	@Test
	public void testApplyToMovesValuePassedOnShortNameToLongNameIfLongNameIsUndefined() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--b", "value"});
		RequiredParameters required = new RequiredParameters();

		try {
			required.add(new Option("berlin").alt("b"));
			parameter = required.applyTo(parameter);
			Assert.assertEquals(parameter.data.get("berlin"), "value");
			Assert.assertEquals(parameter.data.get("b"), "value");
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testDefaultValueDoesNotOverrideValuePassedOnShortKeyIfLongKeyIsNotPassedButPresent() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin", "--b", "value"});
		RequiredParameters required = new RequiredParameters();

		try {
			required.add(new Option("berlin").alt("b").defaultValue("something"));
			parameter = required.applyTo(parameter);
			Assert.assertEquals(parameter.data.get("berlin"), "value");
			Assert.assertEquals(parameter.data.get("b"), "value");
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testApplyToWithNonCastableType() throws RequiredParametersException {

		expectedException.expect(RequiredParametersException.class);
		expectedException.expectMessage("Value for parameter flag cannot be cast to type BOOLEAN");

		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--flag", "15"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("flag").type(OptionType.BOOLEAN));

		required.applyTo(parameter);
	}

	@Test
	public void testApplyToWithSimpleOption() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin", "value"});
		RequiredParameters required = new RequiredParameters();
		try {
			required.add(new Option("berlin"));
			parameter = required.applyTo(parameter);
			Assert.assertEquals(parameter.data.get("berlin"), "value");
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testApplyToWithOptionAndDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		RequiredParameters required = new RequiredParameters();
		try {
			required.add(new Option("berlin").defaultValue("value"));
			parameter = required.applyTo(parameter);
			Assert.assertEquals(parameter.data.get("berlin"), "value");
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testApplyToWithOptionWithLongAndShortNameAndDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		RequiredParameters required = new RequiredParameters();
		try {
			required.add(new Option("berlin").alt("b").defaultValue("value"));
			parameter = required.applyTo(parameter);
			Assert.assertEquals(parameter.data.get("berlin"), "value");
			Assert.assertEquals(parameter.data.get("b"), "value");
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testApplyToWithOptionMultipleOptionsAndOneDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--input", "abc"});
		RequiredParameters rq = new RequiredParameters();
		try {
			rq.add("input");
			rq.add(new Option("parallelism").alt("p").defaultValue("1").type(OptionType.INTEGER));
			parameter = rq.applyTo(parameter);
			Assert.assertEquals(parameter.data.get("parallelism"), "1");
			Assert.assertEquals(parameter.data.get("p"), "1");
			Assert.assertEquals(parameter.data.get("input"), "abc");
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testApplyToWithMultipleTypes() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{});
		RequiredParameters required = new RequiredParameters();
		try {
			required.add(new Option("berlin").defaultValue("value"));
			required.add(new Option("count").defaultValue("15"));
			required.add(new Option("someFlag").alt("sf").defaultValue("true"));

			parameter = required.applyTo(parameter);

			Assert.assertEquals(parameter.data.get("berlin"), "value");
			Assert.assertEquals(parameter.data.get("count"), "15");
			Assert.assertEquals(parameter.data.get("someFlag"), "true");
			Assert.assertEquals(parameter.data.get("sf"), "true");

		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testPrintHelpForFullySetOption() {
		RequiredParameters required = new RequiredParameters();
		try {
			required.add(new Option("option").defaultValue("some").help("help").alt("o").choices("some", "options"));

			String helpText = required.getHelp();
			Assert.assertThat(helpText, CoreMatchers.allOf(
					containsString("Required Parameters:"),
					containsString("-o, --option"),
					containsString("default: some"),
					containsString("choices: "),
					containsString("some"),
					containsString("options")));

		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testPrintHelpForMultipleParams() {
		RequiredParameters required = new RequiredParameters();
		try {
			required.add("input");
			required.add("output");
			required.add(new Option("parallelism").alt("p").help("Set the parallelism for all operators").type(OptionType.INTEGER));

			String helpText = required.getHelp();
			Assert.assertThat(helpText, CoreMatchers.allOf(
					containsString("Required Parameters:"),
					containsString("--input"),
					containsString("--output"),
					containsString("-p, --parallelism"),
					containsString("Set the parallelism for all operators")));

			Assert.assertThat(helpText, CoreMatchers.allOf(
					not(containsString("choices")),
					not(containsString("default"))));
		} catch (RequiredParametersException e) {
			fail("Exception thrown " + e.getMessage());
		}
	}

	@Test
	public void testPrintHelpWithMissingParams() {
		RequiredParameters required = new RequiredParameters();

		String helpText = required.getHelp(Arrays.asList("param1", "param2", "paramN"));
		Assert.assertThat(helpText, CoreMatchers.allOf(
				containsString("Missing arguments for:"),
				containsString("param1 "),
				containsString("param2 "),
				containsString("paramN ")));
	}
}
