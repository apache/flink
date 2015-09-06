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

import static org.hamcrest.CoreMatchers.containsString;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for RequiredParameter class and its interactions with ParameterTool
 */
public class RequiredParametersTest {

	@Test(expected = RequiredParametersException.class)
	public void testAddWithAlreadyExistingParameter() throws RequiredParametersException {
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin"));
		required.add(new Option("berlin"));
	}

	@Test(expected = RequiredParametersException.class)
	public void testStringBasedAddWithAlreadyExistingParameter() throws RequiredParametersException {
		RequiredParameters required = new RequiredParameters();
		required.add("berlin");
		required.add("berlin");
	}

	@Test(expected = RequiredParametersException.class)
	public void testApplyToWithMissingParameters() throws RequiredParametersException {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("munich"));

		required.applyTo(parameter);
	}

	@Test(expected = RequiredParametersException.class)
	public void testApplyToWithMissingDefaultValues() throws RequiredParametersException {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin"));

		required.applyTo(parameter);
	}

	@Test(expected = RequiredParametersException.class)
	public void testApplyToWithInvalidParameterValue() throws RequiredParametersException {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin", "river"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin").choices("city", "metropolis"));

		required.applyTo(parameter);
	}

	@Test(expected = RequiredParametersException.class)
	public void testApplyToWithNonCastableType() throws RequiredParametersException {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--flag", "15"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("flag").type(OptionType.BOOLEAN));

		required.applyTo(parameter);
	}

	@Test
	public void testApplyToWithValidParameter() throws RequiredParametersException {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin").defaultValue("value"));

		required.applyTo(parameter);

		Assert.assertEquals(parameter.data.get("berlin"), "value");
	}

	@Test
	public void testApplyToWithMultipleTypes() throws RequiredParametersException {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin", "--count", "--someFlag"});
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("berlin").defaultValue("value"));
		required.add(new Option("count").defaultValue("15"));
		required.add(new Option("someFlag").defaultValue("true"));

		required.applyTo(parameter);

		Assert.assertEquals(parameter.data.get("berlin"), "value");
		Assert.assertEquals(parameter.data.get("count"), "15");
		Assert.assertEquals(parameter.data.get("someFlag"), "true");
	}

	@Test
	public void testPrintHelp() throws RequiredParametersException {
		RequiredParameters required = new RequiredParameters();
		required.add(new Option("option").defaultValue("some stuff").help("useful text").alt("o"));

		String helpText = required.getHelp();
		Assert.assertThat(helpText, CoreMatchers.allOf(
				containsString("option"),
				containsString("o"),
				containsString("some stuff"),
				containsString("useful text")));
	}
}
