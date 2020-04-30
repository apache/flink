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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link StringParameter}.
 */
public class StringParameterTest
extends ParameterTestBase {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private StringParameter parameter;

	@Before
	public void setup() {
		super.setup();

		parameter = new StringParameter(owner, "test");
	}

	@Test
	public void testWithDefaultWithParameter() {
		parameter.setDefaultValue("Flink");
		Assert.assertEquals("[--test TEST]", parameter.getUsage());

		parameter.configure(ParameterTool.fromArgs(new String[]{"--test", "Gelly"}));
		Assert.assertEquals("Gelly", parameter.getValue());
	}

	@Test
	public void testWithDefaultWithoutParameter() {
		parameter.setDefaultValue("Flink");
		Assert.assertEquals("[--test TEST]", parameter.getUsage());

		parameter.configure(ParameterTool.fromArgs(new String[]{}));
		Assert.assertEquals("Flink", parameter.getValue());
	}

	@Test
	public void testWithoutDefaultWithParameter() {
		Assert.assertEquals("--test TEST", parameter.getUsage());

		parameter.configure(ParameterTool.fromArgs(new String[]{"--test", "Gelly"}));
		Assert.assertEquals("Gelly", parameter.getValue());
	}

	@Test
	public void testWithoutDefaultWithoutParameter() {
		Assert.assertEquals("--test TEST", parameter.getUsage());

		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("No data for required key 'test'");

		parameter.configure(ParameterTool.fromArgs(new String[]{}));
	}
}
