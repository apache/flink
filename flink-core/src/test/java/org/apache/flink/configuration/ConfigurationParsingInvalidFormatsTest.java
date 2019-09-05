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

import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Collections;

/**
 * Tests for reading configuration parameters with invalid formats.
 */
@RunWith(Parameterized.class)
public class ConfigurationParsingInvalidFormatsTest extends TestLogger {
	@Parameterized.Parameters(name = "option: {0}, invalidString: {1}")
	public static Object[][] getSpecs() {
		return new Object[][]{
			new Object[]{ConfigOptions.key("int").intType().defaultValue(1), "ABC"},
			new Object[]{ConfigOptions.key("long").longType().defaultValue(1L), "ABC"},
			new Object[]{ConfigOptions.key("float").floatType().defaultValue(1F), "ABC"},
			new Object[]{ConfigOptions.key("double").doubleType().defaultValue(1D), "ABC"},
			new Object[]{ConfigOptions.key("boolean").booleanType().defaultValue(true), "ABC"},
			new Object[]{ConfigOptions.key("memory").memoryType().defaultValue(MemorySize.parse("1kB")), "ABC"},
			new Object[]{ConfigOptions.key("duration").durationType().defaultValue(Duration.ofSeconds(1)), "ABC"},
			new Object[]{ConfigOptions.key("enum").enumType(TestEnum.class).defaultValue(TestEnum.ENUM1), "ABC"},
			new Object[]{ConfigOptions.key("map").mapType().defaultValue(Collections.emptyMap()), "ABC"},
			new Object[]{ConfigOptions.key("list<int>").intType().asList().defaultValues(1, 2), "A;B;C"},
			new Object[]{ConfigOptions.key("list<string>").stringType().asList().defaultValues("A"), "'A;B;C"}
		};
	}

	@Parameterized.Parameter
	public ConfigOption<?> option;

	@Parameterized.Parameter(value = 1)
	public String invalidString;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInvalidStringParsingWithGetOptional() {
		Configuration configuration = new Configuration();
		configuration.setString(option.key(), invalidString);

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(String.format("Could not parse value '%s' for key '%s'", invalidString, option.key()));
		configuration.getOptional(option);
	}

	@Test
	public void testInvalidStringParsingWithGet() {
		Configuration configuration = new Configuration();
		configuration.setString(option.key(), invalidString);

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(String.format("Could not parse value '%s' for key '%s'", invalidString, option.key()));
		configuration.get(option);
	}

	private enum TestEnum {
		ENUM1,
		ENUM2
	}
}
