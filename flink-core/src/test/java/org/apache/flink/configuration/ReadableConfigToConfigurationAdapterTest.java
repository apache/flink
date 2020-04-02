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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ReadableConfigToConfigurationAdapter}.
 */
public class ReadableConfigToConfigurationAdapterTest {
	/**
	 * Simple enum for testing.
	 */
	public enum TestEnum {
		A,
		B;
	}

	@Test
	public void testGettingEnum() {
		String enumKey = "enumKey";
		Configuration configuration = new Configuration();
		configuration.setString(enumKey, "A");

		ReadableConfigToConfigurationAdapter readableConfig = new ReadableConfigToConfigurationAdapter(configuration);
		TestEnum actual = readableConfig.getEnum(TestEnum.class, ConfigOptions.key(enumKey).stringType().defaultValue("B"));
		assertThat(actual, equalTo(TestEnum.A));
	}

	@Test
	public void testGettingClass() throws ClassNotFoundException {
		String classKey = "classKey";
		Configuration configuration = new Configuration();
		configuration.setString(classKey, TestClassB.class.getCanonicalName());

		ReadableConfigToConfigurationAdapter readableConfig = new ReadableConfigToConfigurationAdapter(configuration);
		Class<TestInterface> actual = readableConfig.getClass(
			classKey,
			TestClassA.class,
			this.getClass().getClassLoader());
		assertThat(actual, equalTo(TestClassB.class));
	}

	@Test
	public void testPrimitiveGetters() {
		Configuration configuration = new Configuration();
		configuration.setString("int", String.valueOf(Integer.MAX_VALUE));
		configuration.setString("long", String.valueOf(Long.MAX_VALUE));
		configuration.setString("boolean", String.valueOf(false));
		configuration.setString("string", "XYZ");
		configuration.setString("double", String.valueOf(Double.MAX_VALUE));
		configuration.setString("float", String.valueOf(Float.MAX_VALUE));

		ReadableConfigToConfigurationAdapter readableConfig = new ReadableConfigToConfigurationAdapter(configuration);
		assertThat(readableConfig.getInteger("int", 0), equalTo(Integer.MAX_VALUE));
		assertThat(readableConfig.getLong("long", 0L), equalTo(Long.MAX_VALUE));
		assertThat(readableConfig.getBoolean("boolean", true), equalTo(false));
		assertThat(readableConfig.getString("string", "ABC"), equalTo("XYZ"));
		assertThat(readableConfig.getDouble("double", 0D), equalTo(Double.MAX_VALUE));
		assertThat(readableConfig.getFloat("float", 0F), equalTo(Float.MAX_VALUE));
	}

	@Test
	public void testPrimitiveOptionGetters() {
		Configuration configuration = new Configuration();
		configuration.setString("int", String.valueOf(Integer.MAX_VALUE));
		configuration.setString("long", String.valueOf(Long.MAX_VALUE));
		configuration.setString("boolean", String.valueOf(false));
		configuration.setString("string", "XYZ");
		configuration.setString("double", String.valueOf(Double.MAX_VALUE));
		configuration.setString("float", String.valueOf(Float.MAX_VALUE));

		ReadableConfigToConfigurationAdapter readableConfig = new ReadableConfigToConfigurationAdapter(configuration);
		assertThat(readableConfig.get(ConfigOptions.key("int").intType().defaultValue(0)), equalTo(Integer.MAX_VALUE));
		assertThat(readableConfig.get(ConfigOptions.key("long").longType().defaultValue(0L)), equalTo(Long.MAX_VALUE));
		assertThat(readableConfig.get(ConfigOptions.key("boolean").booleanType().defaultValue(true)), equalTo(false));
		assertThat(readableConfig.get(ConfigOptions.key("string").stringType().defaultValue("ABC")), equalTo("XYZ"));
		assertThat(
			readableConfig.get(ConfigOptions.key("double").doubleType().defaultValue(0D)),
			equalTo(Double.MAX_VALUE));
		assertThat(
			readableConfig.get(ConfigOptions.key("float").floatType().defaultValue(0F)),
			equalTo(Float.MAX_VALUE));
	}
}

interface TestInterface {}

class TestClassA implements TestInterface {}

class TestClassB implements TestInterface {}
