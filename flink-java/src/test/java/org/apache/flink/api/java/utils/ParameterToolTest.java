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

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class ParameterToolTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	// ----- Parser tests -----------------

	@Test(expected = RuntimeException.class)
	public void testIllegalArgs() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"berlin"});
		Assert.assertEquals(0, parameter.getNumberOfParameters());
	}

	@Test
	public void testNoVal() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-berlin"});
		Assert.assertEquals(1, parameter.getNumberOfParameters());
		Assert.assertTrue(parameter.has("berlin"));
	}

	@Test
	public void testNoValDouble() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--berlin"});
		Assert.assertEquals(1, parameter.getNumberOfParameters());
		Assert.assertTrue(parameter.has("berlin"));
	}

	@Test
	public void testMultipleNoVal() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--a", "--b", "--c", "--d", "--e", "--f"});
		Assert.assertEquals(6, parameter.getNumberOfParameters());
		Assert.assertTrue(parameter.has("a"));
		Assert.assertTrue(parameter.has("b"));
		Assert.assertTrue(parameter.has("c"));
		Assert.assertTrue(parameter.has("d"));
		Assert.assertTrue(parameter.has("e"));
		Assert.assertTrue(parameter.has("f"));
	}

	@Test
	public void testMultipleNoValMixed() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--a", "-b", "-c", "-d", "--e", "--f"});
		Assert.assertEquals(6, parameter.getNumberOfParameters());
		Assert.assertTrue(parameter.has("a"));
		Assert.assertTrue(parameter.has("b"));
		Assert.assertTrue(parameter.has("c"));
		Assert.assertTrue(parameter.has("d"));
		Assert.assertTrue(parameter.has("e"));
		Assert.assertTrue(parameter.has("f"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyVal() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--a", "-b", "--"});
		Assert.assertEquals(2, parameter.getNumberOfParameters());
		Assert.assertTrue(parameter.has("a"));
		Assert.assertTrue(parameter.has("b"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyValShort() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--a", "-b", "-"});
		Assert.assertEquals(2, parameter.getNumberOfParameters());
		Assert.assertTrue(parameter.has("a"));
		Assert.assertTrue(parameter.has("b"));
	}

	@Test
	public void testFromCliArgs() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--input", "myInput", "-expectedCount", "15", "--withoutValues", "--negativeFloat", "-0.58"});
		Assert.assertEquals(4, parameter.getNumberOfParameters());
		validate(parameter);
		Assert.assertTrue(parameter.has("withoutValues"));
		Assert.assertEquals(-0.58, parameter.getFloat("negativeFloat"), 0.1);
	}

	@Test
	public void testFromPropertiesFile() throws IOException {
		File propertiesFile = tmp.newFile();
		Properties props = new Properties();
		props.setProperty("input", "myInput");
		props.setProperty("expectedCount", "15");
		props.store(new FileOutputStream(propertiesFile), "Test properties");
		ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile.getAbsolutePath());
		Assert.assertEquals(2, parameter.getNumberOfParameters());
		validate(parameter);
	}

	@Test
	public void testFromMapOrProperties() {
		Properties props = new Properties();
		props.setProperty("input", "myInput");
		props.setProperty("expectedCount", "15");
		ParameterTool parameter = ParameterTool.fromMap((Map) props);
		Assert.assertEquals(2, parameter.getNumberOfParameters());
		validate(parameter);
	}

	/**
	 * This is mainly meant to be used with -D arguments against the JVM.
	 */
	@Test
	public void testSystemProperties() {
		System.setProperty("input", "myInput");
		System.setProperty("expectedCount", "15");
		ParameterTool parameter = ParameterTool.fromSystemProperties();
		validate(parameter);
	}

	@Test
	public void testMerged() {
		ParameterTool parameter1 = ParameterTool.fromArgs(new String[]{"--input", "myInput"});
		System.setProperty("expectedCount", "15");
		ParameterTool parameter2 = ParameterTool.fromSystemProperties();
		ParameterTool parameter = parameter1.mergeWith(parameter2);
		validate(parameter);
	}

	private void validate(ParameterTool parameter) {
		ClosureCleaner.ensureSerializable(parameter);
		Assert.assertEquals("myInput", parameter.getRequired("input"));
		Assert.assertEquals("myDefaultValue", parameter.get("output", "myDefaultValue"));
		Assert.assertEquals(null, parameter.get("whatever"));
		Assert.assertEquals(15L, parameter.getLong("expectedCount", -1L));

		Configuration config = parameter.getConfiguration();
		Assert.assertEquals(15L, config.getLong("expectedCount", -1L));

		Properties props = parameter.getProperties();
		Assert.assertEquals("myInput", props.getProperty("input"));
		props = null;

		// -------- test the default file creation ------------
		try {
			String pathToFile = tmp.newFile().getAbsolutePath();
			parameter.createPropertiesFile(pathToFile);
			Properties defaultProps = new Properties();
			defaultProps.load(new FileInputStream(pathToFile));

			Assert.assertEquals("myDefaultValue", defaultProps.get("output"));
			Assert.assertEquals("-1", defaultProps.get("expectedCount"));
			Assert.assertTrue(defaultProps.containsKey("input"));

		} catch (IOException e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}
}
