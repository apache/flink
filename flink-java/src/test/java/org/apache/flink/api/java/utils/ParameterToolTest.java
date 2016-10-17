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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

public class ParameterToolTest extends AbstractParameterToolTest {

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
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"--input", "myInput", "-expectedCount", "15", "--withoutValues",
				"--negativeFloat", "-0.58", "-isWorking", "true", "--maxByte", "127", "-negativeShort", "-1024"});
		Assert.assertEquals(7, parameter.getNumberOfParameters());
		validate(parameter);
		Assert.assertTrue(parameter.has("withoutValues"));
		Assert.assertEquals(-0.58, parameter.getFloat("negativeFloat"), 0.1);
		Assert.assertTrue(parameter.getBoolean("isWorking"));
		Assert.assertEquals(127, parameter.getByte("maxByte"));
		Assert.assertEquals(-1024, parameter.getShort("negativeShort"));
	}

	@Test
	public void testFromPropertiesFile() throws IOException {
		File propertiesFile = tmp.newFile();
		Properties props = new Properties();
		props.setProperty("input", "myInput");
		props.setProperty("expectedCount", "15");
		try (final OutputStream out = new FileOutputStream(propertiesFile)) {
			props.store(out, "Test properties");
		}
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

	@Test
	public void testFromGenericOptionsParser() throws IOException {
		ParameterTool parameter = ParameterTool.fromGenericOptionsParser(new String[]{"-D", "input=myInput", "-DexpectedCount=15"});
		validate(parameter);
	}
}
