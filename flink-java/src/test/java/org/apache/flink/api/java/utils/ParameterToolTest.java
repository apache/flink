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

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Tests for {@link ParameterTool}.
 */
public class ParameterToolTest extends AbstractParameterToolTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	// ----- Parser tests -----------------

	@Test(expected = RuntimeException.class)
	public void testIllegalArgs() {
		ParameterTool.fromArgs(new String[]{"berlin"});
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
		ParameterTool.fromArgs(new String[]{"--a", "-b", "--"});
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyValShort() {
		ParameterTool.fromArgs(new String[]{"--a", "-b", "-"});
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

	// Boolean

	@Test
	public void testUnrequestedBoolean() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean", "true"});
		Assert.assertEquals(Sets.newHashSet("boolean"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertTrue(parameter.getBoolean("boolean"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertTrue(parameter.getBoolean("boolean"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedBooleanWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean", "true"});
		Assert.assertEquals(Sets.newHashSet("boolean"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertTrue(parameter.getBoolean("boolean", false));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertTrue(parameter.getBoolean("boolean", false));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedBooleanWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean"});
		Assert.assertEquals(Sets.newHashSet("boolean"), parameter.getUnrequestedParameters());

		parameter.getBoolean("boolean");
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	// Byte

	@Test
	public void testUnrequestedByte() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-byte", "1"});
		Assert.assertEquals(Sets.newHashSet("byte"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(1, parameter.getByte("byte"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(1, parameter.getByte("byte"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedByteWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-byte", "1"});
		Assert.assertEquals(Sets.newHashSet("byte"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(1, parameter.getByte("byte", (byte) 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(1, parameter.getByte("byte", (byte) 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedByteWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-byte"});
		Assert.assertEquals(Sets.newHashSet("byte"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getByte("byte");
	}

	// Short

	@Test
	public void testUnrequestedShort() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-short", "2"});
		Assert.assertEquals(Sets.newHashSet("short"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(2, parameter.getShort("short"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(2, parameter.getShort("short"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedShortWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-short", "2"});
		Assert.assertEquals(Sets.newHashSet("short"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(2, parameter.getShort("short", (short) 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(2, parameter.getShort("short", (short) 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedShortWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-short"});
		Assert.assertEquals(Sets.newHashSet("short"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getShort("short");
	}

	// Int

	@Test
	public void testUnrequestedInt() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-int", "4"});
		Assert.assertEquals(Sets.newHashSet("int"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(4, parameter.getInt("int"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(4, parameter.getInt("int"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedIntWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-int", "4"});
		Assert.assertEquals(Sets.newHashSet("int"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(4, parameter.getInt("int", 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(4, parameter.getInt("int", 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedIntWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-int"});
		Assert.assertEquals(Sets.newHashSet("int"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getInt("int");
	}

	// Long

	@Test
	public void testUnrequestedLong() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-long", "8"});
		Assert.assertEquals(Sets.newHashSet("long"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(8, parameter.getLong("long"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(8, parameter.getLong("long"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedLongWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-long", "8"});
		Assert.assertEquals(Sets.newHashSet("long"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(8, parameter.getLong("long", 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(8, parameter.getLong("long", 0));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedLongWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-long"});
		Assert.assertEquals(Sets.newHashSet("long"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getLong("long");
	}

	// Float

	@Test
	public void testUnrequestedFloat() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-float", "4"});
		Assert.assertEquals(Sets.newHashSet("float"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedFloatWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-float", "4"});
		Assert.assertEquals(Sets.newHashSet("float"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(4.0, parameter.getFloat("float", 0.0f), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(4.0, parameter.getFloat("float", 0.0f), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedFloatWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-float"});
		Assert.assertEquals(Sets.newHashSet("float"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getFloat("float");
	}

	// Double

	@Test
	public void testUnrequestedDouble() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-double", "8"});
		Assert.assertEquals(Sets.newHashSet("double"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedDoubleWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-double", "8"});
		Assert.assertEquals(Sets.newHashSet("double"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals(8.0, parameter.getDouble("double", 0.0), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals(8.0, parameter.getDouble("double", 0.0), 0.00001);
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedDoubleWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-double"});
		Assert.assertEquals(Sets.newHashSet("double"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getDouble("double");
	}

	// String

	@Test
	public void testUnrequestedString() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-string", "∞"});
		Assert.assertEquals(Sets.newHashSet("string"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals("∞", parameter.get("string"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals("∞", parameter.get("string"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedStringWithDefaultValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-string", "∞"});
		Assert.assertEquals(Sets.newHashSet("string"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals("∞", parameter.get("string", "0.0"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals("∞", parameter.get("string", "0.0"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedStringWithMissingValue() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-string"});
		Assert.assertEquals(Sets.newHashSet("string"), parameter.getUnrequestedParameters());

		parameter.get("string");
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	// Additional methods

	@Test
	public void testUnrequestedHas() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean"});
		Assert.assertEquals(Sets.newHashSet("boolean"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertTrue(parameter.has("boolean"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertTrue(parameter.has("boolean"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedRequired() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-required", "∞"});
		Assert.assertEquals(Sets.newHashSet("required"), parameter.getUnrequestedParameters());

		// test parameter access
		Assert.assertEquals("∞", parameter.getRequired("required"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		// test repeated access
		Assert.assertEquals("∞", parameter.getRequired("required"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedMultiple() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean", "true", "-byte", "1",
			"-short", "2", "-int", "4", "-long", "8", "-float", "4.0", "-double", "8.0", "-string", "∞"});
		Assert.assertEquals(Sets.newHashSet("boolean", "byte", "short", "int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertTrue(parameter.getBoolean("boolean"));
		Assert.assertEquals(Sets.newHashSet("byte", "short", "int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(1, parameter.getByte("byte"));
		Assert.assertEquals(Sets.newHashSet("short", "int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(2, parameter.getShort("short"));
		Assert.assertEquals(Sets.newHashSet("int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(4, parameter.getInt("int"));
		Assert.assertEquals(Sets.newHashSet("long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(8, parameter.getLong("long"));
		Assert.assertEquals(Sets.newHashSet("float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
		Assert.assertEquals(Sets.newHashSet("double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
		Assert.assertEquals(Sets.newHashSet("string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals("∞", parameter.get("string"));
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	@Test
	public void testUnrequestedUnknown() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{});
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

		Assert.assertTrue(parameter.getBoolean("boolean", true));
		Assert.assertEquals(0, parameter.getByte("byte", (byte) 0));
		Assert.assertEquals(0, parameter.getShort("short", (short) 0));
		Assert.assertEquals(0, parameter.getInt("int", 0));
		Assert.assertEquals(0, parameter.getLong("long", 0));
		Assert.assertEquals(0, parameter.getFloat("float", 0), 0.00001);
		Assert.assertEquals(0, parameter.getDouble("double", 0), 0.00001);
		Assert.assertEquals("0", parameter.get("string", "0"));

		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}
}
