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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ParameterTool}.
 */
public class ParameterToolTest extends AbstractParameterToolTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	// ----- Parser tests -----------------

	@Test
	public void testThrowExceptionIfParameterIsNotPrefixed() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Error parsing arguments '[a]' on 'a'. Please prefix keys with -- or -.");

		ParameterTool.fromArgs(new String[]{"a"});
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

	@Test
	public void testEmptyVal() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("The input [--a, -b, --] contains an empty argument");

		ParameterTool.fromArgs(new String[]{"--a", "-b", "--"});
	}

	@Test
	public void testEmptyValShort() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("The input [--a, -b, -] contains an empty argument");

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

		parameter = ParameterTool.fromPropertiesFile(propertiesFile);
		Assert.assertEquals(2, parameter.getNumberOfParameters());
		validate(parameter);

		try (FileInputStream fis = new FileInputStream(propertiesFile)) {
			parameter = ParameterTool.fromPropertiesFile(fis);
		}
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

	// Boolean

	@Test
	public void testUnrequestedBoolean() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean", "true"});
		Assert.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

		parameter.getBoolean("boolean");
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	// Byte

	@Test
	public void testUnrequestedByte() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-byte", "1"});
		Assert.assertEquals(createHashSet("byte"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("byte"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("byte"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getByte("byte");
	}

	// Short

	@Test
	public void testUnrequestedShort() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-short", "2"});
		Assert.assertEquals(createHashSet("short"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("short"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("short"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getShort("short");
	}

	// Int

	@Test
	public void testUnrequestedInt() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-int", "4"});
		Assert.assertEquals(createHashSet("int"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("int"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("int"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getInt("int");
	}

	// Long

	@Test
	public void testUnrequestedLong() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-long", "8"});
		Assert.assertEquals(createHashSet("long"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("long"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("long"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getLong("long");
	}

	// Float

	@Test
	public void testUnrequestedFloat() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-float", "4"});
		Assert.assertEquals(createHashSet("float"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("float"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("float"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getFloat("float");
	}

	// Double

	@Test
	public void testUnrequestedDouble() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-double", "8"});
		Assert.assertEquals(createHashSet("double"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("double"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("double"), parameter.getUnrequestedParameters());

		exception.expect(RuntimeException.class);
		exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

		parameter.getDouble("double");
	}

	// String

	@Test
	public void testUnrequestedString() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-string", "∞"});
		Assert.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

		parameter.get("string");
		Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
	}

	// Additional methods

	@Test
	public void testUnrequestedHas() {
		ParameterTool parameter = ParameterTool.fromArgs(new String[]{"-boolean"});
		Assert.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("required"), parameter.getUnrequestedParameters());

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
		Assert.assertEquals(createHashSet("boolean", "byte", "short", "int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertTrue(parameter.getBoolean("boolean"));
		Assert.assertEquals(createHashSet("byte", "short", "int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(1, parameter.getByte("byte"));
		Assert.assertEquals(createHashSet("short", "int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(2, parameter.getShort("short"));
		Assert.assertEquals(createHashSet("int", "long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(4, parameter.getInt("int"));
		Assert.assertEquals(createHashSet("long", "float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(8, parameter.getLong("long"));
		Assert.assertEquals(createHashSet("float", "double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
		Assert.assertEquals(createHashSet("double", "string"),
			parameter.getUnrequestedParameters());

		Assert.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
		Assert.assertEquals(createHashSet("string"),
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

	/**
	 * Tests that we can concurrently serialize and access the ParameterTool. See FLINK-7943
	 */
	@Test
	public void testConcurrentExecutionConfigSerialization() throws ExecutionException, InterruptedException {

		final int numInputs = 10;
		Collection<String> input = new ArrayList<>(numInputs);

		for (int i = 0; i < numInputs; i++) {
			input.add("--" + UUID.randomUUID());
			input.add(UUID.randomUUID().toString());
		}

		final String[] args = input.toArray(new String[0]);

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		final int numThreads = 5;
		final int numSerializations = 100;

		final Collection<CompletableFuture<Void>> futures = new ArrayList<>(numSerializations);

		final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

		try {
			for (int i = 0; i < numSerializations; i++) {
				futures.add(
					CompletableFuture.runAsync(
						() -> {
							try {
								serializeDeserialize(parameterTool);
							} catch (Exception e) {
								throw new CompletionException(e);
							}
						},
						executorService));
			}

			for (CompletableFuture<Void> future : futures) {
				future.get();
			}
		} finally {
			executorService.shutdownNow();
			executorService.awaitTermination(1000L, TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Accesses parameter tool parameters and then serializes the given parameter tool and deserializes again.
	 * @param parameterTool to serialize/deserialize
	 */
	private void serializeDeserialize(ParameterTool parameterTool) throws IOException, ClassNotFoundException {
		// weirdly enough, this call has side effects making the ParameterTool serialization fail if not
		// using a concurrent data structure.
		parameterTool.get(UUID.randomUUID().toString());

		try (
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeObject(parameterTool);
			oos.close();
			baos.close();

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);

			// this should work :-)
			ParameterTool deserializedParameterTool = ((ParameterTool) ois.readObject());
		}
	}

	private static <T> Set<T> createHashSet(T... elements) {
		Set<T> set = new HashSet<>();
		for (T element : elements) {
			set.add(element);
		}
		return set;
	}
}
