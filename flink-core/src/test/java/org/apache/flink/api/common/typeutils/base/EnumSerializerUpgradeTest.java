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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

public class EnumSerializerUpgradeTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String ENUM_NAME = "EnumSerializerUpgradeTestEnum";

	private static final String ENUM_A = "public enum " + ENUM_NAME + " { A, B, C }";
	private static final String ENUM_B = "public enum " + ENUM_NAME + " { A, B, C, D }";
	private static final String ENUM_C = "public enum " + ENUM_NAME + " { A, C }";
	private static final String ENUM_D = "public enum " + ENUM_NAME + " { A, C, B }";

	/**
	 * Check that identical enums don't require migration
	 */
	@Test
	public void checkIndenticalEnums() throws Exception {
		Assert.assertFalse(checkCompatibility(ENUM_A, ENUM_A).isRequiresMigration());
	}

	/**
	 * Check that appending fields to the enum does not require migration
	 */
	@Test
	public void checkAppendedField() throws Exception {
		Assert.assertFalse(checkCompatibility(ENUM_A, ENUM_B).isRequiresMigration());
	}

	/**
	 * Check that removing enum fields requires migration
	 */
	@Test
	public void checkRemovedField() throws Exception {
		Assert.assertTrue(checkCompatibility(ENUM_A, ENUM_C).isRequiresMigration());
	}

	/**
	 * Check that changing the enum field order don't require migration
	 */
	@Test
	public void checkDifferentFieldOrder() throws Exception {
		Assert.assertFalse(checkCompatibility(ENUM_A, ENUM_D).isRequiresMigration());
	}

	@SuppressWarnings("unchecked")
	private static CompatibilityResult checkCompatibility(String enumSourceA, String enumSourceB)
		throws IOException, ClassNotFoundException {

		ClassLoader classLoader = compileAndLoadEnum(
			temporaryFolder.newFolder(), ENUM_NAME + ".java", enumSourceA);

		EnumSerializer enumSerializer = new EnumSerializer(classLoader.loadClass(ENUM_NAME));

		TypeSerializerConfigSnapshot snapshot = enumSerializer.snapshotConfiguration();
		byte[] snapshotBytes;
		try (
			ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outBuffer)) {

			TypeSerializerSerializationUtil.writeSerializerConfigSnapshot(outputViewStreamWrapper, snapshot);
			snapshotBytes = outBuffer.toByteArray();
		}

		ClassLoader classLoader2 = compileAndLoadEnum(
			temporaryFolder.newFolder(), ENUM_NAME + ".java", enumSourceB);

		TypeSerializerConfigSnapshot restoredSnapshot;
		try (
			ByteArrayInputStream inBuffer = new ByteArrayInputStream(snapshotBytes);
			DataInputViewStreamWrapper inputViewStreamWrapper = new DataInputViewStreamWrapper(inBuffer)) {

			restoredSnapshot = TypeSerializerSerializationUtil.readSerializerConfigSnapshot(inputViewStreamWrapper, classLoader2);
		}

		EnumSerializer enumSerializer2 = new EnumSerializer(classLoader2.loadClass(ENUM_NAME));
		return enumSerializer2.ensureCompatibility(restoredSnapshot);
	}

	private static ClassLoader compileAndLoadEnum(File root, String filename, String source) throws IOException {
		File file = writeSourceFile(root, filename, source);

		compileClass(file);

		return new URLClassLoader(
			new URL[]{root.toURI().toURL()},
			Thread.currentThread().getContextClassLoader());
	}

	private static File writeSourceFile(File root, String filename, String source) throws IOException {
		File file = new File(root, filename);
		FileWriter fileWriter = new FileWriter(file);

		fileWriter.write(source);
		fileWriter.close();

		return file;
	}

	private static int compileClass(File sourceFile) {
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		return compiler.run(null, null, null, "-proc:none", sourceFile.getPath());
	}
}
