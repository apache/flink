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

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
		Assert.assertTrue(checkCompatibility(ENUM_A, ENUM_A).isCompatibleAsIs());
	}

	/**
	 * Check that appending fields to the enum does not require migration
	 */
	@Test
	public void checkAppendedField() throws Exception {
		Assert.assertTrue(checkCompatibility(ENUM_A, ENUM_B).isCompatibleWithReconfiguredSerializer());
	}

	/**
	 * Check that removing enum fields makes the snapshot incompatible
	 */
	@Test(expected = IllegalStateException.class)
	public void removingFieldShouldBeIncompatible() throws Exception {
		Assert.assertTrue(checkCompatibility(ENUM_A, ENUM_C).isIncompatible());
	}

	/**
	 * Check that changing the enum field order don't require migration
	 */
	@Test
	public void checkDifferentFieldOrder() throws Exception {
		Assert.assertTrue(checkCompatibility(ENUM_A, ENUM_D).isCompatibleWithReconfiguredSerializer());
	}

	@SuppressWarnings("unchecked")
	private static TypeSerializerSchemaCompatibility checkCompatibility(String enumSourceA, String enumSourceB)
		throws IOException, ClassNotFoundException {

		ClassLoader classLoader = ClassLoaderUtils.compileAndLoadJava(
			temporaryFolder.newFolder(), ENUM_NAME + ".java", enumSourceA);

		EnumSerializer enumSerializer = new EnumSerializer(classLoader.loadClass(ENUM_NAME));

		TypeSerializerSnapshot snapshot = enumSerializer.snapshotConfiguration();
		byte[] snapshotBytes;
		try (
			ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outBuffer)) {

			TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
				outputViewStreamWrapper, snapshot, enumSerializer);
			snapshotBytes = outBuffer.toByteArray();
		}

		ClassLoader classLoader2 = ClassLoaderUtils.compileAndLoadJava(
			temporaryFolder.newFolder(), ENUM_NAME + ".java", enumSourceB);

		TypeSerializerSnapshot restoredSnapshot;
		try (
			ByteArrayInputStream inBuffer = new ByteArrayInputStream(snapshotBytes);
			DataInputViewStreamWrapper inputViewStreamWrapper = new DataInputViewStreamWrapper(inBuffer)) {

			restoredSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
				inputViewStreamWrapper, classLoader2, enumSerializer);
		}

		EnumSerializer enumSerializer2 = new EnumSerializer(classLoader2.loadClass(ENUM_NAME));
		return restoredSnapshot.resolveSchemaCompatibility(enumSerializer2);
	}
}
