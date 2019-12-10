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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 * A test base for testing {@link TypeSerializer} upgrades.
 * <p>
 * You can run {@link #generateTestSetupFiles()} on a Flink branch to (re-)generate the test data
 * files.
 */
public abstract class TypeSerializerUpgradeTestBase<PreviousElementT, UpgradedElementT> extends TestLogger {

	protected static final MigrationVersion[] migrationVersions = new MigrationVersion[]{
			MigrationVersion.v1_7,
			MigrationVersion.v1_8,
			MigrationVersion.v1_9,
	};

	private final TestSpecification<PreviousElementT, UpgradedElementT> testSpecification;

	protected TypeSerializerUpgradeTestBase(TestSpecification<PreviousElementT, UpgradedElementT> testSpecification) {
		this.testSpecification = checkNotNull(testSpecification);
	}

	// ------------------------------------------------------------------------------
	//  APIs
	// ------------------------------------------------------------------------------

	public interface PreUpgradeSetup<PreviousElementT> {
		TypeSerializer<PreviousElementT> createPriorSerializer();
		PreviousElementT createTestData();
	}

	public interface UpgradeVerifier<UpgradedElementT> {
		TypeSerializer<UpgradedElementT> createUpgradedSerializer();
		UpgradedElementT expectedTestData();
		Matcher<TypeSerializerSchemaCompatibility<UpgradedElementT>> schemaCompatibilityMatcher();
	}

	private static class ClassLoaderSafePreUpgradeSetup<PreviousElementT> implements PreUpgradeSetup<PreviousElementT> {

		private final PreUpgradeSetup<PreviousElementT> delegateSetup;
		private final ClassLoader setupClassloader;

		ClassLoaderSafePreUpgradeSetup(Class<? extends PreUpgradeSetup<PreviousElementT>> delegateSetupClass) throws Exception {
			checkNotNull(delegateSetupClass);
			Class<? extends PreUpgradeSetup<PreviousElementT>> relocatedDelegateSetupClass =
				ClassRelocator.relocate(delegateSetupClass);

			this.setupClassloader = relocatedDelegateSetupClass.getClassLoader();
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(setupClassloader)) {
				this.delegateSetup = relocatedDelegateSetupClass.newInstance();
			}
		}

		@Override
		public TypeSerializer<PreviousElementT> createPriorSerializer() {
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(setupClassloader)) {
				return delegateSetup.createPriorSerializer();
			} catch (IOException e) {
				throw new RuntimeException("Error creating prior serializer via ClassLoaderSafePreUpgradeSetup.", e);
			}
		}

		@Override
		public PreviousElementT createTestData() {
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(setupClassloader)) {
				return delegateSetup.createTestData();
			} catch (IOException e) {
				throw new RuntimeException("Error creating test data via ThreadContextClassLoader.", e);
			}
		}
	}

	private static class ClassLoaderSafeUpgradeVerifier<UpgradedElementT> implements UpgradeVerifier<UpgradedElementT> {

		private final UpgradeVerifier<UpgradedElementT> delegateVerifier;
		private final ClassLoader verifierClassloader;

		ClassLoaderSafeUpgradeVerifier(Class<? extends UpgradeVerifier<UpgradedElementT>> delegateVerifierClass) throws Exception {
			checkNotNull(delegateVerifierClass);
			Class<? extends UpgradeVerifier<UpgradedElementT>> relocatedDelegateVerifierClass =
				ClassRelocator.relocate(delegateVerifierClass);

			this.verifierClassloader = relocatedDelegateVerifierClass.getClassLoader();
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(verifierClassloader)) {
				this.delegateVerifier = relocatedDelegateVerifierClass.newInstance();
			}
		}

		@Override
		public TypeSerializer<UpgradedElementT> createUpgradedSerializer() {
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(verifierClassloader)) {
				return delegateVerifier.createUpgradedSerializer();
			} catch (IOException e) {
				throw new RuntimeException("Error creating upgraded serializer via ClassLoaderSafeUpgradeVerifier.", e);
			}
		}

		@Override
		public UpgradedElementT expectedTestData() {
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(verifierClassloader)) {
				return delegateVerifier.expectedTestData();
			} catch (IOException e) {
				throw new RuntimeException("Error creating expected test data via ClassLoaderSafeUpgradeVerifier.", e);
			}
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<UpgradedElementT>> schemaCompatibilityMatcher() {
			try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(verifierClassloader)) {
				return delegateVerifier.schemaCompatibilityMatcher();
			} catch (IOException e) {
				throw new RuntimeException("Error creating schema compatibility matcher via ClassLoaderSafeUpgradeVerifier.", e);
			}
		}
	}

	public static class TestSpecification<PreviousElementT, UpgradedElementT> {
		private final String name;
		private final MigrationVersion migrationVersion;
		private final ClassLoaderSafePreUpgradeSetup<PreviousElementT> setup;
		private final ClassLoaderSafeUpgradeVerifier<UpgradedElementT> verifier;

		public TestSpecification(
				String name,
				MigrationVersion migrationVersion,
				Class<? extends PreUpgradeSetup<PreviousElementT>> setupClass,
				Class<? extends UpgradeVerifier<UpgradedElementT>> verifierClass) throws Exception {
			this.name = checkNotNull(name);
			this.migrationVersion = checkNotNull(migrationVersion);
			this.setup = new ClassLoaderSafePreUpgradeSetup<>(setupClass);
			this.verifier = new ClassLoaderSafeUpgradeVerifier<>(verifierClass);
		}

		@Override
		public String toString() {
			return name + " / " + migrationVersion;
		}
	}

	// ------------------------------------------------------------------------------
	//  Test file generation
	// ------------------------------------------------------------------------------

	private static final int INITIAL_OUTPUT_BUFFER_SIZE = 64;

	/**
	 * Execute this test to generate test files.
	 * Remember to be using the correct branch when generating the test files,
	 * e.g. to generate test files for {@link MigrationVersion#v1_8}, you should be under the
	 * release-1.8 branch.
	 */
	@Ignore
	@Test
	public void generateTestSetupFiles() throws Exception {
		Files.createDirectory(getSerializerSnapshotFilePath().getParent());

		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(testSpecification.setup.setupClassloader)) {
			TypeSerializer<PreviousElementT> priorSerializer = testSpecification.setup.createPriorSerializer();

			// first, use the serializer to write test data
			// NOTE: it is important that we write test data first, because some serializers' configuration
			//       mutates only after being used for serialization (e.g. dynamic type registrations for Pojo / Kryo)
			DataOutputSerializer testDataOut = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
			priorSerializer.serialize(testSpecification.setup.createTestData(), testDataOut);
			writeContentsTo(getTestDataFilePath(), testDataOut.getCopyOfBuffer());

			// ... then write the serializer snapshot
			DataOutputSerializer serializerSnapshotOut = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
			writeSerializerSnapshot(serializerSnapshotOut, priorSerializer, testSpecification.migrationVersion);
			writeContentsTo(getSerializerSnapshotFilePath(), serializerSnapshotOut.getCopyOfBuffer());
		}
	}

	// ------------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------------

	@Test
	public void restoreSerializerIsValid() throws Exception {
		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
			assumeThat(
				"This test only applies for test specifications that verify an upgraded serializer that is not incompatible.",
				TypeSerializerSchemaCompatibility.incompatible(),
				not(testSpecification.verifier.schemaCompatibilityMatcher()));

			TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot = snapshotUnderTest();

			TypeSerializer<UpgradedElementT> restoredSerializer = restoredSerializerSnapshot.restoreSerializer();
			assertSerializerIsValid(
				restoredSerializer,
				dataUnderTest(),
				testSpecification.verifier.expectedTestData());
		}
	}

	@Test
	public void upgradedSerializerHasExpectedSchemaCompatibility() throws Exception {
		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
			TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot = snapshotUnderTest();
			TypeSerializer<UpgradedElementT> upgradedSerializer = testSpecification.verifier.createUpgradedSerializer();

			TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
				restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);

			assertThat(upgradeCompatibility, testSpecification.verifier.schemaCompatibilityMatcher());
		}
	}

	@Test
	public void upgradedSerializerIsValidAfterMigration() throws Exception {
		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
			TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot = snapshotUnderTest();

			TypeSerializer<UpgradedElementT> upgradedSerializer = testSpecification.verifier.createUpgradedSerializer();

			TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
				restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);
			assumeThat(
				"This test only applies for test specifications that verify an upgraded serializer that requires migration to be compatible.",
				upgradeCompatibility,
				TypeSerializerMatchers.isCompatibleAfterMigration());

			// migrate the previous data schema,
			TypeSerializer<UpgradedElementT> restoreSerializer = restoredSerializerSnapshot.restoreSerializer();
			DataInputView migratedData = readAndThenWriteData(
				dataUnderTest(),
				restoreSerializer,
				upgradedSerializer,
				testSpecification.verifier.expectedTestData());

			// .. and then assert that the upgraded serializer is valid with the migrated data
			assertSerializerIsValid(upgradedSerializer, migratedData, testSpecification.verifier.expectedTestData());
		}
	}

	@Test
	public void upgradedSerializerIsValidAfterReconfiguration() throws Exception {
		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
			TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot = snapshotUnderTest();
			TypeSerializer<UpgradedElementT> upgradedSerializer = testSpecification.verifier.createUpgradedSerializer();

			TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
				restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);
			assumeThat(
				"This test only applies for test specifications that verify an upgraded serializer that requires reconfiguration to be compatible.",
				upgradeCompatibility,
				TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer());

			TypeSerializer<UpgradedElementT> reconfiguredUpgradedSerializer = upgradeCompatibility.getReconfiguredSerializer();
			assertSerializerIsValid(
				reconfiguredUpgradedSerializer,
				dataUnderTest(),
				testSpecification.verifier.expectedTestData());
		}
	}

	@Test
	public void upgradedSerializerIsValidWhenCompatibleAsIs() throws Exception {
		try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(testSpecification.verifier.verifierClassloader)) {
			TypeSerializerSnapshot<UpgradedElementT> restoredSerializerSnapshot = snapshotUnderTest();
			TypeSerializer<UpgradedElementT> upgradedSerializer = testSpecification.verifier.createUpgradedSerializer();

			TypeSerializerSchemaCompatibility<UpgradedElementT> upgradeCompatibility =
				restoredSerializerSnapshot.resolveSchemaCompatibility(upgradedSerializer);
			assumeThat(
				"This test only applies for test specifications that verify an upgraded serializer that is compatible as is.",
				upgradeCompatibility,
				TypeSerializerMatchers.isCompatibleAsIs());

			assertSerializerIsValid(
				upgradedSerializer,
				dataUnderTest(),
				testSpecification.verifier.expectedTestData());
		}
	}

	/**
	 * Asserts that a given {@link TypeSerializer} is valid, given a {@link DataInputView} of serialized data.
	 *
	 * <p>A serializer is valid, iff:
	 * <ul>
	 *     <li>1. The serializer can read and then write again the given serialized data.
	 *     <li>2. The serializer can produce a serializer snapshot which can be written and then read back again.
	 *     <li>3. The serializer's produced snapshot is capable of creating a restore serializer.
	 *     <li>4. The restore serializer created from the serializer snapshot can read and then
	 *            write again data written by step 1.
	 * </ul>
	 */
	private static <T> void assertSerializerIsValid(
			TypeSerializer<T> serializer,
			DataInputView dataInput,
			T expectedData) throws Exception {

		DataInputView serializedData = readAndThenWriteData(dataInput, serializer, serializer, expectedData);
		TypeSerializerSnapshot<T> snapshot = writeAndThenReadSerializerSnapshot(serializer);
		TypeSerializer<T> restoreSerializer = snapshot.restoreSerializer();
		readAndThenWriteData(serializedData, restoreSerializer, restoreSerializer, expectedData);
	}

	// ------------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------------

	private Path getSerializerSnapshotFilePath() {
		return Paths.get(getTestResourceDirectory() + "/serializer-snapshot");
	}

	private Path getTestDataFilePath() {
		return Paths.get(getTestResourceDirectory() + "/test-data");
	}

	private String getTestResourceDirectory() {
		return System.getProperty("user.dir") + "/src/test/resources/" + testSpecification.name + "-" + testSpecification.migrationVersion;
	}

	private TypeSerializerSnapshot<UpgradedElementT> snapshotUnderTest() throws Exception {
		return readSerializerSnapshot(
			contentsOf(getSerializerSnapshotFilePath()),
			testSpecification.migrationVersion);
	}

	private DataInputView dataUnderTest() throws IOException {
		return contentsOf(getTestDataFilePath());
	}

	private static void writeContentsTo(Path path, byte[] bytes) {
		try {
			Files.write(path, bytes);
		}
		catch (IOException e) {
			throw new RuntimeException("Failed to write to " + path, e);
		}
	}

	private static DataInputView contentsOf(Path path) {
		try {
			byte[] bytes = Files.readAllBytes(path);
			return new DataInputDeserializer(bytes);
		}
		catch (IOException e) {
			throw new RuntimeException("Failed to read contents of " + path, e);
		}
	}

	private static <T> void writeSerializerSnapshot(
			DataOutputView out,
			TypeSerializer<T> serializer,
			MigrationVersion migrationVersion) throws IOException {

		if (migrationVersion.isNewerVersionThan(MigrationVersion.v1_6)) {
			writeSerializerSnapshotCurrentFormat(out, serializer);
		} else {
			writeSerializerSnapshotPre17Format(out, serializer);
		}
	}

	private static <T> void writeSerializerSnapshotCurrentFormat(
			DataOutputView out,
			TypeSerializer<T> serializer) throws IOException {

		TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
			out, serializer.snapshotConfiguration(), serializer);
	}

	private static <T> void writeSerializerSnapshotPre17Format(
			DataOutputView out,
			TypeSerializer<T> serializer) throws IOException {

		TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
			out, Collections.singletonList(Tuple2.of(serializer, serializer.snapshotConfiguration())));
	}

	private static <T> TypeSerializerSnapshot<T> readSerializerSnapshot(
			DataInputView in,
			MigrationVersion migrationVersion) throws IOException {

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		if (migrationVersion.isNewerVersionThan(MigrationVersion.v1_6)) {
			return readSerializerSnapshotCurrentFormat(in, classLoader);
		} else {
			return readSerializerSnapshotPre17Format(in, classLoader);
		}
	}

	private static <T> TypeSerializerSnapshot<T> readSerializerSnapshotCurrentFormat(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		return TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
			in, userCodeClassLoader, null);
	}

	@SuppressWarnings("unchecked")
	private static <T> TypeSerializerSnapshot<T> readSerializerSnapshotPre17Format(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializerSnapshotPair =
			TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, userCodeClassLoader);
		return (TypeSerializerSnapshot<T>) serializerSnapshotPair.get(0).f1;
	}

	private static <T> DataInputView readAndThenWriteData(
			DataInputView originalDataInput,
			TypeSerializer<T> readSerializer,
			TypeSerializer<T> writeSerializer,
			T sanityCheckData) throws IOException {

		T data = readSerializer.deserialize(originalDataInput);
		assertEquals(sanityCheckData, data);

		DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
		writeSerializer.serialize(data, out);
		return new DataInputDeserializer(out.wrapAsByteBuffer());
	}

	private static <T> TypeSerializerSnapshot<T> writeAndThenReadSerializerSnapshot(
			TypeSerializer<T> serializer) throws IOException {

		DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
		writeSerializerSnapshotCurrentFormat(out, serializer);

		DataInputDeserializer in = new DataInputDeserializer(out.wrapAsByteBuffer());
		return readSerializerSnapshotCurrentFormat(in, Thread.currentThread().getContextClassLoader());
	}
}
