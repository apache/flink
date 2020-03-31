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
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.hasSameCompatibilityAs;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * A test base for verifying {@link TypeSerializerSnapshot} migration.
 *
 * @param <ElementT> the element being serialized.
 */
public abstract class TypeSerializerSnapshotMigrationTestBase<ElementT> extends TestLogger {

	private final TestSpecification<ElementT> testSpecification;

	protected TypeSerializerSnapshotMigrationTestBase(TestSpecification<ElementT> testSpecification) {
		this.testSpecification = checkNotNull(testSpecification);
	}

	@Test
	public void serializerSnapshotIsSuccessfullyRead() {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();

		assertThat(snapshot, allOf(
			notNullValue(),
			instanceOf(TypeSerializerSnapshot.class)
		));
	}

	@Test
	public void specifiedNewSerializerHasExpectedCompatibilityResultsWithSnapshot() {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();

		TypeSerializerSchemaCompatibility<ElementT> result = snapshot.resolveSchemaCompatibility(testSpecification.createSerializer());

		assertThat(result, testSpecification.schemaCompatibilityMatcher);
	}

	@Test
	public void restoredSerializerIsAbleToDeserializePreviousData() throws IOException {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();
		TypeSerializer<ElementT> serializer = snapshot.restoreSerializer();

		assertSerializerIsAbleToReadOldData(serializer);
	}

	@Test
	public void reconfiguredSerializerIsAbleToDeserializePreviousData() throws IOException {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();
		TypeSerializerSchemaCompatibility<ElementT> compatibility = snapshot.resolveSchemaCompatibility(testSpecification.createSerializer());

		if (!compatibility.isCompatibleWithReconfiguredSerializer()) {
			// this test only applies for reconfigured serializers.
			return;
		}

		TypeSerializer<ElementT> serializer = compatibility.getReconfiguredSerializer();
		assertSerializerIsAbleToReadOldData(serializer);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void movingForward() throws IOException {
		TypeSerializerSnapshot<ElementT> previousSnapshot = snapshotUnderTest();
		TypeSerializer<ElementT> restoredSerializer = previousSnapshot.restoreSerializer();

		TypeSerializerSnapshot<ElementT> nextSnapshot = restoredSerializer.snapshotConfiguration();
		assertThat(nextSnapshot, instanceOf(testSpecification.snapshotClass));

		TypeSerializerSnapshot<ElementT> nextSnapshotDeserialized = writeAndThenReadTheSnapshot(restoredSerializer, nextSnapshot);

		assertThat(nextSnapshotDeserialized, allOf(
			notNullValue(),
			not(instanceOf(TypeSerializerConfigSnapshot.class))
		));
	}

	@Test
	public void restoreSerializerFromNewSerializerSnapshotIsAbleToDeserializePreviousData() throws IOException {
		TypeSerializer<ElementT> newSerializer = testSpecification.createSerializer();

		TypeSerializerSchemaCompatibility<ElementT> compatibility =
			snapshotUnderTest().resolveSchemaCompatibility(newSerializer);

		final TypeSerializer<ElementT> nextSerializer;
		if (compatibility.isCompatibleWithReconfiguredSerializer()) {
			nextSerializer = compatibility.getReconfiguredSerializer();
		} else if (compatibility.isCompatibleAsIs()) {
			nextSerializer = newSerializer;
		} else {
			// this test does not apply.
			return;
		}

		TypeSerializerSnapshot<ElementT> nextSnapshot = nextSerializer.snapshotConfiguration();

		assertSerializerIsAbleToReadOldData(nextSnapshot.restoreSerializer());
	}

	// --------------------------------------------------------------------------------------------------------------
	// Test Helpers
	// --------------------------------------------------------------------------------------------------------------

	private TypeSerializerSnapshot<ElementT> writeAndThenReadTheSnapshot(
		TypeSerializer<ElementT> serializer,
		TypeSerializerSnapshot<ElementT> newSnapshot) throws IOException {

		DataOutputSerializer out = new DataOutputSerializer(128);
		TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(out, newSnapshot, serializer);

		DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
		return readSnapshot(in);
	}

	private TypeSerializerSnapshot<ElementT> snapshotUnderTest() {
		DataInputView input = contentsOf(testSpecification.getSnapshotDataLocation());
		try {
			if (!testSpecification.getTestMigrationVersion().isNewerVersionThan(MigrationVersion.v1_6)) {
				return readPre17SnapshotFormat(input);
			} else {
				return readSnapshot(input);
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Unable to read " + testSpecification.getSnapshotDataLocation(),  e);
		}
	}

	@SuppressWarnings({"unchecked", "deprecation"})
	private TypeSerializerSnapshot<ElementT> readPre17SnapshotFormat(DataInputView input) throws IOException {
		final ClassLoader cl = Thread.currentThread().getContextClassLoader();

		List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializers =
			TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(input, cl);

		return (TypeSerializerSnapshot<ElementT>) serializers.get(0).f1;
	}

	private TypeSerializerSnapshot<ElementT> readSnapshot(DataInputView in) throws IOException {
		return TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
			in, Thread.currentThread().getContextClassLoader(), null);
	}

	private DataInputView dataUnderTest() {
		return contentsOf(testSpecification.getTestDataLocation());
	}

	private void assertSerializerIsAbleToReadOldData(TypeSerializer<ElementT> serializer) throws IOException {
		DataInputView input = dataUnderTest();

		final Matcher<ElementT> matcher = testSpecification.testDataElementMatcher;
		for (int i = 0; i < testSpecification.testDataCount; i++) {
			final ElementT result = serializer.deserialize(input);
			assertThat(result, matcher);
		}
	}

	// --------------------------------------------------------------------------------------------------------------
	// Static Helpers
	// --------------------------------------------------------------------------------------------------------------

	private static DataInputView contentsOf(Path path) {
		try {
			byte[] bytes = Files.readAllBytes(path);
			return new DataInputDeserializer(bytes);
		}
		catch (IOException e) {
			throw new RuntimeException("Failed to read " + path, e);
		}
	}

	private static Path resourcePath(String resourceName) {
		checkNotNull(resourceName, "resource name can not be NULL");
		try {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			URL resource = cl.getResource(resourceName);
			if (resource == null) {
				throw new IllegalArgumentException("unable locate test data " + resourceName);
			}
			return Paths.get(resource.toURI());
		}
		catch (URISyntaxException e) {
			throw new RuntimeException("unable", e);
		}
	}

	// --------------------------------------------------------------------------------------------------------------
	// Test Specification
	// --------------------------------------------------------------------------------------------------------------

	/**
	 * Test Specification.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class TestSpecification<T> {
		private final Class<? extends TypeSerializer<T>> serializerType;
		private final Class<? extends TypeSerializerSnapshot<T>> snapshotClass;
		private final String name;
		private final MigrationVersion testMigrationVersion;
		private Supplier<? extends TypeSerializer<T>> serializerProvider;
		private Matcher<TypeSerializerSchemaCompatibility<T>> schemaCompatibilityMatcher;
		private String snapshotDataLocation;
		private String testDataLocation;
		private int testDataCount;

		@SuppressWarnings("unchecked")
		private Matcher<T> testDataElementMatcher = (Matcher<T>) notNullValue();

		@SuppressWarnings("unchecked")
		public static <T> TestSpecification<T> builder(
			String name,
			Class<? extends TypeSerializer> serializerClass,
			Class<? extends TypeSerializerSnapshot> snapshotClass,
			MigrationVersion testMigrationVersion) {

			return new TestSpecification<>(
				name,
				(Class<? extends TypeSerializer<T>>) serializerClass,
				(Class<? extends TypeSerializerSnapshot<T>>) snapshotClass,
				testMigrationVersion);
		}

		private TestSpecification(
			String name,
			Class<? extends TypeSerializer<T>> serializerType,
			Class<? extends TypeSerializerSnapshot<T>> snapshotClass,
			MigrationVersion testMigrationVersion) {

			this.name = name;
			this.serializerType = serializerType;
			this.snapshotClass = snapshotClass;
			this.testMigrationVersion = testMigrationVersion;
		}

		public TestSpecification<T> withNewSerializerProvider(Supplier<? extends TypeSerializer<T>> serializerProvider) {
			return withNewSerializerProvider(serializerProvider, TypeSerializerSchemaCompatibility.compatibleAsIs());
		}

		public TestSpecification<T> withNewSerializerProvider(
				Supplier<? extends TypeSerializer<T>> serializerProvider,
				TypeSerializerSchemaCompatibility<T> expectedCompatibilityResult) {
			this.serializerProvider = serializerProvider;
			this.schemaCompatibilityMatcher = hasSameCompatibilityAs(expectedCompatibilityResult);
			return this;
		}

		public TestSpecification<T> withSchemaCompatibilityMatcher(
				Matcher<TypeSerializerSchemaCompatibility<T>> schemaCompatibilityMatcher) {
			this.schemaCompatibilityMatcher = schemaCompatibilityMatcher;
			return this;
		}

		public TestSpecification<T> withSnapshotDataLocation(String snapshotDataLocation) {
			this.snapshotDataLocation = snapshotDataLocation;
			return this;
		}

		public TestSpecification<T> withTestData(String testDataLocation, int testDataCount) {
			this.testDataLocation = testDataLocation;
			this.testDataCount = testDataCount;
			return this;
		}

		public TestSpecification<T> withTestDataMatcher(Matcher<T> matcher) {
			testDataElementMatcher = matcher;
			return this;
		}

		public TestSpecification<T> withTestDataCount(int expectedDataItmes) {
			this.testDataCount = expectedDataItmes;
			return this;
		}

		private TypeSerializer<T> createSerializer() {
			try {
				return (serializerProvider == null) ? serializerType.newInstance() : serializerProvider.get();
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException("serializer provider was not set, and creating the serializer reflectively failed.", e);
			}
		}

		private Path getTestDataLocation() {
			return resourcePath(this.testDataLocation);
		}

		private Path getSnapshotDataLocation() {
			return resourcePath(this.snapshotDataLocation);
		}

		private MigrationVersion getTestMigrationVersion() {
			return testMigrationVersion;
		}

		@Override
		public String toString() {
			return String.format("%s , %s, %s", name, serializerType.getSimpleName(), snapshotClass.getSimpleName());
		}

	}

	/**
	 * Utility class to help build a collection of {@link TestSpecification} for
	 * multiple test migration versions. For each test specification added,
	 * an entry will be added for each specified migration version.
	 */
	public static final class TestSpecifications {

		private static final int DEFAULT_TEST_DATA_COUNT = 10;
		private static final String DEFAULT_SNAPSHOT_FILENAME_FORMAT = "flink-%s-%s-snapshot";
		private static final String DEFAULT_TEST_DATA_FILENAME_FORMAT = "flink-%s-%s-data";

		private final Collection<TestSpecification<?>> testSpecifications = new LinkedList<>();
		private final MigrationVersion[] testVersions;

		public TestSpecifications(MigrationVersion... testVersions) {
			checkArgument(
				testVersions.length > 0,
				"At least one test migration version should be specified.");
			this.testVersions = testVersions;
		}

		/**
		 * Adds a test specification to be tested for all specified test versions.
		 *
		 * <p>This method adds the specification with pre-defined snapshot and data filenames,
		 * with the format "flink-&lt;testVersion&gt;-&lt;specName&gt;-&lt;data/snapshot&gt;",
		 * and each specification's test data count is assumed to always be 10.
		 *
		 * @param name test specification name.
		 * @param serializerClass class of the current serializer.
		 * @param snapshotClass class of the current serializer snapshot class.
		 * @param serializerProvider provider for an instance of the current serializer.
		 *
		 * @param <T> type of the test data.
		 */
		public <T> void add(
				String name,
				Class<? extends TypeSerializer> serializerClass,
				Class<? extends TypeSerializerSnapshot> snapshotClass,
				Supplier<? extends TypeSerializer<T>> serializerProvider) {
			for (MigrationVersion testVersion : testVersions) {
				testSpecifications.add(
					TestSpecification.<T>builder(
						getSpecNameForVersion(name, testVersion),
						serializerClass,
						snapshotClass,
						testVersion)
						.withNewSerializerProvider(serializerProvider)
						.withSnapshotDataLocation(
							String.format(DEFAULT_SNAPSHOT_FILENAME_FORMAT, testVersion, name))
						.withTestData(
							String.format(DEFAULT_TEST_DATA_FILENAME_FORMAT, testVersion, name),
							DEFAULT_TEST_DATA_COUNT)
				);
			}
		}

		/**
		 * Adds a test specification to be tested for all specified test versions.
		 *
		 * <p>This method adds the specification with pre-defined snapshot and data filenames,
		 * with the format "flink-&lt;testVersion&gt;-&lt;specName&gt;-&lt;data/snapshot&gt;",
		 * and each specification's test data count is assumed to always be 10.
		 *
		 * @param <T> type of the test data.
		 */
		public <T> void addWithCompatibilityMatcher(
				String name,
				Class<? extends TypeSerializer> serializerClass,
				Class<? extends TypeSerializerSnapshot> snapshotClass,
				Supplier<? extends TypeSerializer<T>> serializerProvider,
				Matcher<TypeSerializerSchemaCompatibility<T>> schemaCompatibilityMatcher) {
			for (MigrationVersion testVersion : testVersions) {
				testSpecifications.add(
						TestSpecification.<T>builder(
								getSpecNameForVersion(name, testVersion),
								serializerClass,
								snapshotClass,
								testVersion)
								.withNewSerializerProvider(serializerProvider)
								.withSchemaCompatibilityMatcher(schemaCompatibilityMatcher)
								.withSnapshotDataLocation(
										String.format(DEFAULT_SNAPSHOT_FILENAME_FORMAT, testVersion, name))
								.withTestData(
										String.format(DEFAULT_TEST_DATA_FILENAME_FORMAT, testVersion, name),
										DEFAULT_TEST_DATA_COUNT)
				);
			}
		}

		/**
		 * Adds a test specification to be tested for all specified test versions.
		 *
		 * <p>This method adds the specification with pre-defined snapshot and data filenames,
		 * with the format "flink-&lt;testVersion&gt;-&lt;specName&gt;-&lt;data/snapshot&gt;",
		 * and each specification's test data count is assumed to always be 10.
		 *
		 * @param name test specification name.
		 * @param serializerClass class of the current serializer.
		 * @param snapshotClass class of the current serializer snapshot class.
		 * @param serializerProvider provider for an instance of the current serializer.
		 * @param elementMatcher an {@code hamcrest} matcher to match test data.
		 *
		 * @param <T> type of the test data.
		 */
		public <T> void add(
			String name,
			Class<? extends TypeSerializer> serializerClass,
			Class<? extends TypeSerializerSnapshot> snapshotClass,
			Supplier<? extends TypeSerializer<T>> serializerProvider,
			Matcher<T> elementMatcher)  {
			for (MigrationVersion testVersion : testVersions) {
				testSpecifications.add(
					TestSpecification.<T>builder(
						getSpecNameForVersion(name, testVersion),
						serializerClass,
						snapshotClass,
						testVersion)
						.withNewSerializerProvider(serializerProvider)
						.withSnapshotDataLocation(
							String.format(DEFAULT_SNAPSHOT_FILENAME_FORMAT, testVersion, name))
						.withTestData(
							String.format(DEFAULT_TEST_DATA_FILENAME_FORMAT, testVersion, name),
							DEFAULT_TEST_DATA_COUNT)
					.withTestDataMatcher(elementMatcher)
				);
			}
		}

		/**
		 * Adds a test specification to be tested for all specified test versions.
		 *
		 * @param name test specification name.
		 * @param serializerClass class of the current serializer.
		 * @param snapshotClass class of the current serializer snapshot class.
		 * @param serializerProvider provider for an instance of the current serializer.
		 * @param testSnapshotFilenameProvider provider for the filename of the test snapshot.
		 * @param testDataFilenameProvider provider for the filename of the test data.
		 * @param testDataCount expected number of records to be read in the test data files.
		 *
		 * @param <T> type of the test data.
		 */
		public <T> void add(
				String name,
				Class<? extends TypeSerializer> serializerClass,
				Class<? extends TypeSerializerSnapshot> snapshotClass,
				Supplier<? extends TypeSerializer<T>> serializerProvider,
				TestResourceFilenameSupplier testSnapshotFilenameProvider,
				TestResourceFilenameSupplier testDataFilenameProvider,
				int testDataCount) {
			for (MigrationVersion testVersion : testVersions) {
				testSpecifications.add(
					TestSpecification.<T>builder(
						getSpecNameForVersion(name, testVersion),
						serializerClass,
						snapshotClass,
						testVersion)
					.withNewSerializerProvider(serializerProvider)
					.withSnapshotDataLocation(testSnapshotFilenameProvider.get(testVersion))
					.withTestData(testDataFilenameProvider.get(testVersion), testDataCount)
				);
			}
		}

		public Collection<TestSpecification<?>> get() {
			return Collections.unmodifiableCollection(testSpecifications);
		}

		private static String getSpecNameForVersion(String baseName, MigrationVersion testVersion) {
			return testVersion + "-" + baseName;
		}
	}

	/**
	 * Supplier of paths based on {@link MigrationVersion}.
	 */
	protected interface TestResourceFilenameSupplier {
		String get(MigrationVersion testVersion);
	}
}
