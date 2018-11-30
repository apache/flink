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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

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
	public void serializerSnapshotRestoresCurrentSerializer() {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();

		TypeSerializer<ElementT> restoredSerializer = snapshot.restoreSerializer();

		assertThat(restoredSerializer, instanceOf(testSpecification.serializerType));
	}

	@Test
	public void snapshotIsCompatibleWithTheCurrentSerializer() {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();

		TypeSerializerSchemaCompatibility<ElementT> result = snapshot.resolveSchemaCompatibility(testSpecification.createSerializer());

		assertTrue(result.isCompatibleAsIs());
	}

	@Test
	public void restoredSerializerIsAbleToDeserializePreviousData() throws IOException {
		TypeSerializerSnapshot<ElementT> snapshot = snapshotUnderTest();
		TypeSerializer<ElementT> serializer = snapshot.restoreSerializer();

		DataInputView input = dataUnderTest();
		for (int i = 0; i < testSpecification.testDataCount; i++) {
			final ElementT result = serializer.deserialize(input);
			assertThat(result, notNullValue());
		}
	}

	@SuppressWarnings("deprecation")
	@Test
	public void movingForward() throws IOException {
		TypeSerializerSnapshot<ElementT> previousSnapshot = snapshotUnderTest();
		TypeSerializer<ElementT> restoredSerializer = previousSnapshot.restoreSerializer();

		TypeSerializerSnapshot<ElementT> nextSnapshot = restoredSerializer.snapshotConfiguration();
		TypeSerializerSnapshot<ElementT> nextSnapshotDeserialized = writeAndThenReadTheSnapshot(restoredSerializer, nextSnapshot);

		assertThat(nextSnapshotDeserialized, allOf(
			notNullValue(),
			not(instanceOf(TypeSerializerConfigSnapshot.class))
		));
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
		return TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(in, Thread.currentThread().getContextClassLoader(), null);
	}

	private TypeSerializerSnapshot<ElementT> snapshotUnderTest() {
		DataInputView input = contentsOf(testSpecification.getSnapshotDataLocation());
		try {
			return readPre17SnapshotFormat(input);
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

	private DataInputView dataUnderTest() {
		return contentsOf(testSpecification.getTestDataLocation());
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

	protected static final class TestSpecification<T> {
		private final Class<? extends TypeSerializer<T>> serializerType;
		private final Class<? extends TypeSerializerSnapshot<T>> snapshotClass;
		private final String name;
		private Supplier<? extends TypeSerializer<T>> serializerProvider;
		private String snapshotDataLocation;
		private String testDataLocation;
		private int testDataCount;

		@SuppressWarnings("unchecked")
		public static <T> TestSpecification<T> builder(
			String name,
			Class<? extends TypeSerializer> serializerClass,
			Class<? extends TypeSerializerSnapshot> snapshotClass) {

			return new TestSpecification<>(
				name,
				(Class<? extends TypeSerializer<T>>) serializerClass,
				(Class<? extends TypeSerializerSnapshot<T>>) snapshotClass);
		}

		private TestSpecification(
			String name,
			Class<? extends TypeSerializer<T>> serializerType,
			Class<? extends TypeSerializerSnapshot<T>> snapshotClass) {

			this.name = name;
			this.serializerType = serializerType;
			this.snapshotClass = snapshotClass;
		}

		public TestSpecification<T> withSerializerProvider(Supplier<? extends TypeSerializer<T>> serializerProvider) {
			this.serializerProvider = serializerProvider;
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

		@Override
		public String toString() {
			return String.format("%s , %s, %s", name, serializerType.getSimpleName(), snapshotClass.getSimpleName());
		}
	}
}
