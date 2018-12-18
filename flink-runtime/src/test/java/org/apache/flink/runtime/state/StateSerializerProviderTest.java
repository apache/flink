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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.testutils.statemigration.TestType;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test suit for {@link StateSerializerProvider}.
 */
public class StateSerializerProviderTest {

	// --------------------------------------------------------------------------------
	//  Tests for #currentSchemaSerializer()
	// --------------------------------------------------------------------------------

	@Test
	public void testCurrentSchemaSerializerForNewStateSerializerProvider() {
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromNewState(new TestType.V1TestTypeSerializer());
		assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
	}

	@Test
	public void testCurrentSchemaSerializerForRestoredStateSerializerProvider() {
		TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromRestoredState(serializer.snapshotConfiguration());
		assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
	}

	// --------------------------------------------------------------------------------
	//  Tests for #previousSchemaSerializer()
	// --------------------------------------------------------------------------------

	@Test(expected = UnsupportedOperationException.class)
	public void testPreviousSchemaSerializerForNewStateSerializerProvider() {
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromNewState(new TestType.V1TestTypeSerializer());

		// this should fail with an exception
		testProvider.previousSchemaSerializer();
	}

	@Test
	public void testPreviousSchemaSerializerForRestoredStateSerializerProvider() {
		TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromRestoredState(serializer.snapshotConfiguration());
		assertTrue(testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
	}

	@Test
	public void testLazyInstantiationOfPreviousSchemaSerializer() {
		// create the provider with an exception throwing snapshot;
		// this would throw an exception if the restore serializer was eagerly accessed
		StateSerializerProvider<String> testProvider =
			StateSerializerProvider.fromRestoredState(new ExceptionThrowingSerializerSnapshot());

		try {
			// if we fail here, that means the restore serializer was indeed lazily accessed
			testProvider.previousSchemaSerializer();
			fail("expected to fail when accessing the restore serializer.");
		} catch (Exception expected) {
			// success
		}
	}

	// --------------------------------------------------------------------------------
	//  Tests for #registerNewSerializerForRestoredState(TypeSerializer)
	// --------------------------------------------------------------------------------

	@Test(expected = UnsupportedOperationException.class)
	public void testRegisterNewSerializerWithNewStateSerializerProviderShouldFail() {
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromNewState(new TestType.V1TestTypeSerializer());
		testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRegisterNewSerializerTwiceWithNewStateSerializerProviderShouldFail() {
		TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromRestoredState(serializer.snapshotConfiguration());

		testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());

		// second registration should fail
		testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());
	}

	@Test
	public void testRegisterNewCompatibleAsIsSerializer() {
		TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromRestoredState(serializer.snapshotConfiguration());

		// register compatible serializer for state
		TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
			testProvider.registerNewSerializerForRestoredState(new TestType.V1TestTypeSerializer());
		assertTrue(schemaCompatibility.isCompatibleAsIs());

		assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
		assertTrue(testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
	}

	@Test
	public void testRegisterNewCompatibleAfterMigrationSerializer() {
		TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromRestoredState(serializer.snapshotConfiguration());

		// register serializer that requires migration for state
		TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
			testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());
		assertTrue(schemaCompatibility.isCompatibleAfterMigration());
	}

	@Test
	public void testRegisterIncompatibleSerializer() {
		TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
		StateSerializerProvider<TestType> testProvider = StateSerializerProvider.fromRestoredState(serializer.snapshotConfiguration());

		// register serializer that requires migration for state
		TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
			testProvider.registerNewSerializerForRestoredState(new TestType.IncompatibleTestTypeSerializer());
		assertTrue(schemaCompatibility.isIncompatible());

		try {
			// a serializer for the current schema will no longer be accessible
			testProvider.currentSchemaSerializer();
		} catch (Exception excepted) {
			// success
		}
	}

	// --------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------

	public static class ExceptionThrowingSerializerSnapshot implements TypeSerializerSnapshot<String> {

		@Override
		public TypeSerializer<String> restoreSerializer() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(TypeSerializer<String> newSerializer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getCurrentVersion() {
			throw new UnsupportedOperationException();
		}
	}
}
