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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.IntermediateCompatibilityResult;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.testutils.migration.SchemaCompatibilityTestingSerializer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link CompositeTypeSerializerUtil}.
 */
public class CompositeTypeSerializerUtilTest {

	// ------------------------------------------------------------------------------------------------
	//  Tests for CompositeTypeSerializerUtil#constructIntermediateCompatibilityResult
	// ------------------------------------------------------------------------------------------------

	@Test
	public void testCompatibleAsIsIntermediateCompatibilityResult() {
		final TypeSerializerSnapshot<?>[] testSerializerSnapshots = new TypeSerializerSnapshot<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
		};

		final TypeSerializer<?>[] testNewSerializers = new TypeSerializer<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer(),
		};

		IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
			CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(testNewSerializers, testSerializerSnapshots);

		assertTrue(intermediateCompatibilityResult.isCompatibleAsIs());
		assertTrue(intermediateCompatibilityResult.getFinalResult().isCompatibleAsIs());
		assertArrayEquals(testNewSerializers, intermediateCompatibilityResult.getNestedSerializers());
	}

	@Test
	public void testCompatibleWithReconfiguredSerializerIntermediateCompatibilityResult() {
		final TypeSerializerSnapshot<?>[] testSerializerSnapshots = new TypeSerializerSnapshot<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
		};

		final TypeSerializer<?>[] testNewSerializers = new TypeSerializer<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer(),
			new SchemaCompatibilityTestingSerializer.ReconfigurationRequiringSerializer<>(),
		};

		IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
			CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(testNewSerializers, testSerializerSnapshots);

		assertTrue(intermediateCompatibilityResult.isCompatibleWithReconfiguredSerializer());

		final TypeSerializer<?>[] expectedReconfiguredNestedSerializers = new TypeSerializer<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer(),
		};
		assertArrayEquals(expectedReconfiguredNestedSerializers, intermediateCompatibilityResult.getNestedSerializers());
	}

	@Test
	public void testCompatibleAfterMigrationIntermediateCompatibilityResult() {
		final TypeSerializerSnapshot<?>[] testSerializerSnapshots = new TypeSerializerSnapshot<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
		};

		final TypeSerializer<?>[] testNewSerializers = new TypeSerializer<?>[] {
			new SchemaCompatibilityTestingSerializer.ReconfigurationRequiringSerializer<>(),
			new SchemaCompatibilityTestingSerializer.UpgradedSchemaSerializer<>(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer()
		};

		IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
			CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(testNewSerializers, testSerializerSnapshots);

		assertTrue(intermediateCompatibilityResult.isCompatibleAfterMigration());
		assertTrue(intermediateCompatibilityResult.getFinalResult().isCompatibleAfterMigration());
	}

	@Test
	public void testIncompatibleIntermediateCompatibilityResult() {
		final TypeSerializerSnapshot<?>[] testSerializerSnapshots = new TypeSerializerSnapshot<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
			new SchemaCompatibilityTestingSerializer.InitialSerializer().snapshotConfiguration(),
		};

		final TypeSerializer<?>[] testNewSerializers = new TypeSerializer<?>[] {
			new SchemaCompatibilityTestingSerializer.InitialSerializer(),
			new SchemaCompatibilityTestingSerializer.IncompatibleSerializer<>(),
			new SchemaCompatibilityTestingSerializer.ReconfigurationRequiringSerializer<>(),
			new SchemaCompatibilityTestingSerializer.UpgradedSchemaSerializer()
		};

		IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
			CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(testNewSerializers, testSerializerSnapshots);

		assertTrue(intermediateCompatibilityResult.isIncompatible());
		assertTrue(intermediateCompatibilityResult.getFinalResult().isIncompatible());
	}

	@Test(expected = IllegalStateException.class)
	public void testGetFinalResultOnUndefinedReconfigureIntermediateCompatibilityResultFails() {
		IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
			IntermediateCompatibilityResult.undefinedReconfigureResult(new TypeSerializer[]{ IntSerializer.INSTANCE });

		intermediateCompatibilityResult.getFinalResult();
	}

	@Test(expected = IllegalStateException.class)
	public void testGetNestedSerializersOnCompatibleAfterMigrationIntermediateCompatibilityResultFails() {
		IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
			IntermediateCompatibilityResult.definedCompatibleAfterMigrationResult();

		intermediateCompatibilityResult.getNestedSerializers();
	}

	@Test(expected = IllegalStateException.class)
	public void testGetNestedSerializersOnIncompatibleIntermediateCompatibilityResultFails() {
		IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
			IntermediateCompatibilityResult.definedIncompatibleResult();

		intermediateCompatibilityResult.getNestedSerializers();
	}
}
