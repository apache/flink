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
import org.apache.flink.testutils.migration.SchemaCompatibilityTestingSerializer.SchemaCompatibilityTestingSnapshot;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link CompositeTypeSerializerUtil}. */
public class CompositeTypeSerializerUtilTest {

    // ------------------------------------------------------------------------------------------------
    //  Tests for CompositeTypeSerializerUtil#constructIntermediateCompatibilityResult
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testCompatibleAsIsIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] previousSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    new SchemaCompatibilityTestingSerializer("first serializer")
                            .snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer("second serializer")
                            .snapshotConfiguration(),
                };

        final TypeSerializerSnapshot<?>[] newSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithLastSerializer(
                            "first serializer"),
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithLastSerializer(
                            "second serializer"),
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        newSerializerSnapshots, previousSerializerSnapshots);

        assertTrue(intermediateCompatibilityResult.isCompatibleAsIs());
        assertTrue(intermediateCompatibilityResult.getFinalResult().isCompatibleAsIs());
        assertArrayEquals(
                Arrays.stream(newSerializerSnapshots)
                        .map(TypeSerializerSnapshot::restoreSerializer)
                        .toArray(),
                intermediateCompatibilityResult.getNestedSerializers());
    }

    @Test
    public void testCompatibleWithReconfiguredSerializerIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] previousSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    new SchemaCompatibilityTestingSerializer("a").snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer("b").snapshotConfiguration(),
                };

        final TypeSerializerSnapshot<?>[] newSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithLastSerializer("a"),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithLastSerializerAfterReconfiguration("b"),
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        newSerializerSnapshots, previousSerializerSnapshots);

        final TypeSerializer<?>[] expectedReconfiguredNestedSerializers =
                new TypeSerializer<?>[] {
                    new SchemaCompatibilityTestingSerializer("a"),
                    new SchemaCompatibilityTestingSerializer("b"),
                };

        assertTrue(intermediateCompatibilityResult.isCompatibleWithReconfiguredSerializer());
        assertArrayEquals(
                expectedReconfiguredNestedSerializers,
                intermediateCompatibilityResult.getNestedSerializers());
    }

    @Test
    public void testCompatibleAfterMigrationIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] previousSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    new SchemaCompatibilityTestingSerializer("a").snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer("b").snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer("c").snapshotConfiguration()
                };

        final TypeSerializerSnapshot<?>[] newSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithLastSerializerAfterReconfiguration("a"),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithLastSerializerAfterMigration("b"),
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithLastSerializer("c"),
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        newSerializerSnapshots, previousSerializerSnapshots);

        assertTrue(intermediateCompatibilityResult.isCompatibleAfterMigration());
        assertTrue(intermediateCompatibilityResult.getFinalResult().isCompatibleAfterMigration());
    }

    @Test
    public void testIncompatibleIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] previousSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    new SchemaCompatibilityTestingSerializer().snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer().snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer().snapshotConfiguration(),
                    new SchemaCompatibilityTestingSerializer().snapshotConfiguration()
                };

        final TypeSerializerSnapshot<?>[] newSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithLastSerializer(),
                    SchemaCompatibilityTestingSnapshot.thatIsIncompatibleWithTheLastSerializer(),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithLastSerializerAfterReconfiguration(),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithLastSerializerAfterMigration(),
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        newSerializerSnapshots, previousSerializerSnapshots);

        assertTrue(intermediateCompatibilityResult.isIncompatible());
        assertTrue(intermediateCompatibilityResult.getFinalResult().isIncompatible());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetFinalResultOnUndefinedReconfigureIntermediateCompatibilityResultFails() {
        IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                IntermediateCompatibilityResult.undefinedReconfigureResult(
                        new TypeSerializer[] {IntSerializer.INSTANCE});

        intermediateCompatibilityResult.getFinalResult();
    }

    @Test(expected = IllegalStateException.class)
    public void
            testGetNestedSerializersOnCompatibleAfterMigrationIntermediateCompatibilityResultFails() {
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
