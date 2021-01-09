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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link CompositeTypeSerializerUtil}. */
public class CompositeTypeSerializerUtilTest {

    // ------------------------------------------------------------------------------------------------
    //  Tests for CompositeTypeSerializerUtil#constructIntermediateCompatibilityResult
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testCompatibleAsIsIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] testSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithNextSerializer(
                            "first serializer"),
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithNextSerializer(
                            "second serializer"),
                };

        final TypeSerializer<?>[] testNewSerializers =
                new TypeSerializer<?>[] {
                    new SchemaCompatibilityTestingSerializer("first serializer"),
                    new SchemaCompatibilityTestingSerializer("second serializer"),
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        testNewSerializers, testSerializerSnapshots);

        assertTrue(intermediateCompatibilityResult.isCompatibleAsIs());
        assertTrue(intermediateCompatibilityResult.getFinalResult().isCompatibleAsIs());
        assertArrayEquals(
                testNewSerializers, intermediateCompatibilityResult.getNestedSerializers());
    }

    @Test
    public void testCompatibleWithReconfiguredSerializerIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] testSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithNextSerializer("a"),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithNextSerializerAfterReconfiguration("b"),
                };

        final TypeSerializer<?>[] testNewSerializers =
                new TypeSerializer<?>[] {
                    new SchemaCompatibilityTestingSerializer("a"),
                    new SchemaCompatibilityTestingSerializer("b"),
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        testNewSerializers, testSerializerSnapshots);

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
        final TypeSerializerSnapshot<?>[] testSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithNextSerializerAfterReconfiguration("a"),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithNextSerializerAfterMigration("b"),
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithNextSerializer("c"),
                };

        final TypeSerializer<?>[] testNewSerializers =
                new TypeSerializer<?>[] {
                    new SchemaCompatibilityTestingSerializer("a"),
                    new SchemaCompatibilityTestingSerializer("b"),
                    new SchemaCompatibilityTestingSerializer("c")
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        testNewSerializers, testSerializerSnapshots);

        assertTrue(intermediateCompatibilityResult.isCompatibleAfterMigration());
        assertTrue(intermediateCompatibilityResult.getFinalResult().isCompatibleAfterMigration());
    }

    @Test
    public void testIncompatibleIntermediateCompatibilityResult() {
        final TypeSerializerSnapshot<?>[] testSerializerSnapshots =
                new TypeSerializerSnapshot<?>[] {
                    SchemaCompatibilityTestingSnapshot.thatIsCompatibleWithNextSerializer(),
                    SchemaCompatibilityTestingSnapshot.thatIsIncompatibleWithTheNextSerializer(),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithNextSerializerAfterReconfiguration(),
                    SchemaCompatibilityTestingSnapshot
                            .thatIsCompatibleWithNextSerializerAfterMigration(),
                };

        final TypeSerializer<?>[] testNewSerializers =
                new TypeSerializer<?>[] {
                    new SchemaCompatibilityTestingSerializer(),
                    new SchemaCompatibilityTestingSerializer(),
                    new SchemaCompatibilityTestingSerializer(),
                    new SchemaCompatibilityTestingSerializer()
                };

        IntermediateCompatibilityResult<?> intermediateCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        testNewSerializers, testSerializerSnapshots);

        assertTrue(intermediateCompatibilityResult.isIncompatible());
        assertTrue(intermediateCompatibilityResult.getFinalResult().isIncompatible());
    }

    @Test
    public void testGetFinalResultOnUndefinedReconfigureIntermediateCompatibilityResultFails() {
        assertThrows(
                IllegalStateException.class,
                () -> {
                    IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                            IntermediateCompatibilityResult.undefinedReconfigureResult(
                                    new TypeSerializer[] {IntSerializer.INSTANCE});

                    intermediateCompatibilityResult.getFinalResult();
                });
    }

    @Test
    public void
            testGetNestedSerializersOnCompatibleAfterMigrationIntermediateCompatibilityResultFails() {
        assertThrows(
                IllegalStateException.class,
                () -> {
                    IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                            IntermediateCompatibilityResult.definedCompatibleAfterMigrationResult();

                    intermediateCompatibilityResult.getNestedSerializers();
                });
    }

    @Test
    public void testGetNestedSerializersOnIncompatibleIntermediateCompatibilityResultFails() {
        assertThrows(
                IllegalStateException.class,
                () -> {
                    IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                            IntermediateCompatibilityResult.definedIncompatibleResult();

                    intermediateCompatibilityResult.getNestedSerializers();
                });
    }
}
