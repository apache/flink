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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** Tests for the {@link CompositeTypeSerializerUtil}. */
class CompositeTypeSerializerUtilTest {

    // ------------------------------------------------------------------------------------------------
    //  Tests for CompositeTypeSerializerUtil#constructIntermediateCompatibilityResult
    // ------------------------------------------------------------------------------------------------

    @Test
    void testCompatibleAsIsIntermediateCompatibilityResult() {
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

        assertThat(intermediateCompatibilityResult.isCompatibleAsIs()).isTrue();
        assertThat(intermediateCompatibilityResult.getFinalResult().isCompatibleAsIs()).isTrue();
        assertArrayEquals(
                Arrays.stream(newSerializerSnapshots)
                        .map(TypeSerializerSnapshot::restoreSerializer)
                        .toArray(),
                intermediateCompatibilityResult.getNestedSerializers());
    }

    @Test
    void testCompatibleWithReconfiguredSerializerIntermediateCompatibilityResult() {
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

        assertThat(intermediateCompatibilityResult.isCompatibleWithReconfiguredSerializer())
                .isTrue();
        assertThat(intermediateCompatibilityResult.getNestedSerializers())
                .containsExactly(expectedReconfiguredNestedSerializers);
    }

    @Test
    void testCompatibleAfterMigrationIntermediateCompatibilityResult() {
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

        assertThat(intermediateCompatibilityResult.isCompatibleAfterMigration()).isTrue();
        assertThat(intermediateCompatibilityResult.getFinalResult().isCompatibleAfterMigration())
                .isTrue();
    }

    @Test
    void testIncompatibleIntermediateCompatibilityResult() {
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

        assertThat(intermediateCompatibilityResult.isIncompatible()).isTrue();
        assertThat(intermediateCompatibilityResult.getFinalResult().isIncompatible()).isTrue();
    }

    @Test
    void testGetFinalResultOnUndefinedReconfigureIntermediateCompatibilityResultFails() {
        IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                IntermediateCompatibilityResult.undefinedReconfigureResult(
                        new TypeSerializer[] {IntSerializer.INSTANCE});

        assertThatThrownBy(intermediateCompatibilityResult::getFinalResult)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testGetNestedSerializersOnCompatibleAfterMigrationIntermediateCompatibilityResultFails() {
        IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                IntermediateCompatibilityResult.definedCompatibleAfterMigrationResult();

        assertThatThrownBy(intermediateCompatibilityResult::getNestedSerializers)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testGetNestedSerializersOnIncompatibleIntermediateCompatibilityResultFails() {
        IntermediateCompatibilityResult<Integer> intermediateCompatibilityResult =
                IntermediateCompatibilityResult.definedIncompatibleResult();

        assertThatThrownBy(intermediateCompatibilityResult::getNestedSerializers)
                .isInstanceOf(IllegalStateException.class);
    }
}
