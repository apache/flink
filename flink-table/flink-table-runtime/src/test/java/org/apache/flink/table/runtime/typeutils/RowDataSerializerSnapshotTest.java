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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer.RowDataSerializerSnapshot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isCompatibleAsIs;
import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isIncompatible;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataSerializerSnapshot}. */
class RowDataSerializerSnapshotTest {

    @Test
    void sameTypesIsCompatibleAsIs() throws Exception {
        RowDataSerializer previous =
                new RowDataSerializer(new IntType(false), new VarCharType(false, 100));
        RowDataSerializer current =
                new RowDataSerializer(new IntType(false), new VarCharType(false, 100));

        assertThat(resolveCompatibility(previous, current)).is(isCompatibleAsIs());
    }

    /** Widening field nullability (NOT NULL -> NULL) should be compatible as-is. */
    @Test
    void wideningFieldNullabilityIsCompatible() throws Exception {
        RowDataSerializer previous =
                new RowDataSerializer(new IntType(false), new VarCharType(false, 100));
        RowDataSerializer current =
                new RowDataSerializer(new IntType(true), new VarCharType(true, 100));

        assertThat(resolveCompatibility(previous, current)).is(isCompatibleAsIs());
    }

    /** Narrowing field nullability (NULL -> NOT NULL) should be incompatible. */
    @Test
    void narrowingFieldNullabilityIsIncompatible() throws Exception {
        RowDataSerializer previous =
                new RowDataSerializer(new IntType(true), new VarCharType(true, 100));
        RowDataSerializer current =
                new RowDataSerializer(new IntType(false), new VarCharType(false, 100));

        assertThat(resolveCompatibility(previous, current)).is(isIncompatible());
    }

    /** Mixed nullability changes should be incompatible when any field narrows. */
    @Test
    void mixedNullabilityChangeIsIncompatibleWhenAnyFieldNarrows() throws Exception {
        RowDataSerializer previous =
                new RowDataSerializer(new IntType(false), new VarCharType(true, 100));
        // INT widened, VARCHAR narrowed
        RowDataSerializer current =
                new RowDataSerializer(new IntType(true), new VarCharType(false, 100));

        assertThat(resolveCompatibility(previous, current)).is(isIncompatible());
    }

    @Test
    void differentFieldCountIsIncompatible() throws Exception {
        RowDataSerializer previous = new RowDataSerializer(new IntType(false));
        RowDataSerializer current =
                new RowDataSerializer(new IntType(false), new VarCharType(false, 100));

        assertThat(resolveCompatibility(previous, current)).is(isIncompatible());
    }

    @Test
    void differentFieldTypeIsIncompatible() throws Exception {
        RowDataSerializer previous = new RowDataSerializer(new IntType(false));
        RowDataSerializer current = new RowDataSerializer(new BigIntType(false));

        assertThat(resolveCompatibility(previous, current)).is(isIncompatible());
    }

    /**
     * Nullability changes in nested row fields (even widening) are not supported and should be
     * incompatible.
     */
    @Test
    void nullabilityChangeInNestedRowFieldIsIncompatible() throws Exception {
        RowType nestedNotNull =
                RowType.of(
                        new LogicalType[] {new IntType(false), new VarCharType(false, 100)},
                        new String[] {"a", "b"});
        RowType nestedNullable =
                RowType.of(
                        new LogicalType[] {new IntType(true), new VarCharType(false, 100)},
                        new String[] {"a", "b"});

        RowDataSerializer previous = new RowDataSerializer(new LogicalType[] {nestedNotNull});
        RowDataSerializer current = new RowDataSerializer(new LogicalType[] {nestedNullable});

        assertThat(resolveCompatibility(previous, current)).is(isIncompatible());
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    /**
     * Round-trips the previous serializer's snapshot through serialization (mirroring what happens
     * on checkpoint restore) and resolves compatibility against the current serializer's snapshot.
     */
    private static TypeSerializerSchemaCompatibility<RowData> resolveCompatibility(
            RowDataSerializer previous, RowDataSerializer current) throws Exception {
        TypeSerializerSnapshot<RowData> previousSnapshot = previous.snapshotConfiguration();

        DataOutputSerializer out = new DataOutputSerializer(64);
        previousSnapshot.writeSnapshot(out);

        TypeSerializerSnapshot<RowData> restoredPreviousSnapshot = new RowDataSerializerSnapshot();
        restoredPreviousSnapshot.readSnapshot(
                previousSnapshot.getCurrentVersion(),
                new DataInputDeserializer(out.getCopyOfBuffer()),
                RowDataSerializerSnapshotTest.class.getClassLoader());

        return current.snapshotConfiguration().resolveSchemaCompatibility(restoredPreviousSnapshot);
    }
}
