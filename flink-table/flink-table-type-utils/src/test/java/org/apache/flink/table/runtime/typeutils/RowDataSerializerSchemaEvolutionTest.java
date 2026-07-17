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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer.RowDataSerializerSnapshot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests name-based schema evolution on {@link RowDataSerializerSnapshot}: the compatibility verdict
 * for differing layouts and the {@code migrate} remap that adapts a deserialized {@link RowData} to
 * the new layout. These complement {@link RowDataSerializerFieldNamesTest} (field-name metadata and
 * V4 snapshot format) by exercising the actual evolution algorithm.
 */
class RowDataSerializerSchemaEvolutionTest {

    private static RowType row(String[] names, LogicalType... types) {
        return RowType.of(types, names);
    }

    private static LogicalType nullableInt() {
        return new IntType(true);
    }

    /**
     * Builds a name-retaining snapshot and round-trips it through the versioned snapshot format, so
     * nested field names flow through the nested delegate exactly as they would on a real restore.
     */
    private static RowDataSerializerSnapshot snapshot(RowType rowType) throws IOException {
        TypeSerializerSnapshot<RowData> snapshot =
                RowDataSerializer.withFieldNames(rowType).snapshotConfiguration();
        DataOutputSerializer out = new DataOutputSerializer(256);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return (RowDataSerializerSnapshot)
                TypeSerializerSnapshot.<RowData>readVersionedSnapshot(
                        in, RowDataSerializerSchemaEvolutionTest.class.getClassLoader());
    }

    @Test
    void addNullableFieldAtEnd() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), VarCharType.STRING_TYPE));
        RowDataSerializerSnapshot newSnap =
                snapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                VarCharType.STRING_TYPE,
                                nullableInt()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        GenericRowData oldValue = GenericRowData.of(1, fromString("x"));
        RowData migrated = newSnap.migrate(oldSnap, oldValue);

        assertThat(migrated.getArity()).isEqualTo(3);
        assertThat(migrated.getInt(0)).isEqualTo(1);
        assertThat(migrated.getString(1)).isEqualTo(fromString("x"));
        assertThat(migrated.isNullAt(2)).isTrue();
        assertThat(migrated.getRowKind()).isEqualTo(oldValue.getRowKind());
    }

    @Test
    void addNullableFieldInMiddle() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "c"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap =
                snapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                nullableInt(),
                                new BigIntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        GenericRowData oldValue = GenericRowData.of(1, 99L);
        RowData migrated = newSnap.migrate(oldSnap, oldValue);

        assertThat(migrated.getInt(0)).isEqualTo(1);
        assertThat(migrated.isNullAt(1)).isTrue();
        assertThat(migrated.getLong(2)).isEqualTo(99L);
    }

    @Test
    void reorderExistingFieldsByName() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"b", "a"}, new BigIntType(), new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        GenericRowData oldValue = GenericRowData.of(7, 42L);
        RowData migrated = newSnap.migrate(oldSnap, oldValue);

        // new position 0 == "b" (42L), new position 1 == "a" (7).
        assertThat(migrated.getLong(0)).isEqualTo(42L);
        assertThat(migrated.getInt(1)).isEqualTo(7);
    }

    /**
     * The key case for the ROW exemption in the type check: a nested ROW that gains a nullable
     * field has a different top-level {@code LogicalType}, so requiring {@code .equals} there would
     * wrongly reject the evolution. The nested recursion must validate it instead.
     */
    @Test
    void nestedRowEvolution() throws IOException {
        RowType oldNested = row(new String[] {"x", "y"}, new IntType(), VarCharType.STRING_TYPE);
        RowType newNested =
                row(
                        new String[] {"x", "y", "z"},
                        new IntType(),
                        VarCharType.STRING_TYPE,
                        nullableInt());
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"id", "nested"}, new IntType(), oldNested));
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"id", "nested"}, new IntType(), newNested));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        GenericRowData oldValue = GenericRowData.of(5, GenericRowData.of(8, fromString("hi")));
        RowData migrated = newSnap.migrate(oldSnap, oldValue);

        assertThat(migrated.getInt(0)).isEqualTo(5);
        RowData migratedNested = migrated.getRow(1, 3);
        assertThat(migratedNested.getInt(0)).isEqualTo(8);
        assertThat(migratedNested.getString(1)).isEqualTo(fromString("hi"));
        assertThat(migratedNested.isNullAt(2)).isTrue();
    }

    /**
     * The rejection counterpart to {@link #nestedRowEvolution}: a nested ROW with an incompatible
     * leaf change (a: INT -> BIGINT). The ROW exemption skips the top-level {@code .equals} on the
     * nested field, so the verdict depends on the nested recursion reporting incompatibility and
     * that propagating up to a top-level {@code incompatible()}.
     */
    @Test
    void rejectNestedRowIncompatibleChange() throws IOException {
        RowType oldNested = row(new String[] {"a"}, new IntType());
        RowType newNested = row(new String[] {"a"}, new BigIntType());
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"id", "nested"}, new IntType(), oldNested));
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"id", "nested"}, new IntType(), newNested));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void rejectFieldRemoval() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap = snapshot(row(new String[] {"a"}, new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void rejectLeafTypeChange() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new IntType()));
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void rejectRename() throws IOException {
        // "b" is removed and "c" added (with a differing type so the layout differs and the
        // name-based path runs): the old "b" has no name match in the new schema -> removal.
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"a", "c"}, new IntType(), nullableInt()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void rejectNonNullableAddedField() throws IOException {
        RowDataSerializerSnapshot oldSnap = snapshot(row(new String[] {"a"}, new IntType()));
        RowDataSerializerSnapshot newSnap =
                snapshot(
                        row(
                                new String[] {"a", "b"},
                                new IntType(),
                                new IntType(false))); // NOT NULL added field

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void namesLessDifferingLayoutIsIncompatible() throws IOException {
        // Old side is names-less (plain constructor); layouts differ -> no positional relaxation.
        RowDataSerializerSnapshot oldSnap =
                (RowDataSerializerSnapshot)
                        new RowDataSerializer(new IntType()).snapshotConfiguration();
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), nullableInt()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void identicalLayoutStaysCompatible() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap =
                snapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isFalse();
    }
}
