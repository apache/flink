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

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.StateSchemaEvolvingSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer.RowDataSerializerSnapshot;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the opt-in state schema evolution carried by {@link RowDataSerializer}: the two evolution
 * bits on the serializer, and the compatibility verdict plus {@code migrate} remap performed by
 * {@link RowDataSerializerSnapshot} once the state bit is set.
 *
 * <p>Complements {@link RowDataSerializerFieldNamesTest}, which covers the field-name metadata and
 * the V4 snapshot format that this evolution builds on, and {@link RowDataSerializerTest}, which
 * covers per-record serialization round trips.
 */
class RowDataSerializerSchemaEvolutionTest {

    /**
     * Mirrors {@code ExecutionConfigOptions.TABLE_EXEC_STATE_SCHEMA_EVOLUTION_ENABLED}, which lives
     * in a module this one cannot depend on.
     */
    private static final ConfigOption<Boolean> STATE_SCHEMA_EVOLUTION_ENABLED =
            ConfigOptions.key("table.exec.state.schema-evolution.enabled")
                    .booleanType()
                    .defaultValue(false);

    // ------------------------------------------------------------------------
    //  Name-based evolution
    // ------------------------------------------------------------------------

    @Test
    void addedNullableFieldAtEndIsMigrated() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), VarCharType.STRING_TYPE));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                VarCharType.STRING_TYPE,
                                new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        GenericRowData oldValue = GenericRowData.of(1, fromString("x"));
        oldValue.setRowKind(RowKind.UPDATE_AFTER);
        RowData migrated = newSnap.migrate(oldSnap, oldValue);

        assertThat(migrated.getArity()).isEqualTo(3);
        assertThat(migrated.getInt(0)).isEqualTo(1);
        assertThat(migrated.getString(1)).isEqualTo(fromString("x"));
        assertThat(migrated.isNullAt(2)).isTrue();
        assertThat(migrated.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
    }

    @Test
    void addedNullableFieldInMiddleIsMigrated() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "c"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                new IntType(),
                                new BigIntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        RowData migrated = newSnap.migrate(oldSnap, GenericRowData.of(1, 99L));

        assertThat(migrated.getInt(0)).isEqualTo(1);
        assertThat(migrated.isNullAt(1)).isTrue();
        assertThat(migrated.getLong(2)).isEqualTo(99L);
    }

    @Test
    void reorderedFieldsAreMigratedByName() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(row(new String[] {"b", "a"}, new BigIntType(), new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        RowData migrated = newSnap.migrate(oldSnap, GenericRowData.of(7, 42L));

        assertThat(migrated.getLong(0)).isEqualTo(42L);
        assertThat(migrated.getInt(1)).isEqualTo(7);
    }

    /**
     * The reorder that {@link #reorderedFieldsAreMigratedByName} cannot catch: with the same type
     * at every position the {@code LogicalType[]} arrays are equal, so only a name comparison
     * distinguishes this from an unchanged layout. Resolving it as needing no migration would leave
     * every value under its neighbour's name.
     */
    @Test
    void sameTypedReorderIsMigratedByName() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), new IntType()));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(row(new String[] {"b", "a"}, new IntType(), new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        RowData migrated = newSnap.migrate(oldSnap, GenericRowData.of(7, 42));

        assertThat(migrated.getInt(0)).isEqualTo(42);
        assertThat(migrated.getInt(1)).isEqualTo(7);
    }

    /** A rename among same-typed fields drops the old field, so it cannot be migrated. */
    @Test
    void sameTypedRenameIsIncompatible() throws IOException {
        RowDataSerializerSnapshot oldSnap = oldSnapshot(row(new String[] {"a"}, new IntType()));
        RowDataSerializerSnapshot newSnap = newSnapshot(row(new String[] {"x"}, new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    /**
     * With the option off, field names are not consulted at all: an identical positional layout
     * resolves exactly as it does without this feature, whatever the names did.
     */
    @Test
    void sameTypedReorderIsCompatibleAsIsWhenNotOptedIn() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), new IntType()));
        RowDataSerializerSnapshot unarmedNewSnap =
                (RowDataSerializerSnapshot)
                        InternalSerializers.create(
                                        row(new String[] {"b", "a"}, new IntType(), new IntType()))
                                .snapshotConfiguration();

        assertThat(unarmedNewSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAsIs()).isTrue();
    }

    /**
     * A prior snapshot from before field names were persisted names nothing, so a reorder cannot be
     * detected there and identical types must keep meaning "no migration needed". Reporting a
     * migration instead would rewrite every entry of an unchanged state on the first restore after
     * a user opts in, which is the most common path this option will meet.
     */
    @Test
    void nameLessOldSnapshotWithUnchangedLayoutIsCompatibleAsIs() {
        RowDataSerializerSnapshot oldSnap = nameLessSnapshot(new IntType(), new BigIntType());
        RowDataSerializerSnapshot newSnap =
                newSnapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAsIs()).isTrue();
    }

    /**
     * The mirror case: a name-less new side against a named prior snapshot with identical types.
     * Opting in must never narrow a restore that succeeds with the option off.
     */
    @Test
    void nameLessNewSnapshotWithUnchangedLayoutIsCompatibleAsIs() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializer nameLessArmed =
                (RowDataSerializer)
                        new RowDataSerializer(new IntType(), new BigIntType())
                                .withSchemaEvolutionAllowed()
                                .withStateSchemaEvolution();
        RowDataSerializerSnapshot newSnap =
                (RowDataSerializerSnapshot) nameLessArmed.snapshotConfiguration();

        assertThat(nameLessArmed.isStateSchemaEvolutionEnabled()).isTrue();
        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAsIs()).isTrue();
    }

    /**
     * An unchanged layout can still reach {@code migrate}, because a nested serializer may report
     * {@code compatibleAfterMigration} on its own. The remap must then reproduce the row exactly.
     */
    @Test
    void migrateReproducesAnUnchangedLayout() throws IOException {
        RowType rowType = row(new String[] {"a", "b"}, new IntType(), VarCharType.STRING_TYPE);

        RowData migrated =
                newSnapshot(rowType)
                        .migrate(oldSnapshot(rowType), GenericRowData.of(1, fromString("x")));

        assertThat(migrated.getArity()).isEqualTo(2);
        assertThat(migrated.getInt(0)).isEqualTo(1);
        assertThat(migrated.getString(1)).isEqualTo(fromString("x"));
    }

    @Test
    void addedNotNullFieldIsIncompatible() throws IOException {
        RowDataSerializerSnapshot oldSnap = oldSnapshot(row(new String[] {"a"}, new IntType()));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(row(new String[] {"a", "b"}, new IntType(), new IntType(false)));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void droppedFieldIsIncompatible() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializerSnapshot newSnap = newSnapshot(row(new String[] {"a"}, new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void changedLeafTypeIsIncompatible() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"a", "b"}, new IntType(), new IntType()));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void evolvedNestedRowIsMigrated() throws IOException {
        RowType oldNested = row(new String[] {"x", "y"}, new IntType(), VarCharType.STRING_TYPE);
        RowType newNested =
                row(
                        new String[] {"x", "y", "z"},
                        new IntType(),
                        VarCharType.STRING_TYPE,
                        new IntType());
        RowType newType = row(new String[] {"id", "nested"}, new IntType(), newNested);

        RowDataSerializer stateSerializer =
                (RowDataSerializer)
                        StateSchemaEvolvingSerializer.armStateValueSerializer(optedIn(newType));

        // Nested field serializers are built by InternalSerializers inside the RowDataSerializer
        // constructor, so they never carry the opt-in bit. The state bit therefore has to be set on
        // them unconditionally down the ROW spine; re-checking the opt-in bit one level down would
        // leave the nested snapshot unarmed and short-circuit the whole row to incompatible.
        RowDataSerializer nestedSerializer =
                (RowDataSerializer) stateSerializer.fieldSerializers()[1];
        assertThat(nestedSerializer.isSchemaEvolutionAllowed()).isFalse();
        assertThat(nestedSerializer.isStateSchemaEvolutionEnabled()).isTrue();

        RowDataSerializerSnapshot newSnap =
                (RowDataSerializerSnapshot) stateSerializer.snapshotConfiguration();
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(row(new String[] {"id", "nested"}, new IntType(), oldNested));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        RowData migrated =
                newSnap.migrate(
                        oldSnap, GenericRowData.of(5, GenericRowData.of(8, fromString("hi"))));

        assertThat(migrated.getInt(0)).isEqualTo(5);
        RowData migratedNested = migrated.getRow(1, 3);
        assertThat(migratedNested.getInt(0)).isEqualTo(8);
        assertThat(migratedNested.getString(1)).isEqualTo(fromString("hi"));
        assertThat(migratedNested.isNullAt(2)).isTrue();
    }

    @Test
    void incompatibleNestedRowChangeIsIncompatible() throws IOException {
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(
                        row(
                                new String[] {"id", "nested"},
                                new IntType(),
                                row(new String[] {"a"}, new IntType())));
        RowDataSerializerSnapshot newSnap =
                newSnapshot(
                        row(
                                new String[] {"id", "nested"},
                                new IntType(),
                                row(new String[] {"a"}, new BigIntType())));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void changedArrayElementRowIsIncompatible() throws IOException {
        RowType oldElement = row(new String[] {"a"}, new IntType());
        RowType newElement = row(new String[] {"a"}, new BigIntType());
        RowType newType = row(new String[] {"id", "arr"}, new IntType(), new ArrayType(newElement));

        // The recursion descends only into ROW-typed fields, so the ROW nested under the ARRAY is
        // never reached and the change has to be rejected by the leaf type-equality check instead.
        RowDataSerializer stateSerializer = armed(newType);
        ArrayDataSerializer arraySerializer =
                (ArrayDataSerializer) stateSerializer.fieldSerializers()[1];
        RowDataSerializer elementSerializer = (RowDataSerializer) arraySerializer.getEleSer();
        assertThat(elementSerializer.isStateSchemaEvolutionEnabled()).isFalse();

        RowDataSerializerSnapshot newSnap =
                (RowDataSerializerSnapshot) stateSerializer.snapshotConfiguration();
        RowDataSerializerSnapshot oldSnap =
                oldSnapshot(
                        row(new String[] {"id", "arr"}, new IntType(), new ArrayType(oldElement)));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Opt-in semantics
    // ------------------------------------------------------------------------

    @Test
    void unarmedNewSnapshotIsIncompatible() throws IOException {
        RowType newType = row(new String[] {"a", "b"}, new IntType(), new IntType());
        RowDataSerializer notOptedIn =
                (RowDataSerializer) InternalTypeInfo.of(newType).createSerializer(config(false));
        RowDataSerializerSnapshot newSnap =
                (RowDataSerializerSnapshot)
                        notOptedIn.withStateSchemaEvolution().snapshotConfiguration();

        assertThat(
                        newSnap.resolveSchemaCompatibility(
                                        oldSnapshot(row(new String[] {"a"}, new IntType())))
                                .isIncompatible())
                .isTrue();
    }

    @Test
    void armedBitIsNotPersisted() throws IOException {
        RowType oldType = row(new String[] {"a"}, new IntType());
        RowType newType = row(new String[] {"a", "b"}, new IntType(), new IntType());

        assertThat(
                        newSnapshot(newType)
                                .resolveSchemaCompatibility(oldSnapshot(oldType))
                                .isCompatibleAfterMigration())
                .isTrue();

        RowDataSerializerSnapshot restored = roundTrip(armed(newType).snapshotConfiguration());

        assertThat(restored.resolveSchemaCompatibility(oldSnapshot(oldType)).isIncompatible())
                .isTrue();
    }

    @Test
    void withStateSchemaEvolutionIsIdentityWhenNotOptedIn() {
        RowDataSerializer serializer =
                InternalSerializers.create(
                        row(new String[] {"a", "b"}, new IntType(), new BigIntType()));

        assertThat(serializer.withStateSchemaEvolution()).isSameAs(serializer);
    }

    @Test
    void optingInDoesNotMutateTheCachedSerializer() {
        InternalTypeInfo<RowData> typeInfo =
                InternalTypeInfo.of(row(new String[] {"a", "b"}, new IntType(), new BigIntType()));
        RowDataSerializer cached = typeInfo.toRowSerializer();

        RowDataSerializer optedIn = (RowDataSerializer) typeInfo.createSerializer(config(true));

        assertThat(optedIn).isNotSameAs(cached);
        assertThat(optedIn.isSchemaEvolutionAllowed()).isTrue();
        assertThat(cached.isSchemaEvolutionAllowed()).isFalse();
        assertThat(typeInfo.createSerializer(config(false))).isSameAs(cached);
    }

    @Test
    void duplicateCarriesBothEvolutionBits() {
        RowType rowType = row(new String[] {"a", "b"}, new IntType(), new BigIntType());

        RowDataSerializer armedDuplicate = (RowDataSerializer) armed(rowType).duplicate();
        assertThat(armedDuplicate.isSchemaEvolutionAllowed()).isTrue();
        assertThat(armedDuplicate.isStateSchemaEvolutionEnabled()).isTrue();

        RowDataSerializer plainDuplicate =
                (RowDataSerializer) InternalSerializers.create(rowType).duplicate();
        assertThat(plainDuplicate.isSchemaEvolutionAllowed()).isFalse();
        assertThat(plainDuplicate.isStateSchemaEvolutionEnabled()).isFalse();
    }

    @Test
    void equalsAndHashCodeIgnoreEvolutionBits() {
        RowType rowType = row(new String[] {"a", "b"}, new IntType(), new BigIntType());
        RowDataSerializer plain = InternalSerializers.create(rowType);

        assertThat(armed(rowType)).isEqualTo(plain);
        assertThat(armed(rowType).hashCode()).isEqualTo(plain.hashCode());
    }

    // ------------------------------------------------------------------------
    //  Positional fallback for a prior snapshot written without field names
    // ------------------------------------------------------------------------

    @Test
    void positionalAppendedNullableFieldIsMigrated() {
        RowDataSerializerSnapshot oldSnap = nameLessSnapshot(new IntType(), new BigIntType());
        RowDataSerializerSnapshot newSnap =
                newSnapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                new BigIntType(),
                                new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isCompatibleAfterMigration())
                .isTrue();

        RowData migrated = newSnap.migrate(oldSnap, GenericRowData.of(1, 2L));

        assertThat(migrated.getArity()).isEqualTo(3);
        assertThat(migrated.getInt(0)).isEqualTo(1);
        assertThat(migrated.getLong(1)).isEqualTo(2L);
        assertThat(migrated.isNullAt(2)).isTrue();
    }

    @Test
    void positionalAppendedNotNullFieldIsIncompatible() {
        RowDataSerializerSnapshot oldSnap = nameLessSnapshot(new IntType(), new BigIntType());
        RowDataSerializerSnapshot newSnap =
                newSnapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                new BigIntType(),
                                new IntType(false)));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void positionalRetypedFieldIsIncompatible() {
        RowDataSerializerSnapshot oldSnap = nameLessSnapshot(new IntType(), new BigIntType());
        RowDataSerializerSnapshot newSnap =
                newSnapshot(
                        row(
                                new String[] {"a", "b", "c"},
                                new IntType(),
                                new IntType(),
                                new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void positionalDroppedFieldIsIncompatible() {
        RowDataSerializerSnapshot oldSnap = nameLessSnapshot(new IntType(), new BigIntType());
        RowDataSerializerSnapshot newSnap = newSnapshot(row(new String[] {"a"}, new IntType()));

        assertThat(newSnap.resolveSchemaCompatibility(oldSnap).isIncompatible()).isTrue();
    }

    @Test
    void nameLessNewSnapshotIsIncompatible() throws IOException {
        // A STRUCTURED_TYPE resolves to a RowDataSerializer built from field types only, so it can
        // be opted in and armed while still carrying no field names.
        StructuredType structuredType =
                StructuredType.newBuilder("org.apache.flink.table.NameLess")
                        .attributes(
                                Arrays.asList(
                                        new StructuredType.StructuredAttribute("a", new IntType()),
                                        new StructuredType.StructuredAttribute(
                                                "b", new BigIntType())))
                        .build();
        RowDataSerializer nameLess =
                (RowDataSerializer)
                        InternalTypeInfo.<RowData>of(structuredType).createSerializer(config(true));
        RowDataSerializer stateSerializer = (RowDataSerializer) nameLess.withStateSchemaEvolution();

        assertThat(stateSerializer.getFieldNames()).isNull();
        assertThat(stateSerializer.isStateSchemaEvolutionEnabled()).isTrue();

        RowDataSerializerSnapshot newSnap =
                (RowDataSerializerSnapshot) stateSerializer.snapshotConfiguration();

        assertThat(
                        newSnap.resolveSchemaCompatibility(
                                        oldSnapshot(row(new String[] {"a"}, new IntType())))
                                .isIncompatible())
                .isTrue();
    }

    // ------------------------------------------------------------------------

    private static RowType row(String[] names, LogicalType... types) {
        return RowType.of(types, names);
    }

    private static SerializerConfig config(boolean schemaEvolutionEnabled) {
        Configuration configuration = new Configuration();
        configuration.set(STATE_SCHEMA_EVOLUTION_ENABLED, schemaEvolutionEnabled);
        return new SerializerConfigImpl(configuration);
    }

    /** A serializer the job opted in, but that is not yet a state's own value serializer. */
    private static RowDataSerializer optedIn(RowType rowType) {
        return (RowDataSerializer) InternalTypeInfo.of(rowType).createSerializer(config(true));
    }

    private static RowDataSerializer armed(RowType rowType) {
        return (RowDataSerializer) optedIn(rowType).withStateSchemaEvolution();
    }

    /**
     * The new side of a resolution. Kept in memory rather than round-tripped, because the state bit
     * is not part of the snapshot format and a round trip clears it.
     */
    private static RowDataSerializerSnapshot newSnapshot(RowType rowType) {
        return (RowDataSerializerSnapshot) armed(rowType).snapshotConfiguration();
    }

    /** The old side of a resolution, read back from bytes the way a restore produces it. */
    private static RowDataSerializerSnapshot oldSnapshot(RowType rowType) throws IOException {
        return roundTrip(InternalSerializers.create(rowType).snapshotConfiguration());
    }

    /** A prior snapshot from before field names were persisted. */
    private static RowDataSerializerSnapshot nameLessSnapshot(LogicalType... types) {
        return (RowDataSerializerSnapshot) new RowDataSerializer(types).snapshotConfiguration();
    }

    private static RowDataSerializerSnapshot roundTrip(TypeSerializerSnapshot<RowData> snapshot)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return (RowDataSerializerSnapshot)
                TypeSerializerSnapshot.<RowData>readVersionedSnapshot(
                        in, RowDataSerializerSchemaEvolutionTest.class.getClassLoader());
    }
}
