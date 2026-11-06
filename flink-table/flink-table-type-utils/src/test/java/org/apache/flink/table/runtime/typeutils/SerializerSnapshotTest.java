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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isCompatibleAsIs;
import static org.apache.flink.api.common.typeutils.TypeSerializerConditions.isIncompatible;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for serializer snapshots. */
class SerializerSnapshotTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("serializerSpecs")
    void sameTypesIsCompatibleAsIs(SerializerSpec<?> serializerSpec) throws Exception {
        assertThat(resolveCompatibility(serializerSpec, new IntType(false), new IntType(false)))
                .is(isCompatibleAsIs());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("serializerSpecs")
    void differentLogicalTypeIsIncompatible(SerializerSpec<?> serializerSpec) throws Exception {
        assertThat(resolveCompatibility(serializerSpec, new IntType(false), new BigIntType(false)))
                .is(isIncompatible());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("serializerSpecs")
    void wideningNullabilityInNestedRowIsCompatible(SerializerSpec<?> serializerSpec)
            throws Exception {
        assertThat(resolveCompatibility(serializerSpec, nestedRowType(false), nestedRowType(true)))
                .is(isCompatibleAsIs());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("serializerSpecs")
    void narrowingNullabilityInNestedRowIsIncompatible(SerializerSpec<?> serializerSpec)
            throws Exception {
        assertThat(resolveCompatibility(serializerSpec, nestedRowType(true), nestedRowType(false)))
                .is(isIncompatible());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("serializerCascadingTypes")
    void wideningNullabilityInCascadingTypeIsCompatible(
            SerializerSpec<?> serializerSpec, LogicalType previousType, LogicalType currentType)
            throws Exception {
        assertThat(resolveCompatibility(serializerSpec, previousType, currentType))
                .is(isCompatibleAsIs());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("serializerCascadingTypes")
    void narrowingNullabilityInCascadingTypeIsIncompatible(
            SerializerSpec<?> serializerSpec, LogicalType previousType, LogicalType currentType)
            throws Exception {
        assertThat(resolveCompatibility(serializerSpec, currentType, previousType))
                .is(isIncompatible());
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    /**
     * Round-trips the previous serializer's snapshot through serialization and resolves
     * compatibility against the current serializer's snapshot.
     */
    private static <T> TypeSerializerSchemaCompatibility<T> resolveCompatibility(
            SerializerSpec<T> serializerSpec, LogicalType previousType, LogicalType currentType)
            throws Exception {
        TypeSerializer<T> previous = serializerSpec.create(previousType);
        TypeSerializer<T> current = serializerSpec.create(currentType);
        TypeSerializerSnapshot<T> previousSnapshot = previous.snapshotConfiguration();

        DataOutputSerializer out = new DataOutputSerializer(64);
        previousSnapshot.writeSnapshot(out);

        TypeSerializerSnapshot<T> restoredPreviousSnapshot = serializerSpec.createSnapshot();
        restoredPreviousSnapshot.readSnapshot(
                previousSnapshot.getCurrentVersion(),
                new DataInputDeserializer(out.getCopyOfBuffer()),
                SerializerSnapshotTest.class.getClassLoader());

        return current.snapshotConfiguration().resolveSchemaCompatibility(restoredPreviousSnapshot);
    }

    private static Stream<Arguments> serializerCascadingTypes() {
        return serializerSpecs()
                .flatMap(
                        serializerSpec ->
                                cascadingLogicalTypes()
                                        .map(typeSpec -> typeSpec.toArguments(serializerSpec)));
    }

    private static Stream<SerializerSpec<?>> serializerSpecs() {
        return Stream.of(
                new SerializerSpec<>(
                        "ARRAY",
                        ArrayDataSerializer::new,
                        ArrayDataSerializer.ArrayDataSerializerSnapshot::new),
                new SerializerSpec<>(
                        "ROW",
                        RowDataSerializer::new,
                        RowDataSerializer.RowDataSerializerSnapshot::new),
                new SerializerSpec<>(
                        "MAP_KEY",
                        SerializerSnapshotTest::mapKeySerializer,
                        MapDataSerializer.MapDataSerializerSnapshot::new),
                new SerializerSpec<>(
                        "MAP_VALUE",
                        SerializerSnapshotTest::mapValueSerializer,
                        MapDataSerializer.MapDataSerializerSnapshot::new));
    }

    private static MapDataSerializer mapKeySerializer(LogicalType keyType) {
        return new MapDataSerializer(keyType, new IntType(false));
    }

    private static MapDataSerializer mapValueSerializer(LogicalType valueType) {
        return new MapDataSerializer(new VarCharType(false, 5), valueType);
    }

    private static Stream<CascadingLogicalTypeSpec> cascadingLogicalTypes() {
        return Stream.of(
                // Row with every nested nullability changed.
                new CascadingLogicalTypeSpec(cascadingRowType(false), cascadingRowType(true)),
                // Row with only leaf nullability changed.
                new CascadingLogicalTypeSpec(
                        cascadingRowTypeWithLeafNullability(false),
                        cascadingRowTypeWithLeafNullability(true)),
                // Array with cascading element type nullability changed.
                new CascadingLogicalTypeSpec(cascadingArrayType(false), cascadingArrayType(true)),
                // Map with cascading key and value type nullability changed.
                new CascadingLogicalTypeSpec(cascadingMapType(false), cascadingMapType(true)),
                // Multiset with cascading element type nullability changed.
                new CascadingLogicalTypeSpec(
                        cascadingMultisetType(false), cascadingMultisetType(true)),
                // Structured type with cascading attribute type nullability changed.
                new CascadingLogicalTypeSpec(
                        cascadingStructuredType(false), cascadingStructuredType(true)),
                // Distinct type with cascading source type nullability changed.
                new CascadingLogicalTypeSpec(
                        cascadingDistinctType(false), cascadingDistinctType(true)));
    }

    private static RowType nestedRowType(boolean isNullable) {
        return RowType.of(
                new LogicalType[] {new IntType(isNullable), new VarCharType(false, 100)},
                new String[] {"a", "b"});
    }

    private static RowType cascadingRowType(boolean isNullable) {
        return new RowType(
                isNullable,
                Arrays.asList(
                        new RowField(
                                "arrayField",
                                new ArrayType(
                                        isNullable,
                                        new RowType(
                                                isNullable,
                                                Arrays.asList(
                                                        new RowField(
                                                                "element",
                                                                new IntType(isNullable)))))),
                        new RowField(
                                "mapField",
                                new MapType(
                                        isNullable,
                                        new VarCharType(isNullable, 5),
                                        new RowType(
                                                isNullable,
                                                Arrays.asList(
                                                        new RowField(
                                                                "value",
                                                                new BigIntType(isNullable)))))),
                        new RowField(
                                "multisetField",
                                new MultisetType(
                                        isNullable,
                                        new RowType(
                                                isNullable,
                                                Arrays.asList(
                                                        new RowField(
                                                                "nested",
                                                                new BooleanType(isNullable))))))));
    }

    private static RowType cascadingRowTypeWithLeafNullability(boolean isNullable) {
        return new RowType(
                false,
                Arrays.asList(
                        new RowField(
                                "arrayField",
                                new ArrayType(
                                        false,
                                        new RowType(
                                                false,
                                                Arrays.asList(
                                                        new RowField(
                                                                "element",
                                                                new IntType(isNullable)))))),
                        new RowField(
                                "mapField",
                                new MapType(
                                        false,
                                        new VarCharType(false, 5),
                                        new RowType(
                                                false,
                                                Arrays.asList(
                                                        new RowField(
                                                                "value",
                                                                new BigIntType(isNullable)))))),
                        new RowField(
                                "multisetField",
                                new MultisetType(
                                        false,
                                        new RowType(
                                                false,
                                                Arrays.asList(
                                                        new RowField(
                                                                "nested",
                                                                new BooleanType(isNullable))))))));
    }

    private static ArrayType cascadingArrayType(boolean isNullable) {
        return new ArrayType(isNullable, cascadingRowType(isNullable));
    }

    private static MapType cascadingMapType(boolean isNullable) {
        return new MapType(
                isNullable, new VarCharType(isNullable, 5), cascadingRowType(isNullable));
    }

    private static MultisetType cascadingMultisetType(boolean isNullable) {
        return new MultisetType(isNullable, cascadingRowType(isNullable));
    }

    private static StructuredType cascadingStructuredType(boolean isNullable) {
        return StructuredType.newBuilder(
                        ObjectIdentifier.of("cat", "db", "CascadingStructuredType"))
                .attributes(
                        Arrays.asList(
                                new StructuredAttribute("field", cascadingArrayType(isNullable))))
                .setNullable(isNullable)
                .build();
    }

    private static DistinctType cascadingDistinctType(boolean isNullable) {
        return DistinctType.newBuilder(
                        ObjectIdentifier.of("cat", "db", "CascadingDistinctType"),
                        cascadingRowType(isNullable))
                .build();
    }

    private static final class SerializerSpec<T> {
        private final String name;
        private final Function<LogicalType, TypeSerializer<T>> serializerFactory;
        private final Supplier<TypeSerializerSnapshot<T>> snapshotFactory;

        private SerializerSpec(
                String name,
                Function<LogicalType, TypeSerializer<T>> serializerFactory,
                Supplier<TypeSerializerSnapshot<T>> snapshotFactory) {
            this.name = name;
            this.serializerFactory = serializerFactory;
            this.snapshotFactory = snapshotFactory;
        }

        private TypeSerializer<T> create(LogicalType type) {
            return serializerFactory.apply(type);
        }

        private TypeSerializerSnapshot<T> createSnapshot() {
            return snapshotFactory.get();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final class CascadingLogicalTypeSpec {
        private final LogicalType previousType;
        private final LogicalType currentType;

        private CascadingLogicalTypeSpec(LogicalType previousType, LogicalType currentType) {
            this.previousType = previousType;
            this.currentType = currentType;
        }

        private Arguments toArguments(SerializerSpec<?> serializerSpec) {
            return Arguments.of(serializerSpec, previousType, currentType);
        }
    }
}
