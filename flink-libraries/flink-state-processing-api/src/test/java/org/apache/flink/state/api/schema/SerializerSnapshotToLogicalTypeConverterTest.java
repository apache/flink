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

package org.apache.flink.state.api.schema;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.example.state.writer.job.schema.avro.AvroRecord;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SerializerSnapshotToLogicalTypeConverter}. */
class SerializerSnapshotToLogicalTypeConverterTest {

    // -------------------------------------------------------------------------
    // POJO classes
    // -------------------------------------------------------------------------

    /** Simple POJO used to exercise field-by-field POJO schema extraction. */
    public static class SimplePojo {
        public String name;
        public int age;
        public long score;
        public boolean active;
    }

    /** POJO with a nested POJO field, used to exercise recursive schema extraction. */
    public static class NestedPojo {
        public String label;
        public SimplePojo inner;
    }

    // -------------------------------------------------------------------------
    // Primitive / scalar snapshots
    // -------------------------------------------------------------------------

    @Test
    void testPrimitives() {
        List<PrimitiveCase> cases =
                Arrays.asList(
                        new PrimitiveCase(
                                IntSerializer.IntSerializerSnapshot::new, LogicalTypeRoot.INTEGER),
                        new PrimitiveCase(
                                LongSerializer.LongSerializerSnapshot::new, LogicalTypeRoot.BIGINT),
                        new PrimitiveCase(
                                FloatSerializer.FloatSerializerSnapshot::new,
                                LogicalTypeRoot.FLOAT),
                        new PrimitiveCase(
                                DoubleSerializer.DoubleSerializerSnapshot::new,
                                LogicalTypeRoot.DOUBLE),
                        new PrimitiveCase(
                                BooleanSerializer.BooleanSerializerSnapshot::new,
                                LogicalTypeRoot.BOOLEAN),
                        new PrimitiveCase(
                                StringSerializer.StringSerializerSnapshot::new,
                                LogicalTypeRoot.VARCHAR));

        for (PrimitiveCase c : cases) {
            var snapshot = c.snapshotSupplier.get();
            LogicalType t = convert(snapshot);
            assertThat(t.getTypeRoot())
                    .as("wrong type root for %s", snapshot.getClass().getSimpleName())
                    .isEqualTo(c.expectedRoot);
        }

        // Numeric primitives are non-nullable at the wire level; spot-check one representative.
        assertThat(convert(new IntSerializer.IntSerializerSnapshot()).isNullable()).isFalse();
    }

    private static final class PrimitiveCase {
        final Supplier<org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?>>
                snapshotSupplier;
        final LogicalTypeRoot expectedRoot;

        PrimitiveCase(
                Supplier<org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?>>
                        snapshotSupplier,
                LogicalTypeRoot expectedRoot) {
            this.snapshotSupplier = snapshotSupplier;
            this.expectedRoot = expectedRoot;
        }
    }

    // -------------------------------------------------------------------------
    // Composite types
    // -------------------------------------------------------------------------

    @Test
    void testListOfString() {
        ListSerializer<String> ser = new ListSerializer<>(StringSerializer.INSTANCE);
        LogicalType t = convert(ser.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ARRAY);
        ArrayType at = (ArrayType) t;
        assertThat(at.getElementType().getTypeRoot()).isEqualTo(LogicalTypeRoot.VARCHAR);
    }

    @Test
    void testMapStringToLong() {
        MapSerializer<String, Long> ser =
                new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        LogicalType t = convert(ser.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.MAP);
        MapType mt = (MapType) t;
        assertThat(mt.getKeyType().getTypeRoot()).isEqualTo(LogicalTypeRoot.VARCHAR);
        assertThat(mt.getValueType().getTypeRoot()).isEqualTo(LogicalTypeRoot.BIGINT);
    }

    @Test
    void testNullableWrapsNestedTypeAsNullable() {
        // LongSerializer alone always maps to a non-nullable BIGINT (see testPrimitives); wrapping
        // it in NullableSerializer must flip only the nullability, not the underlying type.
        TypeSerializer<Long> wrapped = NullableSerializer.wrap(LongSerializer.INSTANCE, true);
        LogicalType t = convert(wrapped.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.BIGINT);
        assertThat(t.isNullable()).isTrue();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testTuple() {
        TupleSerializer<Tuple2<Integer, String>> ser =
                new TupleSerializer<>(
                        (Class) Tuple2.class,
                        new TypeSerializer<?>[] {
                            IntSerializer.INSTANCE, StringSerializer.INSTANCE
                        });
        LogicalType t = convert(ser.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(2);
        // Tuples have no field names in the serializer snapshot, so fields fall back to
        // positional names, same as the RowData-without-field-names case.
        assertField(rt, "f0", LogicalTypeRoot.INTEGER);
        assertField(rt, "f1", LogicalTypeRoot.VARCHAR);
    }

    // -------------------------------------------------------------------------
    // POJO types
    // -------------------------------------------------------------------------

    @Test
    void testSimplePojo() {
        LogicalType t = convertPojoType(SimplePojo.class);
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(4);

        assertField(rt, "name", LogicalTypeRoot.VARCHAR);
        assertField(rt, "age", LogicalTypeRoot.INTEGER);
        assertField(rt, "score", LogicalTypeRoot.BIGINT);
        assertField(rt, "active", LogicalTypeRoot.BOOLEAN);
    }

    @Test
    void testNestedPojo() {
        LogicalType t = convertPojoType(NestedPojo.class);
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(2);
        assertField(rt, "label", LogicalTypeRoot.VARCHAR);

        // The nested 'inner' field should map to ROW
        RowType.RowField innerField = findField(rt, "inner");
        assertThat(innerField).isNotNull();
        assertThat(innerField.getType().getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType innerRow = (RowType) innerField.getType();
        assertField(innerRow, "name", LogicalTypeRoot.VARCHAR);
        assertField(innerRow, "age", LogicalTypeRoot.INTEGER);
    }

    @Test
    void testPojoFieldNamesPreservedWithoutClass() {
        // Even without the POJO class on the classpath, field names should be available.
        var snapshot = buildPojoSnapshot(SimplePojo.class);
        // Field name extraction happens via the snapshot, no class needed
        LogicalType t = SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
        RowType rt = (RowType) t;
        List<String> names = rt.getFieldNames();
        assertThat(names).contains("name", "age", "score", "active");
    }

    // -------------------------------------------------------------------------
    // Avro types
    // -------------------------------------------------------------------------

    @Test
    void testAvroSpecificRecord() {
        // AvroRecord has one field: longData (long)
        LogicalType t = convertAvroSpecificType(AvroRecord.class);
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(1);
        assertField(rt, "longData", LogicalTypeRoot.BIGINT);
    }

    @Test
    void testAvroGenericRecord() {
        // convertAvro() reads only the embedded writer Schema (see
        // SerializerSnapshotToLogicalTypeConverter#convertAvro), so this also covers the
        // "specific record class missing at read time" fallback: AvroSerializerSnapshot degrades
        // to GenericRecord.class in that case, but the schema-derived RowType is identical either
        // way. The actual missing-class read path is covered end-to-end by
        // StateCatalogGeneratedSavepointITCase#testReadAvroKeyedStateFromSchemaDiscovery.
        Schema schema = AvroRecord.getClassSchema();
        LogicalType t = convertAvroGenericType(schema);
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(1);
        assertField(rt, "longData", LogicalTypeRoot.BIGINT);
    }

    // -------------------------------------------------------------------------
    // RowData types
    // -------------------------------------------------------------------------

    @Test
    void testRowDataWithFieldNames() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(), VarCharType.STRING_TYPE},
                        new String[] {"id", "name"});
        RowDataSerializer serializer = new RowDataSerializer(rowType);

        LogicalType t = convert(serializer.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(2);
        assertField(rt, "id", LogicalTypeRoot.INTEGER);
        assertField(rt, "name", LogicalTypeRoot.VARCHAR);
    }

    @Test
    void testRowDataWithoutFieldNamesFallsBackToPositional() {
        // Many production call sites (e.g. window operators) build a RowDataSerializer from a
        // bare LogicalType[], so no field names are available. The converter must still produce
        // a usable RowType, falling back to positional names like the Tuple case.
        RowDataSerializer serializer = new RowDataSerializer(new IntType(), new BigIntType());

        LogicalType t = convert(serializer.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(2);
        assertField(rt, "f0", LogicalTypeRoot.INTEGER);
        assertField(rt, "f1", LogicalTypeRoot.BIGINT);
    }

    @Test
    void testNestedRowData() {
        RowType innerType =
                RowType.of(
                        new LogicalType[] {new IntType(), VarCharType.STRING_TYPE},
                        new String[] {"innerId", "innerName"});
        RowType outerType =
                RowType.of(
                        new LogicalType[] {VarCharType.STRING_TYPE, innerType},
                        new String[] {"label", "inner"});
        RowDataSerializer serializer = new RowDataSerializer(outerType);

        LogicalType t = convert(serializer.snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(2);
        assertField(rt, "label", LogicalTypeRoot.VARCHAR);

        RowType.RowField innerField = findField(rt, "inner");
        assertThat(innerField).isNotNull();
        assertThat(innerField.getType().getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        RowType innerRow = (RowType) innerField.getType();
        assertField(innerRow, "innerId", LogicalTypeRoot.INTEGER);
        assertField(innerRow, "innerName", LogicalTypeRoot.VARCHAR);
    }

    // -------------------------------------------------------------------------
    // Window namespace types
    // -------------------------------------------------------------------------

    @Test
    void testTimeWindow() {
        LogicalType t =
                convert(
                        new org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer()
                                .snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        assertThat(t.isNullable()).isFalse();
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(2);
        assertField(rt, "window_start", LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        assertField(rt, "window_end", LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }

    @Test
    void testGlobalWindow() {
        LogicalType t =
                convert(
                        new org.apache.flink.streaming.api.windowing.windows.GlobalWindow
                                        .Serializer()
                                .snapshotConfiguration());
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        assertThat(t.isNullable()).isFalse();
        RowType rt = (RowType) t;
        assertThat(rt.getFieldCount()).isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // Null / unknown snapshot
    // -------------------------------------------------------------------------

    @Test
    void testNullSnapshot() {
        LogicalType t = SerializerSnapshotToLogicalTypeConverter.convert(null);
        assertThat(t.getTypeRoot()).isEqualTo(LogicalTypeRoot.VARBINARY);
    }

    @Test
    void testVoidNamespaceSnapshotUnsupported() {
        // VoidNamespace is filtered out by StateTableUtils before ever reaching the converter
        // (plain per-key state has no namespace to convert); confirm it stays unsupported here.
        assertThatThrownBy(
                        () ->
                                convert(
                                        new org.apache.flink.runtime.state.VoidNamespaceSerializer()
                                                .snapshotConfiguration()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static LogicalType convert(
            org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?> snapshot) {
        return SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static LogicalType convertPojoType(Class<?> pojoClass) {
        var snapshot = buildPojoSnapshot(pojoClass);
        return SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?>
            buildPojoSnapshot(Class<?> clazz) {
        return TypeExtractor.createTypeInfo(clazz)
                .createSerializer(new SerializerConfigImpl())
                .snapshotConfiguration();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T extends org.apache.avro.specific.SpecificRecordBase>
            LogicalType convertAvroSpecificType(Class<T> avroClass) {
        return SerializerSnapshotToLogicalTypeConverter.convert(
                new AvroTypeInfo<>(avroClass)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration());
    }

    private static LogicalType convertAvroGenericType(Schema schema) {
        return SerializerSnapshotToLogicalTypeConverter.convert(
                new GenericRecordAvroTypeInfo(schema)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration());
    }

    private static void assertField(RowType row, String name, LogicalTypeRoot expectedRoot) {
        RowType.RowField field = findField(row, name);
        assertThat(field).as("Field '%s' not found in row type", name).isNotNull();
        assertThat(field.getType().getTypeRoot())
                .as("Wrong type for field '%s'", name)
                .isEqualTo(expectedRoot);
    }

    private static RowType.RowField findField(RowType row, String name) {
        return row.getFields().stream()
                .filter(f -> f.getName().equals(name))
                .findFirst()
                .orElse(null);
    }
}
