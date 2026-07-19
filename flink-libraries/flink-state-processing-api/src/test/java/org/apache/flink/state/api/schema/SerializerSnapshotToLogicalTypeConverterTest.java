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
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/** Unit tests for {@link SerializerSnapshotToLogicalTypeConverter}. */
public class SerializerSnapshotToLogicalTypeConverterTest {

    // -------------------------------------------------------------------------
    // POJO classes
    // -------------------------------------------------------------------------

    public static class SimplePojo {
        public String name;
        public int age;
        public long score;
        public boolean active;
    }

    public static class NestedPojo {
        public String label;
        public SimplePojo inner;
    }

    // -------------------------------------------------------------------------
    // Primitive / scalar snapshots
    // -------------------------------------------------------------------------

    @Test
    public void testPrimitives() {
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
            Assert.assertEquals(
                    "wrong type root for " + snapshot.getClass().getSimpleName(),
                    c.expectedRoot,
                    t.getTypeRoot());
        }

        // Numeric primitives are non-nullable at the wire level; spot-check one representative.
        Assert.assertFalse(convert(new IntSerializer.IntSerializerSnapshot()).isNullable());
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
    public void testListOfString() {
        ListSerializer<String> ser = new ListSerializer<>(StringSerializer.INSTANCE);
        LogicalType t = convert(ser.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ARRAY, t.getTypeRoot());
        ArrayType at = (ArrayType) t;
        Assert.assertEquals(LogicalTypeRoot.VARCHAR, at.getElementType().getTypeRoot());
    }

    @Test
    public void testMapStringToLong() {
        MapSerializer<String, Long> ser =
                new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        LogicalType t = convert(ser.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.MAP, t.getTypeRoot());
        MapType mt = (MapType) t;
        Assert.assertEquals(LogicalTypeRoot.VARCHAR, mt.getKeyType().getTypeRoot());
        Assert.assertEquals(LogicalTypeRoot.BIGINT, mt.getValueType().getTypeRoot());
    }

    // -------------------------------------------------------------------------
    // POJO types
    // -------------------------------------------------------------------------

    @Test
    public void testSimplePojo() {
        LogicalType t = convertPojoType(SimplePojo.class);
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(4, rt.getFieldCount());

        assertField(rt, "name", LogicalTypeRoot.VARCHAR);
        assertField(rt, "age", LogicalTypeRoot.INTEGER);
        assertField(rt, "score", LogicalTypeRoot.BIGINT);
        assertField(rt, "active", LogicalTypeRoot.BOOLEAN);
    }

    @Test
    public void testNestedPojo() {
        LogicalType t = convertPojoType(NestedPojo.class);
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(2, rt.getFieldCount());
        assertField(rt, "label", LogicalTypeRoot.VARCHAR);

        // The nested 'inner' field should map to ROW
        RowType.RowField innerField = findField(rt, "inner");
        Assert.assertNotNull(innerField);
        Assert.assertEquals(LogicalTypeRoot.ROW, innerField.getType().getTypeRoot());
        RowType innerRow = (RowType) innerField.getType();
        assertField(innerRow, "name", LogicalTypeRoot.VARCHAR);
        assertField(innerRow, "age", LogicalTypeRoot.INTEGER);
    }

    @Test
    public void testPojoFieldNamesPreservedWithoutClass() {
        // Even without the POJO class on the classpath, field names should be available.
        var snapshot = buildPojoSnapshot(SimplePojo.class);
        // Field name extraction happens via the snapshot, no class needed
        LogicalType t = SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
        RowType rt = (RowType) t;
        List<String> names = rt.getFieldNames();
        Assert.assertTrue(names.contains("name"));
        Assert.assertTrue(names.contains("age"));
        Assert.assertTrue(names.contains("score"));
        Assert.assertTrue(names.contains("active"));
    }

    // -------------------------------------------------------------------------
    // Avro types
    // -------------------------------------------------------------------------

    @Test
    public void testAvroSpecificRecord() {
        // AvroRecord has one field: longData (long)
        LogicalType t = convertAvroSpecificType(AvroRecord.class);
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(1, rt.getFieldCount());
        assertField(rt, "longData", LogicalTypeRoot.BIGINT);
    }

    @Test
    public void testAvroGenericRecord() {
        // convertAvro() reads only the embedded writer Schema (see
        // SerializerSnapshotToLogicalTypeConverter#convertAvro), so this also covers the
        // "specific record class missing at read time" fallback: AvroSerializerSnapshot degrades
        // to GenericRecord.class in that case, but the schema-derived RowType is identical either
        // way. The actual missing-class read path is covered end-to-end by
        // StateCatalogGeneratedSavepointITCase#testReadAvroKeyedStateFromSchemaDiscovery.
        Schema schema = AvroRecord.getClassSchema();
        LogicalType t = convertAvroGenericType(schema);
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(1, rt.getFieldCount());
        assertField(rt, "longData", LogicalTypeRoot.BIGINT);
    }

    // -------------------------------------------------------------------------
    // RowData types
    // -------------------------------------------------------------------------

    @Test
    public void testRowDataWithFieldNames() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(), VarCharType.STRING_TYPE},
                        new String[] {"id", "name"});
        RowDataSerializer serializer = new RowDataSerializer(rowType);

        LogicalType t = convert(serializer.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(2, rt.getFieldCount());
        assertField(rt, "id", LogicalTypeRoot.INTEGER);
        assertField(rt, "name", LogicalTypeRoot.VARCHAR);
    }

    @Test
    public void testRowDataWithoutFieldNamesFallsBackToPositional() {
        // Many production call sites (e.g. window operators) build a RowDataSerializer from a
        // bare LogicalType[], so no field names are available. The converter must still produce
        // a usable RowType, falling back to positional names like the Tuple case.
        RowDataSerializer serializer = new RowDataSerializer(new IntType(), new BigIntType());

        LogicalType t = convert(serializer.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(2, rt.getFieldCount());
        assertField(rt, "f0", LogicalTypeRoot.INTEGER);
        assertField(rt, "f1", LogicalTypeRoot.BIGINT);
    }

    @Test
    public void testNestedRowData() {
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
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        RowType rt = (RowType) t;
        Assert.assertEquals(2, rt.getFieldCount());
        assertField(rt, "label", LogicalTypeRoot.VARCHAR);

        RowType.RowField innerField = findField(rt, "inner");
        Assert.assertNotNull(innerField);
        Assert.assertEquals(LogicalTypeRoot.ROW, innerField.getType().getTypeRoot());
        RowType innerRow = (RowType) innerField.getType();
        assertField(innerRow, "innerId", LogicalTypeRoot.INTEGER);
        assertField(innerRow, "innerName", LogicalTypeRoot.VARCHAR);
    }

    // -------------------------------------------------------------------------
    // Window namespace types
    // -------------------------------------------------------------------------

    @Test
    public void testTimeWindow() {
        LogicalType t =
                convert(
                        new org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer()
                                .snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        Assert.assertFalse(t.isNullable());
        RowType rt = (RowType) t;
        Assert.assertEquals(2, rt.getFieldCount());
        assertField(rt, "window_start", LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        assertField(rt, "window_end", LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }

    @Test
    public void testGlobalWindow() {
        LogicalType t =
                convert(
                        new org.apache.flink.streaming.api.windowing.windows.GlobalWindow
                                        .Serializer()
                                .snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, t.getTypeRoot());
        Assert.assertFalse(t.isNullable());
        RowType rt = (RowType) t;
        Assert.assertEquals(0, rt.getFieldCount());
    }

    // -------------------------------------------------------------------------
    // Null / unknown snapshot
    // -------------------------------------------------------------------------

    @Test
    public void testNullSnapshot() {
        LogicalType t = SerializerSnapshotToLogicalTypeConverter.convert(null);
        Assert.assertEquals(LogicalTypeRoot.VARBINARY, t.getTypeRoot());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testVoidNamespaceSnapshotUnsupported() {
        // VoidNamespace is filtered out by StateTableUtils before ever reaching the converter
        // (plain per-key state has no namespace to convert); confirm it stays unsupported here.
        convert(
                new org.apache.flink.runtime.state.VoidNamespaceSerializer()
                        .snapshotConfiguration());
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
        Assert.assertNotNull("Field '" + name + "' not found in row type", field);
        Assert.assertEquals(
                "Wrong type for field '" + name + "'", expectedRoot, field.getType().getTypeRoot());
    }

    private static RowType.RowField findField(RowType row, String name) {
        return row.getFields().stream()
                .filter(f -> f.getName().equals(name))
                .findFirst()
                .orElse(null);
    }
}
