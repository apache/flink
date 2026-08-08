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

package org.apache.flink.state.table;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.state.api.schema.SerializerSnapshotToLogicalTypeConverter;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.example.state.writer.job.schema.avro.AvroRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Drift guard between {@link SerializerSnapshotToLogicalTypeConverter} (schema-time: {@code
 * TypeSerializerSnapshot} -> {@link LogicalType}) and {@link StateValueConverter}/{@link
 * org.apache.flink.state.api.input.deserializer.InternalTypeConverter} (runtime: raw deserialized
 * value -> internal representation).
 *
 * <p>For every type shape the schema-time converter knows how to describe, this asserts that a
 * representative runtime value of that shape round-trips through the runtime converters without
 * throwing and lands on the internal representation the shape implies. This is a permanent
 * regression guard, not a one-time check: whenever a new case is added to {@link
 * SerializerSnapshotToLogicalTypeConverter#convert}, a matching case must be added here too, or
 * this test stops actually covering the new shape.
 */
public class TypeConversionDriftGuardTest {

    private final StateValueConverter converter = new StateValueConverter();

    @Test
    public void testInt() {
        LogicalType type = new IntType(false);
        Object result = converter.getValue(type, 42);
        Assert.assertEquals(42, result);
    }

    @Test
    public void testLong() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new LongSerializer.LongSerializerSnapshot());
        Object result = converter.getValue(type, 42L);
        Assert.assertEquals(42L, result);
    }

    @Test
    public void testString() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new StringSerializer.StringSerializerSnapshot());
        Object result = converter.getValue(type, "hello");
        Assert.assertTrue(result instanceof StringData);
        Assert.assertEquals("hello", result.toString());
    }

    @Test
    public void testList() {
        ListSerializer<String> ser = new ListSerializer<>(StringSerializer.INSTANCE);
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(ser.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ARRAY, type.getTypeRoot());

        List<String> value = Arrays.asList("a", "b", "c");
        Object result = converter.getValue(type, value);
        Assert.assertTrue(result instanceof GenericArrayData);
        Assert.assertEquals(3, ((GenericArrayData) result).size());
    }

    @Test
    public void testMap() {
        MapSerializer<String, Long> ser =
                new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(ser.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.MAP, type.getTypeRoot());

        Map<String, Long> value = Collections.singletonMap("k", 7L);
        Object result = converter.getValue(type, value);
        Assert.assertTrue(result instanceof GenericMapData);
        Assert.assertEquals(1, ((GenericMapData) result).size());
    }

    @Test
    public void testNullableInnerType() {
        ListSerializer<String> ser = new ListSerializer<>(StringSerializer.INSTANCE);
        LogicalType innerType =
                SerializerSnapshotToLogicalTypeConverter.convert(ser.snapshotConfiguration())
                        .copy(true);
        Assert.assertNull(converter.getValue(innerType, null));
    }

    @Test
    public void testTimeWindow() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new TimeWindow.Serializer().snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, type.getTypeRoot());

        TimeWindow window = new TimeWindow(100L, 200L);
        Object result = converter.getValue(type, window);
        Assert.assertTrue(result instanceof GenericRowData);
        GenericRowData row = (GenericRowData) result;
        Assert.assertEquals(2, row.getArity());
        Assert.assertEquals(TimestampData.fromEpochMillis(100L), row.getField(0));
        Assert.assertEquals(TimestampData.fromEpochMillis(200L), row.getField(1));
    }

    @Test
    public void testGlobalWindow() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new GlobalWindow.Serializer().snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, type.getTypeRoot());
        Assert.assertEquals(0, ((RowType) type).getFieldCount());

        Object result = converter.getValue(type, GlobalWindow.get());
        Assert.assertTrue(result instanceof GenericRowData);
        Assert.assertEquals(0, ((GenericRowData) result).getArity());
    }

    @Test
    public void testTuple() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?> snapshot =
                TypeExtractor.getForObject(Tuple2.of(1, "x"))
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();
        LogicalType type = SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
        Assert.assertEquals(LogicalTypeRoot.ROW, type.getTypeRoot());

        Object result = converter.getValue(type, Tuple2.of(1, "x"));
        Assert.assertTrue(result instanceof GenericRowData);
        GenericRowData row = (GenericRowData) result;
        Assert.assertEquals(2, row.getArity());
        Assert.assertEquals(1, row.getField(0));
        Assert.assertEquals("x", row.getField(1).toString());
    }

    /** POJO with the same shape used by {@code SerializerSnapshotToLogicalTypeConverterTest}. */
    public static class SamplePojo {
        public String name;
        public int age;

        public SamplePojo() {}

        public SamplePojo(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    @Test
    public void testPojo() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?> snapshot =
                TypeExtractor.createTypeInfo(SamplePojo.class)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();
        LogicalType type = SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
        Assert.assertEquals(LogicalTypeRoot.ROW, type.getTypeRoot());

        Object result = converter.getValue(type, new SamplePojo("bob", 30));
        Assert.assertTrue(result instanceof GenericRowData);
        RowType rowType = (RowType) type;
        GenericRowData row = (GenericRowData) result;
        int nameIdx = rowType.getFieldIndex("name");
        int ageIdx = rowType.getFieldIndex("age");
        Assert.assertEquals("bob", row.getField(nameIdx).toString());
        Assert.assertEquals(30, row.getField(ageIdx));
    }

    @Test
    public void testAvroSpecificRecord() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new AvroTypeInfo<>(AvroRecord.class)
                                .createSerializer(new SerializerConfigImpl())
                                .snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, type.getTypeRoot());

        AvroRecord avroRecord = AvroRecord.newBuilder().setLongData(99L).build();
        Object result = converter.getValue(type, avroRecord);
        Assert.assertTrue(result instanceof GenericRowData);
        RowType rowType = (RowType) type;
        int idx = rowType.getFieldIndex("longData");
        Assert.assertEquals(99L, ((GenericRowData) result).getField(idx));
    }

    @Test
    public void testRowDataPassThrough() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(), VarCharType.STRING_TYPE},
                        new String[] {"id", "name"});
        RowDataSerializer serializer = new RowDataSerializer(rowType);
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        serializer.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ROW, type.getTypeRoot());

        GenericRowData sourceRow = new GenericRowData(2);
        sourceRow.setField(0, 1);
        sourceRow.setField(1, StringData.fromString("abc"));

        Object result = converter.getValue(type, sourceRow);
        Assert.assertTrue(result instanceof GenericRowData);
        GenericRowData resultRow = (GenericRowData) result;
        Assert.assertEquals(1, resultRow.getField(0));
        Assert.assertEquals("abc", resultRow.getField(1).toString());
    }

    @Test
    public void testArrayOfArray() {
        ListSerializer<List<String>> serializer =
                new ListSerializer<>(new ListSerializer<>(StringSerializer.INSTANCE));
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        serializer.snapshotConfiguration());
        Assert.assertEquals(LogicalTypeRoot.ARRAY, type.getTypeRoot());
        Assert.assertEquals(
                LogicalTypeRoot.ARRAY, ((ArrayType) type).getElementType().getTypeRoot());

        List<List<String>> value =
                Arrays.asList(Arrays.asList("a", "b"), Collections.singletonList("c"));
        Object result = converter.getValue(type, value);
        Assert.assertTrue(result instanceof GenericArrayData);
        GenericArrayData outerArray = (GenericArrayData) result;
        Assert.assertEquals(2, outerArray.size());
        Assert.assertTrue(outerArray.getArray(0) instanceof GenericArrayData);
    }
}
