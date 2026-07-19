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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
class TypeConversionDriftGuardTest {

    private final StateValueConverter converter = new StateValueConverter();

    @Test
    void testInt() {
        LogicalType type = new IntType(false);
        Object result = converter.getValue(type, 42);
        assertThat(result).isEqualTo(42);
    }

    @Test
    void testLong() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new LongSerializer.LongSerializerSnapshot());
        Object result = converter.getValue(type, 42L);
        assertThat(result).isEqualTo(42L);
    }

    @Test
    void testString() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new StringSerializer.StringSerializerSnapshot());
        Object result = converter.getValue(type, "hello");
        assertThat(result).isInstanceOf(StringData.class);
        assertThat(result.toString()).isEqualTo("hello");
    }

    @Test
    void testList() {
        ListSerializer<String> ser = new ListSerializer<>(StringSerializer.INSTANCE);
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(ser.snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ARRAY);

        List<String> value = Arrays.asList("a", "b", "c");
        Object result = converter.getValue(type, value);
        assertThat(result).isInstanceOf(GenericArrayData.class);
        assertThat(((GenericArrayData) result).size()).isEqualTo(3);
    }

    @Test
    void testMap() {
        MapSerializer<String, Long> ser =
                new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(ser.snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.MAP);

        Map<String, Long> value = Collections.singletonMap("k", 7L);
        Object result = converter.getValue(type, value);
        assertThat(result).isInstanceOf(GenericMapData.class);
        assertThat(((GenericMapData) result).size()).isEqualTo(1);
    }

    @Test
    void testNullableInnerType() {
        ListSerializer<String> ser = new ListSerializer<>(StringSerializer.INSTANCE);
        LogicalType innerType =
                SerializerSnapshotToLogicalTypeConverter.convert(ser.snapshotConfiguration())
                        .copy(true);
        assertThat(converter.getValue(innerType, null)).isNull();
    }

    @Test
    void testTimeWindow() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new TimeWindow.Serializer().snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);

        TimeWindow window = new TimeWindow(100L, 200L);
        Object result = converter.getValue(type, window);
        assertThat(result).isInstanceOf(GenericRowData.class);
        GenericRowData row = (GenericRowData) result;
        assertThat(row.getArity()).isEqualTo(2);
        assertThat(row.getField(0)).isEqualTo(TimestampData.fromEpochMillis(100L));
        assertThat(row.getField(1)).isEqualTo(TimestampData.fromEpochMillis(200L));
    }

    @Test
    void testGlobalWindow() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new GlobalWindow.Serializer().snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
        assertThat(((RowType) type).getFieldCount()).isEqualTo(0);

        Object result = converter.getValue(type, GlobalWindow.get());
        assertThat(result).isInstanceOf(GenericRowData.class);
        assertThat(((GenericRowData) result).getArity()).isEqualTo(0);
    }

    @Test
    void testTuple() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?> snapshot =
                TypeExtractor.getForObject(Tuple2.of(1, "x"))
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();
        LogicalType type = SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);

        Object result = converter.getValue(type, Tuple2.of(1, "x"));
        assertThat(result).isInstanceOf(GenericRowData.class);
        GenericRowData row = (GenericRowData) result;
        assertThat(row.getArity()).isEqualTo(2);
        assertThat(row.getField(0)).isEqualTo(1);
        assertThat(row.getField(1).toString()).isEqualTo("x");
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
    void testPojo() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?> snapshot =
                TypeExtractor.createTypeInfo(SamplePojo.class)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();
        LogicalType type = SerializerSnapshotToLogicalTypeConverter.convert(snapshot);
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);

        Object result = converter.getValue(type, new SamplePojo("bob", 30));
        assertThat(result).isInstanceOf(GenericRowData.class);
        RowType rowType = (RowType) type;
        GenericRowData row = (GenericRowData) result;
        int nameIdx = rowType.getFieldIndex("name");
        int ageIdx = rowType.getFieldIndex("age");
        assertThat(row.getField(nameIdx).toString()).isEqualTo("bob");
        assertThat(row.getField(ageIdx)).isEqualTo(30);
    }

    @Test
    void testAvroSpecificRecord() {
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        new AvroTypeInfo<>(AvroRecord.class)
                                .createSerializer(new SerializerConfigImpl())
                                .snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);

        AvroRecord avroRecord = AvroRecord.newBuilder().setLongData(99L).build();
        Object result = converter.getValue(type, avroRecord);
        assertThat(result).isInstanceOf(GenericRowData.class);
        RowType rowType = (RowType) type;
        int idx = rowType.getFieldIndex("longData");
        assertThat(((GenericRowData) result).getField(idx)).isEqualTo(99L);
    }

    @Test
    void testRowDataPassThrough() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(), VarCharType.STRING_TYPE},
                        new String[] {"id", "name"});
        RowDataSerializer serializer = new RowDataSerializer(rowType);
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        serializer.snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);

        GenericRowData sourceRow = new GenericRowData(2);
        sourceRow.setField(0, 1);
        sourceRow.setField(1, StringData.fromString("abc"));

        Object result = converter.getValue(type, sourceRow);
        assertThat(result).isInstanceOf(GenericRowData.class);
        GenericRowData resultRow = (GenericRowData) result;
        assertThat(resultRow.getField(0)).isEqualTo(1);
        assertThat(resultRow.getField(1).toString()).isEqualTo("abc");
    }

    @Test
    void testArrayOfArray() {
        ListSerializer<List<String>> serializer =
                new ListSerializer<>(new ListSerializer<>(StringSerializer.INSTANCE));
        LogicalType type =
                SerializerSnapshotToLogicalTypeConverter.convert(
                        serializer.snapshotConfiguration());
        assertThat(type.getTypeRoot()).isEqualTo(LogicalTypeRoot.ARRAY);
        assertThat(((ArrayType) type).getElementType().getTypeRoot())
                .isEqualTo(LogicalTypeRoot.ARRAY);

        List<List<String>> value =
                Arrays.asList(Arrays.asList("a", "b"), Collections.singletonList("c"));
        Object result = converter.getValue(type, value);
        assertThat(result).isInstanceOf(GenericArrayData.class);
        GenericArrayData outerArray = (GenericArrayData) result;
        assertThat(outerArray.size()).isEqualTo(2);
        assertThat(outerArray.getArray(0)).isInstanceOf(GenericArrayData.class);
    }
}
