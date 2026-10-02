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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.utils.UuidUtils;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToVariantConverterTest {

    @Test
    void testRowBecomesObjectKeyedByFieldNames() {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("name", DataTypes.STRING()),
                                FIELD("id", DataTypes.INT()),
                                FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())),
                                FIELD(
                                        "address",
                                        DataTypes.ROW(
                                                FIELD("city", DataTypes.STRING()),
                                                FIELD("zip", DataTypes.INT()))))
                        .getLogicalType();
        final GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("ada"),
                        7,
                        new GenericArrayData(
                                new Object[] {
                                    StringData.fromString("a"), StringData.fromString("b")
                                }),
                        GenericRowData.of(StringData.fromString("Berlin"), 10115));

        // A variant object keeps its keys sorted, so the field order of the ROW is not kept.
        assertThat(convert(type, row).toJson())
                .isEqualTo(
                        "{\"address\":{\"city\":\"Berlin\",\"zip\":10115},\"id\":7,"
                                + "\"name\":\"ada\",\"tags\":[\"a\",\"b\"]}");
    }

    @Test
    void testLeavesKeepTheKindOfTheirType() {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("bool", DataTypes.BOOLEAN()),
                                FIELD("tiny", DataTypes.TINYINT()),
                                FIELD("small", DataTypes.SMALLINT()),
                                FIELD("int", DataTypes.INT()),
                                FIELD("big", DataTypes.BIGINT()),
                                FIELD("float", DataTypes.FLOAT()),
                                FIELD("double", DataTypes.DOUBLE()),
                                FIELD("decimal", DataTypes.DECIMAL(10, 2)),
                                FIELD("char", DataTypes.CHAR(4)),
                                FIELD("bytes", DataTypes.BYTES()),
                                FIELD("date", DataTypes.DATE()),
                                FIELD("time", DataTypes.TIME()),
                                FIELD("ts", DataTypes.TIMESTAMP(3)),
                                FIELD("tsNanos", DataTypes.TIMESTAMP(9)),
                                FIELD("ltz", DataTypes.TIMESTAMP_LTZ(6)),
                                FIELD("uuid", DataTypes.UUID()))
                        .getLogicalType();
        final LocalDateTime dateTime = LocalDateTime.of(2026, 9, 25, 10, 15, 30, 123_000_000);
        final UUID uuid = UUID.fromString("0e1f2a3b-4c5d-6e7f-8091-a2b3c4d5e6f7");
        final GenericRowData row =
                GenericRowData.of(
                        true,
                        (byte) 1,
                        (short) 2,
                        3,
                        4L,
                        1.5f,
                        2.5d,
                        DecimalData.fromBigDecimal(new BigDecimal("12.50"), 10, 2),
                        StringData.fromString("ab  "),
                        new byte[] {(byte) 0xCA, (byte) 0xFE},
                        (int) LocalDate.of(2026, 9, 25).toEpochDay(),
                        LocalTime.of(10, 15, 30).toSecondOfDay() * 1000,
                        TimestampData.fromLocalDateTime(dateTime),
                        TimestampData.fromLocalDateTime(dateTime),
                        TimestampData.fromEpochMillis(0),
                        UuidUtils.toBytes(uuid));

        final Variant variant = convert(type, row);

        assertThat(variant.getField("bool").getBoolean()).isTrue();
        assertThat(variant.getField("tiny").getType()).isEqualTo(Variant.Type.TINYINT);
        assertThat(variant.getField("small").getType()).isEqualTo(Variant.Type.SMALLINT);
        assertThat(variant.getField("int").getType()).isEqualTo(Variant.Type.INT);
        assertThat(variant.getField("big").getType()).isEqualTo(Variant.Type.BIGINT);
        assertThat(variant.getField("float").getFloat()).isEqualTo(1.5f);
        assertThat(variant.getField("double").getDouble()).isEqualTo(2.5d);
        assertThat(variant.getField("decimal").getDecimal()).isEqualByComparingTo("12.50");
        assertThat(variant.getField("char").getString()).isEqualTo("ab  ");
        assertThat(variant.getField("bytes").getBytes()).containsExactly((byte) 0xCA, (byte) 0xFE);
        assertThat(variant.getField("date").getDate()).isEqualTo(LocalDate.of(2026, 9, 25));
        assertThat(variant.getField("time").getTime()).isEqualTo(LocalTime.of(10, 15, 30));
        assertThat(variant.getField("ts").getType()).isEqualTo(Variant.Type.TIMESTAMP);
        assertThat(variant.getField("ts").getDateTime()).isEqualTo(dateTime);
        assertThat(variant.getField("tsNanos").getType()).isEqualTo(Variant.Type.TIMESTAMP_NS);
        assertThat(variant.getField("ltz").getType()).isEqualTo(Variant.Type.TIMESTAMP_LTZ);
        assertThat(variant.getField("uuid").getUuid()).isEqualTo(uuid);
    }

    @Test
    void testNullBecomesVariantNull() {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("a", DataTypes.INT()),
                                FIELD("list", DataTypes.ARRAY(DataTypes.INT())),
                                FIELD("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())))
                        .getLogicalType();
        final GenericRowData row =
                GenericRowData.of(
                        null,
                        new GenericArrayData(new Object[] {1, null}),
                        mapOf(new Object[] {StringData.fromString("k")}, new Object[] {null}));

        assertThat(convert(type, row).toJson())
                .isEqualTo("{\"a\":null,\"list\":[1,null],\"map\":{\"k\":null}}");
        final GenericArrayData nullElement = new GenericArrayData(new Object[] {null});
        assertThat(toJson(DataTypes.ARRAY(DataTypes.NULL()), nullElement)).isEqualTo("[null]");
        final GenericRowData nullField = GenericRowData.of((Object) null);
        assertThat(toJson(DataTypes.ROW(FIELD("n", DataTypes.NULL())), nullField))
                .isEqualTo("{\"n\":null}");
    }

    @Test
    void testEmptyValues() {
        assertThat(toJson(DataTypes.ARRAY(DataTypes.INT()), new GenericArrayData(new Object[0])))
                .isEqualTo("[]");
        final GenericMapData emptyMap = new GenericMapData(Map.of());
        assertThat(toJson(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), emptyMap))
                .isEqualTo("{}");
        assertThat(convert(new RowType(List.of()), new GenericRowData(0)).toJson()).isEqualTo("{}");
    }

    @Test
    void testDuplicateMapKeyKeepsTheLastValue() {
        final MapData map =
                mapOf(
                        new Object[] {StringData.fromString("a"), StringData.fromString("a")},
                        new Object[] {1, 2});

        assertThat(convert(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), map).toJson())
                .isEqualTo("{\"a\":2}");
    }

    @Test
    void testNullMapKeyFails() {
        final MapData map = mapOf(new Object[] {null}, new Object[] {1});

        assertThatThrownBy(() -> convert(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), map))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a value of type MAP<STRING, INT> with a NULL key to VARIANT. "
                                + "A VARIANT object key cannot be NULL.");
    }

    @Test
    void testNestedVariantIsEmbedded() throws Exception {
        final Variant parsed =
                BinaryVariantInternalBuilder.parseJson("{\"x\":[1,2],\"y\":\"z\"}", false);
        final LogicalType type =
                DataTypes.ROW(FIELD("v", DataTypes.VARIANT()), FIELD("w", DataTypes.VARIANT()))
                        .getLogicalType();

        // The field of another variant starts at a position other than 0 in its buffer.
        assertThat(convert(type, GenericRowData.of(parsed, parsed.getField("x"))).toJson())
                .isEqualTo("{\"v\":{\"x\":[1,2],\"y\":\"z\"},\"w\":[1,2]}");
    }

    @Test
    void testStructuredAndDistinctTypes() {
        final LogicalType money =
                DistinctType.newBuilder(ObjectIdentifier.of("cat", "db", "Money"), new IntType())
                        .build();
        final LogicalType type =
                StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "Order"))
                        .attributes(
                                List.of(
                                        new StructuredAttribute("id", new IntType()),
                                        new StructuredAttribute("price", money)))
                        .build();

        final Variant variant = convert(type, GenericRowData.of(1, 42));

        assertThat(variant.toJson()).isEqualTo("{\"id\":1,\"price\":42}");
        assertThat(variant.getField("price").getType()).isEqualTo(Variant.Type.INT);
    }

    @Test
    void testValueOverTheSizeLimitFails() {
        final StringData half = StringData.fromString("x".repeat(9 * 1024 * 1024));
        final ArrayData array = new GenericArrayData(new Object[] {half, half});

        assertThatThrownBy(() -> convert(DataTypes.ARRAY(DataTypes.STRING()), array))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a value of type ARRAY<STRING> to VARIANT. A VARIANT is "
                                + "limited to 16 MiB.");
    }

    @Test
    void testTimestampOutsideTheNanosecondRangeFails() {
        final ArrayData array =
                new GenericArrayData(
                        new Object[] {
                            TimestampData.fromLocalDateTime(LocalDateTime.of(3000, 1, 1, 0, 0))
                        });

        assertThatThrownBy(() -> convert(DataTypes.ARRAY(DataTypes.TIMESTAMP(9)), array))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith(
                        "Cannot cast the TIMESTAMP(9) value 3000-01-01T00:00 to VARIANT.");
    }

    @Test
    void testConverterSurvivesSerialization() throws Exception {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("a", DataTypes.INT()),
                                FIELD(
                                        "b",
                                        DataTypes.MAP(
                                                DataTypes.VARCHAR(10),
                                                DataTypes.ARRAY(DataTypes.INT()))))
                        .getLogicalType();
        final ToVariantConverter converter =
                InstantiationUtil.clone(ToVariantConverter.create(type));

        final GenericRowData row =
                GenericRowData.of(
                        1,
                        new GenericMapData(
                                Map.of(
                                        StringData.fromString("k"),
                                        new GenericArrayData(new Object[] {2, 3}))));
        assertThat(converter.convert(row).toJson()).isEqualTo("{\"a\":1,\"b\":{\"k\":[2,3]}}");
    }

    private static Variant convert(DataType dataType, Object value) {
        return convert(dataType.getLogicalType(), value);
    }

    private static String toJson(DataType dataType, Object value) {
        return convert(dataType, value).toJson();
    }

    private static Variant convert(LogicalType type, Object value) {
        return ToVariantConverter.create(type).convert(value);
    }

    private static MapData mapOf(Object[] keys, Object[] values) {
        return new MapData() {
            @Override
            public int size() {
                return keys.length;
            }

            @Override
            public ArrayData keyArray() {
                return new GenericArrayData(keys);
            }

            @Override
            public ArrayData valueArray() {
                return new GenericArrayData(values);
            }
        };
    }
}
