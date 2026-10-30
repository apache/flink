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

package org.apache.flink.table.data.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.utils.UuidUtils;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class FromVariantConverterTest {

    private static final VariantBuilder BUILDER = Variant.newBuilder();

    private static final LocalDateTime DATE_TIME =
            LocalDateTime.of(2026, 9, 25, 10, 15, 30, 123_000_000);

    private static final UUID UUID_VALUE = UUID.fromString("0e1f2a3b-4c5d-6e7f-8091-a2b3c4d5e6f7");

    private static final LogicalType MONEY =
            DistinctType.newBuilder(ObjectIdentifier.of("cat", "db", "Money"), new IntType())
                    .build();

    private static final LogicalType ORDER =
            StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "Order"))
                    .attributes(
                            List.of(
                                    new StructuredAttribute("id", new IntType()),
                                    new StructuredAttribute("price", MONEY)))
                    .build();

    @Test
    void testLeavesApplyTheLengthAndPrecisionOfTheirType() {
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
                                FIELD("varchar", DataTypes.VARCHAR(2)),
                                FIELD("binary", DataTypes.BINARY(2)),
                                FIELD("bytes", DataTypes.BYTES()),
                                FIELD("date", DataTypes.DATE()),
                                FIELD("time", DataTypes.TIME(0)),
                                FIELD("ts", DataTypes.TIMESTAMP(0)),
                                FIELD("ltz", DataTypes.TIMESTAMP_LTZ(3)),
                                FIELD("uuid", DataTypes.UUID()),
                                FIELD("variant", DataTypes.VARIANT()))
                        .getLogicalType();
        final Variant variant =
                BUILDER.object()
                        .add("bool", BUILDER.of(true))
                        .add("tiny", BUILDER.of((byte) 1))
                        .add("small", BUILDER.of((short) 2))
                        .add("int", BUILDER.of(3))
                        .add("big", BUILDER.of(4L))
                        .add("float", BUILDER.of(1.5f))
                        .add("double", BUILDER.of(2.5d))
                        .add("decimal", BUILDER.of(new BigDecimal("12.5")))
                        .add("char", BUILDER.of("ab"))
                        .add("varchar", BUILDER.of("abc"))
                        .add("binary", BUILDER.of(new byte[] {7}))
                        .add("bytes", BUILDER.of(new byte[] {(byte) 0xCA, (byte) 0xFE}))
                        .add("date", BUILDER.of(LocalDate.of(2026, 9, 25)))
                        .add("time", BUILDER.of(LocalTime.of(10, 15, 30, 123_456_000)))
                        .add("ts", BUILDER.of(DATE_TIME))
                        .add("ltz", BUILDER.of(Instant.ofEpochSecond(0, 123_456_000)))
                        .add("uuid", BUILDER.of(UUID_VALUE))
                        .add("variant", BUILDER.of("raw"))
                        .build();

        final RowData row = (RowData) convert(type, variant);

        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 1);
        assertThat(row.getShort(2)).isEqualTo((short) 2);
        assertThat(row.getInt(3)).isEqualTo(3);
        assertThat(row.getLong(4)).isEqualTo(4L);
        assertThat(row.getFloat(5)).isEqualTo(1.5f);
        assertThat(row.getDouble(6)).isEqualTo(2.5d);
        assertThat(row.getDecimal(7, 10, 2)).isEqualTo(decimal("12.50", 10, 2));
        assertThat(row.getString(8)).isEqualTo(StringData.fromString("ab  "));
        assertThat(row.getString(9)).isEqualTo(StringData.fromString("ab"));
        assertThat(row.getBinary(10)).containsExactly(7, 0);
        assertThat(row.getBinary(11)).containsExactly((byte) 0xCA, (byte) 0xFE);
        assertThat(row.getInt(12)).isEqualTo((int) LocalDate.of(2026, 9, 25).toEpochDay());
        assertThat(row.getInt(13)).isEqualTo(LocalTime.of(10, 15, 30).toSecondOfDay() * 1000);
        assertThat(row.getTimestamp(14, 0))
                .isEqualTo(TimestampData.fromLocalDateTime(DATE_TIME.withNano(0)));
        assertThat(row.getTimestamp(15, 3)).isEqualTo(TimestampData.fromEpochMillis(123));
        assertThat(row.getBinary(16)).isEqualTo(UuidUtils.toBytes(UUID_VALUE));
        assertThat(row.getVariant(17).getString()).isEqualTo("raw");
    }

    @Test
    void testIntegerLeavesCheckTheRangeOfTheirType() {
        assertThatThrownBy(() -> convert(DataTypes.TINYINT(), BUILDER.of(128)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Casting the VARIANT value 128 to TINYINT overflowed.");
        assertThatThrownBy(() -> convert(DataTypes.SMALLINT(), BUILDER.of(32768)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Casting the VARIANT value 32768 to SMALLINT overflowed.");
        assertThatThrownBy(() -> convert(DataTypes.INT(), BUILDER.of(2147483648L)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Casting the VARIANT value 2147483648 to INTEGER overflowed.");
        assertThatThrownBy(
                        () ->
                                convert(
                                        DataTypes.BIGINT(),
                                        BUILDER.of(new BigDecimal("9223372036854775808"))))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Casting the VARIANT value 9223372036854775808 to BIGINT overflowed.");
    }

    @Test
    void testMapKeysFollowTheLengthOfTheKeyType() {
        final Variant variant = BUILDER.object().add("ab", BUILDER.of(1)).build();

        final MapData charKey =
                (MapData) convert(DataTypes.MAP(DataTypes.CHAR(3), DataTypes.INT()), variant);
        final MapData varcharKey =
                (MapData) convert(DataTypes.MAP(DataTypes.VARCHAR(1), DataTypes.INT()), variant);

        assertThat(charKey.keyArray().getString(0)).isEqualTo(StringData.fromString("ab "));
        assertThat(varcharKey.keyArray().getString(0)).isEqualTo(StringData.fromString("a"));
    }

    @Test
    void testArrayIsTypedByItsElement() {
        final Variant variant = BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of(2)).build();

        final GenericArrayData array =
                (GenericArrayData) convert(DataTypes.ARRAY(DataTypes.INT().notNull()), variant);

        assertThat(array.toIntArray()).containsExactly(1, 2);
    }

    @Test
    void testNotNullConstructedChildFails() {
        final Variant array = BUILDER.array().add(BUILDER.ofNull()).build();

        assertThatThrownBy(
                        () ->
                                convert(
                                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).notNull()),
                                        array))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Cannot cast a VARIANT null array element to a NOT NULL element type.");
    }

    @Test
    void testNotNullVariantChildKeepsVariantNull() {
        final Variant array = BUILDER.array().add(BUILDER.ofNull()).build();

        final ArrayData result =
                (ArrayData) convert(DataTypes.ARRAY(DataTypes.VARIANT().notNull()), array);

        assertThat(result.getVariant(0).isNull()).isTrue();
    }

    @Test
    void testRowCopiesEveryVariantBelowIt() {
        final Variant list = BUILDER.array().add(BUILDER.of("x")).build();
        final Variant object =
                BUILDER.object()
                        .add("v", BUILDER.of("x"))
                        .add("list", list)
                        .add("map", BUILDER.object().add("k", BUILDER.of("x")).build())
                        .build();
        final DataType type =
                DataTypes.ROW(
                        FIELD("v", DataTypes.VARIANT()),
                        FIELD("list", DataTypes.ARRAY(DataTypes.VARIANT())),
                        FIELD("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.VARIANT())));

        final RowData row = (RowData) convert(type, object);

        assertThat(row.getVariant(0)).isEqualTo(copyOf(object.getField("v")));
        assertThat(row.getArray(1).getVariant(0))
                .isEqualTo(copyOf(object.getField("list").getElement(0)));
        assertThat(row.getMap(2).valueArray().getVariant(0))
                .isEqualTo(copyOf(object.getField("map").getField("k")));
        // outside a ROW, an element stays a view into its parent
        assertThat(((ArrayData) convert(DataTypes.ARRAY(DataTypes.VARIANT()), list)).getVariant(0))
                .isEqualTo(list.getElement(0));
    }

    @Test
    void testAbsentVariantFieldFails() {
        final Variant object = BUILDER.object().add("b", BUILDER.of(1)).build();

        assertThatThrownBy(() -> convert(DataTypes.ROW(FIELD("a", DataTypes.VARIANT())), object))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast the VARIANT object to ROW<`a` VARIANT> because the field 'a' "
                                + "is not present in the VARIANT.");
    }

    @Test
    void testTopLevelVariantNullReachesTheLeaf() {
        final Variant nullVariant = BUILDER.ofNull();

        // a cast to a nullable type maps a variant null to NULL before it calls the converter
        assertThatThrownBy(() -> convert(DataTypes.INT(), nullVariant))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith("Cannot cast a VARIANT NULL value to INTEGER.");
        assertThatThrownBy(() -> convert(DataTypes.INT().notNull(), nullVariant))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith("Cannot cast a VARIANT NULL value to INTEGER.");
        assertThatThrownBy(() -> convert(DataTypes.ARRAY(DataTypes.INT()).notNull(), nullVariant))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageStartingWith(
                        "Cannot cast a VARIANT NULL value to ARRAY<INT> NOT NULL because the "
                                + "target requires an array.");
        assertThat(convert(DataTypes.VARIANT(), nullVariant)).isSameAs(nullVariant);
    }

    @Test
    void testNestedWrongShapeNamesItsOwnType() {
        final Variant array = BUILDER.array().add(BUILDER.of(1)).build();

        assertThatThrownBy(() -> convert(DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())), array))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Cannot cast a VARIANT INT value to ARRAY<INT> because the target "
                                + "requires an array. Only a variant array casts to ARRAY and "
                                + "only a variant object casts to ROW, STRUCTURED, or MAP.");
    }

    @Test
    void testStructuredAndDistinctTypes() {
        final Variant variant =
                BUILDER.object().add("price", BUILDER.of(42)).add("id", BUILDER.of(1)).build();

        assertThat(convert(ORDER, variant)).isEqualTo(GenericRowData.of(1, 42));
        assertThat(convert(MONEY, BUILDER.of(42))).isEqualTo(42);
    }

    @Test
    void testCreateRejectsTypesThatVariantDoesNotCastTo() {
        assertThatThrownBy(
                        () ->
                                FromVariantConverter.create(
                                        DataTypes.MULTISET(DataTypes.INT()).getLogicalType(),
                                        ZoneOffset.UTC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot cast VARIANT to MULTISET<INT>.");
        assertThatThrownBy(
                        () ->
                                FromVariantConverter.create(
                                        DataTypes.ARRAY(DataTypes.NULL()).getLogicalType(),
                                        ZoneOffset.UTC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot cast VARIANT to NULL.");
        assertThatThrownBy(
                        () ->
                                FromVariantConverter.create(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())
                                                .getLogicalType(),
                                        ZoneOffset.UTC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageEndingWith("A MAP key must be a character string.");
    }

    @Test
    void testConverterSurvivesJavaSerialization() throws Exception {
        final LogicalType type =
                DataTypes.ROW(
                                FIELD("int", DataTypes.INT()),
                                FIELD("ltz", DataTypes.STRING()),
                                FIELD("ts", DataTypes.TIMESTAMP(0)),
                                FIELD("list", DataTypes.ARRAY(DataTypes.INT().notNull())),
                                FIELD("map", DataTypes.MAP(DataTypes.CHAR(2), DataTypes.INT())),
                                FIELD("variant", DataTypes.VARIANT()),
                                FIELD("order", DataTypes.of(ORDER)))
                        .getLogicalType();
        final Variant variant =
                BUILDER.object()
                        .add("int", BUILDER.of(1))
                        .add("ltz", BUILDER.of(Instant.EPOCH))
                        .add("ts", BUILDER.of(Instant.EPOCH))
                        .add("list", BUILDER.array().add(BUILDER.of(2)).build())
                        .add("map", BUILDER.object().add("k", BUILDER.ofNull()).build())
                        .add("variant", BUILDER.ofNull())
                        .add(
                                "order",
                                BUILDER.object()
                                        .add("id", BUILDER.of(3))
                                        .add("price", BUILDER.of(4))
                                        .build())
                        .build();
        final FromVariantConverter converter =
                FromVariantConverter.create(type, ZoneId.of("Asia/Tokyo"));

        final RowData actual = (RowData) InstantiationUtil.clone(converter).convert(variant);

        assertThat(actual).isEqualTo(converter.convert(variant));
        // the session zone survives too
        assertThat(actual.getString(1).toString()).startsWith("1970-01-01 09:00:00");
        assertThat(actual.getTimestamp(2, 0))
                .isEqualTo(TimestampData.fromLocalDateTime(LocalDateTime.of(1970, 1, 1, 9, 0)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTrips")
    void testRoundTripThroughToVariantConverter(final DataType type, final Object value) {
        final LogicalType logicalType = type.getLogicalType();
        final Variant variant = ToVariantConverter.create(logicalType).convert(value);

        assertThat(FromVariantConverter.create(logicalType, ZoneOffset.UTC).convert(variant))
                .isEqualTo(value);
    }

    private static Stream<Arguments> roundTrips() {
        return Stream.of(
                arguments(DataTypes.BOOLEAN(), true),
                arguments(DataTypes.TINYINT(), (byte) -1),
                arguments(DataTypes.SMALLINT(), (short) 300),
                arguments(DataTypes.INT(), 70_000),
                arguments(DataTypes.BIGINT(), Long.MIN_VALUE),
                arguments(DataTypes.FLOAT(), 1.5f),
                arguments(DataTypes.DOUBLE(), -2.25d),
                arguments(DataTypes.DECIMAL(10, 2), decimal("12.50", 10, 2)),
                arguments(
                        DataTypes.DECIMAL(38, 10),
                        decimal("1234567890123456789012345678.0123456789", 38, 10)),
                arguments(DataTypes.CHAR(4), StringData.fromString("ab  ")),
                arguments(DataTypes.STRING(), StringData.fromString("héllo 👋")),
                arguments(DataTypes.BINARY(2), new byte[] {1, 2}),
                arguments(DataTypes.BYTES(), new byte[] {(byte) 0xCA, (byte) 0xFE}),
                arguments(DataTypes.DATE(), (int) LocalDate.of(2026, 9, 25).toEpochDay()),
                arguments(DataTypes.TIME(3), LocalTime.of(10, 15, 30).toSecondOfDay() * 1000 + 123),
                arguments(DataTypes.TIMESTAMP(3), TimestampData.fromLocalDateTime(DATE_TIME)),
                arguments(DataTypes.TIMESTAMP(9), TimestampData.fromEpochMillis(1, 234_567)),
                arguments(DataTypes.TIMESTAMP_LTZ(6), TimestampData.fromEpochMillis(-1, 1_000)),
                arguments(DataTypes.TIMESTAMP_LTZ(9), TimestampData.fromEpochMillis(0, 1)),
                arguments(DataTypes.UUID(), UuidUtils.toBytes(UUID_VALUE)),
                arguments(
                        DataTypes.ARRAY(DataTypes.INT()),
                        new GenericArrayData(new Object[] {1, null, 3})),
                arguments(
                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())),
                        new GenericArrayData(
                                new Object[] {
                                    new GenericArrayData(new Object[] {StringData.fromString("a")}),
                                    null
                                })),
                arguments(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                        mapOf(StringData.fromString("a"), 1L)),
                arguments(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                        mapOf(StringData.fromString("a"), null)),
                arguments(
                        DataTypes.ROW(
                                FIELD("id", DataTypes.INT().notNull()),
                                FIELD("name", DataTypes.STRING()),
                                FIELD("tags", DataTypes.ARRAY(DataTypes.STRING()))),
                        GenericRowData.of(
                                7,
                                null,
                                new GenericArrayData(new Object[] {StringData.fromString("x")}))),
                arguments(DataTypes.of(ORDER), GenericRowData.of(1, 42)),
                arguments(DataTypes.of(MONEY), 42));
    }

    private static Variant copyOf(final Variant view) {
        final BinaryVariant binary = (BinaryVariant) view;
        return new BinaryVariant(binary.getValue(), binary.getMetadata());
    }

    private static DecimalData decimal(final String value, final int precision, final int scale) {
        return DecimalData.fromBigDecimal(new BigDecimal(value), precision, scale);
    }

    private static GenericMapData mapOf(final Object key, final Object value) {
        final Map<Object, Object> map = new HashMap<>();
        map.put(key, value);
        return new GenericMapData(map);
    }

    private static Object convert(final DataType dataType, final Variant variant) {
        return convert(dataType.getLogicalType(), variant);
    }

    private static Object convert(final LogicalType type, final Variant variant) {
        return FromVariantConverter.create(type, ZoneOffset.UTC).convert(variant);
    }
}
