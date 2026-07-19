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

package org.apache.flink.state.api.input.deserializer;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Unit tests for {@link InternalTypeConverter}. */
public class InternalTypeConverterTest {

    @Test
    public void testNullReturnsNull() {
        assertNull(InternalTypeConverter.toInternal(null, new IntType()));
        assertNull(InternalTypeConverter.toInternal(null, new VarCharType()));
        assertNull(InternalTypeConverter.toInternal("anything", new NullType()));
    }

    @Test
    public void testVarChar() {
        // String → StringData
        assertEquals(
                StringData.fromString("hello"),
                InternalTypeConverter.toInternal("hello", new VarCharType()));
        // StringData → pass-through
        StringData sd = StringData.fromString("world");
        assertSame(sd, InternalTypeConverter.toInternal(sd, new VarCharType()));
        // Other type → toString()
        assertEquals(
                StringData.fromString("42"),
                InternalTypeConverter.toInternal(42, new VarCharType()));
    }

    @Test
    public void testPrimitivePassThroughs() {
        // All of these are returned unchanged.
        assertSame(Boolean.TRUE, InternalTypeConverter.toInternal(true, new BooleanType()));
        Byte b = (byte) 7;
        assertSame(b, InternalTypeConverter.toInternal(b, new TinyIntType()));
        Short s = (short) 100;
        assertSame(s, InternalTypeConverter.toInternal(s, new SmallIntType()));
        Integer i = 42;
        assertSame(i, InternalTypeConverter.toInternal(i, new IntType()));
        Long l = 123L;
        assertSame(l, InternalTypeConverter.toInternal(l, new BigIntType()));
        Float f = 1.5f;
        assertSame(f, InternalTypeConverter.toInternal(f, new FloatType()));
        Double d = 3.14;
        assertSame(d, InternalTypeConverter.toInternal(d, new DoubleType()));
        Integer timeMillis = 3_600_000;
        assertSame(timeMillis, InternalTypeConverter.toInternal(timeMillis, new TimeType()));
        Long months = 13L;
        assertSame(
                months,
                InternalTypeConverter.toInternal(
                        months,
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH)));
        Long dayMillis = 86_400_000L;
        assertSame(
                dayMillis,
                InternalTypeConverter.toInternal(
                        dayMillis,
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY)));
    }

    @Test
    public void testDecimal() {
        DecimalType type = new DecimalType(10, 2);
        // BigDecimal → DecimalData
        BigDecimal bd = new BigDecimal("12.34");
        assertEquals(
                DecimalData.fromBigDecimal(bd, 10, 2), InternalTypeConverter.toInternal(bd, type));
        // byte[] → DecimalData (unscaled bytes)
        byte[] unscaledBytes = BigDecimal.valueOf(1234).unscaledValue().toByteArray();
        assertEquals(
                DecimalData.fromUnscaledBytes(unscaledBytes, 10, 2),
                InternalTypeConverter.toInternal(unscaledBytes, type));
        // ByteBuffer → DecimalData (unscaled bytes)
        assertEquals(
                DecimalData.fromUnscaledBytes(unscaledBytes, 10, 2),
                InternalTypeConverter.toInternal(ByteBuffer.wrap(unscaledBytes), type));
        // DecimalData → pass-through
        DecimalData dd = DecimalData.fromBigDecimal(new BigDecimal("9.99"), 10, 2);
        assertSame(dd, InternalTypeConverter.toInternal(dd, type));
    }

    @Test
    public void testDate() {
        // Integer (epoch day) → pass-through
        Integer epochDay = 19_000;
        assertSame(epochDay, InternalTypeConverter.toInternal(epochDay, new DateType()));
        // LocalDate → epoch day int
        LocalDate ld = LocalDate.of(2022, 6, 15);
        assertEquals((int) ld.toEpochDay(), InternalTypeConverter.toInternal(ld, new DateType()));
        // java.sql.Date → epoch day int
        java.sql.Date sqlDate = java.sql.Date.valueOf("2022-06-15");
        assertEquals(
                (int) sqlDate.toLocalDate().toEpochDay(),
                InternalTypeConverter.toInternal(sqlDate, new DateType()));
    }

    @Test
    public void testTimestamp() {
        Timestamp ts = Timestamp.valueOf("2023-01-15 10:30:00");
        Instant instant = Instant.parse("2023-01-15T10:30:00Z");
        LocalDateTime ldt = LocalDateTime.of(2023, 1, 15, 10, 30, 0);

        // All three timestamp type roots accept the same source types.
        for (LogicalType tsType :
                new LogicalType[] {
                    new TimestampType(), new ZonedTimestampType(), new LocalZonedTimestampType()
                }) {
            assertEquals(
                    TimestampData.fromTimestamp(ts), InternalTypeConverter.toInternal(ts, tsType));
            assertEquals(
                    TimestampData.fromInstant(instant),
                    InternalTypeConverter.toInternal(instant, tsType));
            assertEquals(
                    TimestampData.fromLocalDateTime(ldt),
                    InternalTypeConverter.toInternal(ldt, tsType));
        }
        // TimestampData → pass-through
        TimestampData td = TimestampData.fromEpochMillis(1000L);
        assertSame(td, InternalTypeConverter.toInternal(td, new TimestampType()));
    }

    @Test
    public void testBinary() {
        byte[] bytes = {1, 2, 3};
        // byte[] → pass-through
        assertSame(bytes, InternalTypeConverter.toInternal(bytes, new VarBinaryType()));
        // ByteBuffer → extracted byte[]
        assertArrayEquals(
                bytes,
                (byte[])
                        InternalTypeConverter.toInternal(
                                ByteBuffer.wrap(bytes), new VarBinaryType()));
    }

    @Test
    public void testRow() {
        RowType rowType = RowType.of(new VarCharType(), new IntType());
        // Flink Row → GenericRowData with recursive field conversion
        Row row = Row.ofKind(RowKind.INSERT, "Alice", 30);
        GenericRowData result = (GenericRowData) InternalTypeConverter.toInternal(row, rowType);
        assertEquals(StringData.fromString("Alice"), result.getString(0));
        assertEquals(30, result.getInt(1));
        // GenericRowData → pass-through
        GenericRowData grd = GenericRowData.of(StringData.fromString("x"), 1);
        assertSame(grd, InternalTypeConverter.toInternal(grd, rowType));
    }

    @Test
    public void testArray() {
        ArrayType intArrayType = new ArrayType(new IntType());
        ArrayType strArrayType = new ArrayType(new VarCharType());

        // List → GenericArrayData
        GenericArrayData fromList =
                (GenericArrayData)
                        InternalTypeConverter.toInternal(Arrays.asList(1, 2, 3), intArrayType);
        assertEquals(3, fromList.size());
        assertEquals(1, fromList.getInt(0));
        assertEquals(3, fromList.getInt(2));

        // Object[] → GenericArrayData with recursive element conversion
        GenericArrayData fromObjectArray =
                (GenericArrayData)
                        InternalTypeConverter.toInternal(new Object[] {"a", "b"}, strArrayType);
        assertEquals(StringData.fromString("a"), fromObjectArray.getString(0));
        assertEquals(StringData.fromString("b"), fromObjectArray.getString(1));

        // Iterable → GenericArrayData (ListState returns Iterable)
        GenericArrayData fromIterable =
                (GenericArrayData)
                        InternalTypeConverter.toInternal(
                                Arrays.asList(10L, 20L), new ArrayType(new BigIntType()));
        assertEquals(10L, fromIterable.getLong(0));
        assertEquals(20L, fromIterable.getLong(1));

        // GenericArrayData → pass-through
        GenericArrayData gad = new GenericArrayData(new Object[] {1, 2});
        assertSame(gad, InternalTypeConverter.toInternal(gad, intArrayType));
    }

    @Test
    public void testMap() {
        MapType type = new MapType(new VarCharType(), new IntType());

        // Map → GenericMapData with recursive key/value conversion
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        GenericMapData fromMap = (GenericMapData) InternalTypeConverter.toInternal(map, type);
        assertEquals(2, fromMap.size());
        assertEquals(1, fromMap.get(StringData.fromString("a")));
        assertEquals(2, fromMap.get(StringData.fromString("b")));

        // Iterable<Map.Entry> → GenericMapData (MapState.entries() returns this)
        GenericMapData fromEntries =
                (GenericMapData) InternalTypeConverter.toInternal(map.entrySet(), type);
        assertEquals(1, fromEntries.get(StringData.fromString("a")));
        assertEquals(2, fromEntries.get(StringData.fromString("b")));

        // GenericMapData → pass-through
        Map<Object, Object> inner = new HashMap<>();
        inner.put(StringData.fromString("k"), 99);
        GenericMapData gmd = new GenericMapData(inner);
        assertSame(gmd, InternalTypeConverter.toInternal(gmd, type));
    }

    @Test
    public void testMultiset() {
        // MultisetType is not a MapType: it only carries an element type, and is represented
        // internally as Map<element, Integer> (element -> multiplicity).
        MultisetType type = new MultisetType(new VarCharType());

        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 3);
        map.put("b", 1);
        GenericMapData fromMap = (GenericMapData) InternalTypeConverter.toInternal(map, type);
        assertEquals(2, fromMap.size());
        assertEquals(3, fromMap.get(StringData.fromString("a")));
        assertEquals(1, fromMap.get(StringData.fromString("b")));

        // Iterable<Map.Entry> → GenericMapData
        GenericMapData fromEntries =
                (GenericMapData) InternalTypeConverter.toInternal(map.entrySet(), type);
        assertEquals(3, fromEntries.get(StringData.fromString("a")));
        assertEquals(1, fromEntries.get(StringData.fromString("b")));

        // GenericMapData → pass-through
        Map<Object, Object> inner = new HashMap<>();
        inner.put(StringData.fromString("k"), 5);
        GenericMapData gmd = new GenericMapData(inner);
        assertSame(gmd, InternalTypeConverter.toInternal(gmd, type));
    }
}
