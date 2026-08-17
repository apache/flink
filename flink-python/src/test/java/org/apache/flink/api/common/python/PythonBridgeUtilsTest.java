/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.python;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PythonBridgeUtils}. */
class PythonBridgeUtilsTest {

    @Test
    void testPickleNull() throws Exception {
        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(null, new IntType());

        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isEqualTo(0);
    }

    @Test
    void testPickleDate() throws Exception {
        LocalDate date = LocalDate.of(2025, 9, 26);
        DateType dateType = new DateType();

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(date, dateType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleTime() throws Exception {
        LocalTime time = LocalTime.of(12, 30, 45, 123000000);
        TimeType timeType = new TimeType();

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(time, timeType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleTimestamp() throws Exception {
        LocalDateTime timestamp = LocalDateTime.of(2025, 9, 26, 12, 30, 45, 123000000);
        TimestampType timestampType = new TimestampType(6);

        Object pickledBytes =
                PythonBridgeUtils.getPickledBytesFromJavaObject(timestamp, timestampType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleLocalZonedTimestamp() throws Exception {
        Instant instant = Instant.ofEpochSecond(1727337723L, 123000000L);
        LocalZonedTimestampType ltzType = new LocalZonedTimestampType(6);

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(instant, ltzType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleLocalZonedTimestampWithDifferentPrecisions() throws Exception {
        Instant instant = Instant.ofEpochSecond(1727337723L, 123456789L);

        for (int precision = 0; precision <= 9; precision++) {
            LocalZonedTimestampType ltzType = new LocalZonedTimestampType(precision);
            Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(instant, ltzType);

            assertThat(pickledBytes).isNotNull();
            assertThat(pickledBytes).isInstanceOf(byte[].class);
        }
    }

    @Test
    void testPickleFloat() throws Exception {
        float value = 3.14159f;
        FloatType floatType = new FloatType();

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(value, floatType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleString() throws Exception {
        String value = "test string";
        VarCharType varcharType = VarCharType.STRING_TYPE;

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(value, varcharType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleArray() throws Exception {
        Integer[] array = {1, 2, 3, 4, 5};
        ArrayType arrayType = new ArrayType(new IntType());

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(array, arrayType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleMap() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        MapType mapType = new MapType(VarCharType.STRING_TYPE, new IntType());

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(map, mapType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }

    @Test
    void testPickleRow() throws Exception {
        RowType rowType = RowType.of(new IntType(), VarCharType.STRING_TYPE, new DateType());

        Row row = new Row(3);
        row.setField(0, 42);
        row.setField(1, "test");
        row.setField(2, LocalDate.of(2025, 9, 26));

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(row, rowType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(List.class);
        List<?> rowBytes = (List<?>) pickledBytes;
        assertThat(rowBytes).hasSize(4);
    }

    @Test
    void testPickleNestedStructures() throws Exception {
        ArrayType arrayType = new ArrayType(new IntType());
        MapType mapType = new MapType(VarCharType.STRING_TYPE, arrayType);

        Map<String, Integer[]> nestedMap = new HashMap<>();
        nestedMap.put("numbers", new Integer[] {1, 2, 3});
        nestedMap.put("more", new Integer[] {4, 5, 6});

        Object pickledBytes = PythonBridgeUtils.getPickledBytesFromJavaObject(nestedMap, mapType);

        assertThat(pickledBytes).isNotNull();
        assertThat(pickledBytes).isInstanceOf(byte[].class);
        assertThat(((byte[]) pickledBytes).length).isGreaterThan(0);
    }
}
