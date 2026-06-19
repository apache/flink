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

package org.apache.flink.table.gateway.utils;

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.gateway.rest.util.RowDataLocalTimeZoneConverter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link RowDataLocalTimeZoneConverter}. */
public class RowDataLocalTimeZoneConverterTest {
    @Test
    public void testCheckHasTimeZoneData() {
        List<LogicalType> logicalTypeListWithoutTimestamp =
                Collections.singletonList(new IntType());
        List<LogicalType> logicalTypeListWithTimestamp =
                Collections.singletonList(new LocalZonedTimestampType());
        List<LogicalType> logicalTypeListWithMapTimestamp =
                Arrays.asList(
                        new IntType(),
                        new MapType(
                                new VarCharType(100),
                                new MapType(new VarCharType(20), new LocalZonedTimestampType())));
        List<LogicalType> logicalTypeListWithMultisetTimestamp =
                Arrays.asList(new IntType(), new MultisetType(new LocalZonedTimestampType()));
        List<LogicalType> logicalTypeListWithRowTimestamp =
                Arrays.asList(
                        new VarCharType(100),
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("a", new LocalZonedTimestampType()),
                                        new RowType.RowField("b", new IntType()))));
        assertFalse(
                new RowDataLocalTimeZoneConverter(
                                logicalTypeListWithoutTimestamp,
                                TimeZone.getTimeZone("Asia/Shanghai"))
                        .hasTimeZoneData());
        assertTrue(
                new RowDataLocalTimeZoneConverter(
                                logicalTypeListWithTimestamp, TimeZone.getTimeZone("Asia/Shanghai"))
                        .hasTimeZoneData());
        assertTrue(
                new RowDataLocalTimeZoneConverter(
                                logicalTypeListWithMapTimestamp,
                                TimeZone.getTimeZone("Asia/Shanghai"))
                        .hasTimeZoneData());
        assertTrue(
                new RowDataLocalTimeZoneConverter(
                                logicalTypeListWithMultisetTimestamp,
                                TimeZone.getTimeZone("Asia/Shanghai"))
                        .hasTimeZoneData());
        assertTrue(
                new RowDataLocalTimeZoneConverter(
                                logicalTypeListWithRowTimestamp,
                                TimeZone.getTimeZone("Asia/Shanghai"))
                        .hasTimeZoneData());
    }

    @Test
    public void testSimpleTimestampWithLocalZone() {
        List<LogicalType> logicalTypeList =
                Collections.singletonList(new LocalZonedTimestampType());
        RowDataLocalTimeZoneConverter converter1 =
                new RowDataLocalTimeZoneConverter(
                        logicalTypeList, TimeZone.getTimeZone("Asia/Shanghai"));
        RowDataLocalTimeZoneConverter converter2 =
                new RowDataLocalTimeZoneConverter(
                        logicalTypeList, TimeZone.getTimeZone("Europe/Berlin"));

        RowData data = GenericRowData.of(TimestampData.fromEpochMillis(100000000000L));
        RowData data1 = converter1.convertTimeZoneRowData(data);
        RowData data2 = converter2.convertTimeZoneRowData(data);
        assertEquals(data.toString(), "+I(1973-03-03T09:46:40)");
        assertEquals(data1.toString(), "+I(1973-03-03T17:46:40)");
        assertEquals(data2.toString(), "+I(1973-03-03T10:46:40)");
    }

    @Test
    public void testComplexTimestampWithLocalZone() {
        List<LogicalType> logicalTypeList =
                Arrays.asList(
                        new IntType(),
                        new MapType(
                                new VarCharType(100),
                                new MapType(new VarCharType(20), new LocalZonedTimestampType())));
        RowDataLocalTimeZoneConverter converter1 =
                new RowDataLocalTimeZoneConverter(
                        logicalTypeList, TimeZone.getTimeZone("Asia/Shanghai"));
        RowDataLocalTimeZoneConverter converter2 =
                new RowDataLocalTimeZoneConverter(
                        logicalTypeList, TimeZone.getTimeZone("Europe/Berlin"));

        Map<StringData, TimestampData> timestampMapValue = new HashMap<>();
        timestampMapValue.put(
                StringData.fromString("123"), TimestampData.fromEpochMillis(100000000000L));
        Map<StringData, MapData> timestampMapData = new HashMap<>();
        timestampMapData.put(StringData.fromString("321"), new GenericMapData(timestampMapValue));
        RowData data = GenericRowData.of(100, new GenericMapData(timestampMapData));
        RowData data1 = converter1.convertTimeZoneRowData(data);
        RowData data2 = converter2.convertTimeZoneRowData(data);

        assertEquals(2, data.getArity());
        assertEquals(2, data1.getArity());
        assertEquals(2, data2.getArity());

        assertEquals(100L, data.getInt(0));
        assertEquals(100L, data1.getInt(0));
        assertEquals(100L, data2.getInt(0));

        MapData mapData = data.getMap(1);
        MapData mapData1 = data1.getMap(1);
        MapData mapData2 = data2.getMap(1);
        assertEquals(1, mapData.size());
        assertEquals(1, mapData1.size());
        assertEquals(1, mapData2.size());
        assertEquals("321", mapData.keyArray().getString(0).toString());
        assertEquals("321", mapData1.keyArray().getString(0).toString());
        assertEquals("321", mapData2.keyArray().getString(0).toString());

        MapData mapValue = mapData.valueArray().getMap(0);
        MapData mapValue1 = mapData1.valueArray().getMap(0);
        MapData mapValue2 = mapData2.valueArray().getMap(0);
        assertEquals(1, mapValue.size());
        assertEquals(1, mapValue1.size());
        assertEquals(1, mapValue2.size());
        assertEquals("123", mapValue.keyArray().getString(0).toString());
        assertEquals("123", mapValue1.keyArray().getString(0).toString());
        assertEquals("123", mapValue2.keyArray().getString(0).toString());
        assertEquals("1973-03-03T09:46:40", mapValue.valueArray().getTimestamp(0, 0).toString());
        assertEquals("1973-03-03T17:46:40", mapValue1.valueArray().getTimestamp(0, 0).toString());
        assertEquals("1973-03-03T10:46:40", mapValue2.valueArray().getTimestamp(0, 0).toString());
    }
}
