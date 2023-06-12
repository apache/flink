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

package org.apache.flink.table.jdbc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkResultSet}. */
public class FlinkResultSetTest {
    private static final int RECORD_SIZE = 5000;
    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("v1", DataTypes.BOOLEAN()),
                    Column.physical("v2", DataTypes.TINYINT()),
                    Column.physical("v3", DataTypes.SMALLINT()),
                    Column.physical("v4", DataTypes.INT()),
                    Column.physical("v5", DataTypes.BIGINT()),
                    Column.physical("v6", DataTypes.FLOAT()),
                    Column.physical("v7", DataTypes.DOUBLE()),
                    Column.physical("v8", DataTypes.DECIMAL(10, 5)),
                    Column.physical("v9", DataTypes.STRING()),
                    Column.physical("v10", DataTypes.BYTES()),
                    Column.physical(
                            "v11",
                            DataTypes.MAP(
                                    DataTypes.STRING(),
                                    DataTypes.MAP(DataTypes.INT(), DataTypes.BIGINT()))));

    @Test
    public void testResultSetPrimitiveData() throws Exception {
        CloseableIterator<RowData> data =
                CloseableIterator.adapterForIterator(
                        IntStream.range(0, RECORD_SIZE)
                                .boxed()
                                .map(
                                        v -> {
                                            Map<StringData, MapData> map = new HashMap<>();
                                            Map<Integer, Long> valueMap = new HashMap<>();
                                            valueMap.put(v, v.longValue());
                                            map.put(
                                                    StringData.fromString(v.toString()),
                                                    new GenericMapData(valueMap));
                                            return (RowData)
                                                    GenericRowData.of(
                                                            v % 2 == 0,
                                                            v.byteValue(),
                                                            v.shortValue(),
                                                            v,
                                                            v.longValue(),
                                                            (float) (v + 0.1),
                                                            v + 0.22,
                                                            DecimalData.fromBigDecimal(
                                                                    new BigDecimal(v + ".55555"),
                                                                    10,
                                                                    5),
                                                            StringData.fromString(v.toString()),
                                                            v.toString().getBytes(),
                                                            new GenericMapData(map));
                                        })
                                .iterator());
        try (ResultSet resultSet =
                new FlinkResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()))) {
            validateResultData(resultSet);
        }
    }

    @Test
    public void testStringResultSetNullData() throws Exception {
        CloseableIterator<RowData> data =
                CloseableIterator.adapterForIterator(
                        Collections.singletonList(
                                        (RowData)
                                                GenericRowData.of(
                                                        null, null, null, null, null, null, null,
                                                        null, null, null, null))
                                .iterator());
        try (ResultSet resultSet =
                new FlinkResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                SCHEMA, data, true, ResultKind.SUCCESS, JobID.generate()))) {
            assertTrue(resultSet.next());
            assertFalse(resultSet.getBoolean(1));
            assertNull(resultSet.getObject(1));
            assertEquals((byte) 0, resultSet.getByte(2));
            assertNull(resultSet.getObject(2));
            assertEquals((short) 0, resultSet.getShort(3));
            assertNull(resultSet.getObject(3));
            assertEquals(0, resultSet.getInt(4));
            assertNull(resultSet.getObject(4));
            assertEquals(0L, resultSet.getLong(5));
            assertNull(resultSet.getObject(5));
            assertEquals((float) 0.0, resultSet.getFloat(6));
            assertNull(resultSet.getObject(6));
            assertEquals(0.0, resultSet.getDouble(7));
            assertNull(resultSet.getObject(7));
            assertNull(resultSet.getBigDecimal(8));
            assertNull(resultSet.getObject(8));
            assertNull(resultSet.getString(9));
            assertNull(resultSet.getObject(9));
            assertNull(resultSet.getBytes(10));
            assertNull(resultSet.getObject(10));
            assertNull(resultSet.getObject(11));
            assertFalse(resultSet.next());
        }
    }

    private static void validateResultData(ResultSet resultSet) throws SQLException {
        int resultCount = 0;
        while (resultSet.next()) {
            Integer val = resultSet.getInt("v4");
            assertEquals(val, resultCount);
            resultCount++;

            // Get and validate each column value
            assertEquals(val % 2 == 0, resultSet.getBoolean(1));
            assertEquals(val % 2 == 0, resultSet.getBoolean("v1"));
            assertEquals(val % 2 == 0, resultSet.getObject(1));
            assertEquals(val % 2 == 0, resultSet.getObject("v1"));
            assertEquals(val.byteValue(), resultSet.getByte(2));
            assertEquals(val.byteValue(), resultSet.getByte("v2"));
            assertEquals(val.byteValue(), resultSet.getObject(2));
            assertEquals(val.byteValue(), resultSet.getObject("v2"));
            assertEquals(val.shortValue(), resultSet.getShort(3));
            assertEquals(val.shortValue(), resultSet.getShort("v3"));
            assertEquals(val.shortValue(), resultSet.getObject(3));
            assertEquals(val.shortValue(), resultSet.getObject("v3"));
            assertEquals(val, resultSet.getInt(4));
            assertEquals(val, resultSet.getInt("v4"));
            assertEquals(val, resultSet.getObject(4));
            assertEquals(val, resultSet.getObject("v4"));
            assertEquals(val.longValue(), resultSet.getLong(5));
            assertEquals(val.longValue(), resultSet.getLong("v5"));
            assertEquals(val.longValue(), resultSet.getObject(5));
            assertEquals(val.longValue(), resultSet.getObject("v5"));
            assertTrue(resultSet.getFloat(6) - val - 0.1 < 0.0001);
            assertTrue(resultSet.getFloat("v6") - val - 0.1 < 0.0001);
            assertTrue((float) resultSet.getObject(6) - val - 0.1 < 0.0001);
            assertTrue((float) resultSet.getObject("v6") - val - 0.1 < 0.0001);
            assertTrue(resultSet.getDouble(7) - val - 0.22 < 0.0001);
            assertTrue(resultSet.getDouble("v7") - val - 0.22 < 0.0001);
            assertTrue((double) resultSet.getObject(7) - val - 0.22 < 0.0001);
            assertTrue((double) resultSet.getObject("v7") - val - 0.22 < 0.0001);
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getBigDecimal(8));
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getBigDecimal("v8"));
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getObject(8));
            assertEquals(new BigDecimal(val + ".55555"), resultSet.getObject("v8"));
            assertEquals(val.toString(), resultSet.getString(9));
            assertEquals(val.toString(), resultSet.getString("v9"));
            assertEquals(val.toString(), resultSet.getObject(9));
            assertEquals(val.toString(), resultSet.getObject("v9"));
            assertEquals(val.toString(), new String(resultSet.getBytes(10)));
            assertEquals(val.toString(), new String(resultSet.getBytes("v10")));
            assertEquals(val.toString(), new String((byte[]) resultSet.getObject(10)));
            assertEquals(val.toString(), new String((byte[]) resultSet.getObject("v10")));

            // Validate map data
            Map<String, Map<Integer, Long>> map = new HashMap<>();
            Map<Integer, Long> valueMap = new HashMap<>();
            valueMap.put(val, val.longValue());
            map.put(String.valueOf(val), valueMap);
            assertEquals(map, resultSet.getObject(11));
            assertEquals(map, resultSet.getObject("v11"));

            // Get data according to wrong data type
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(1),
                    "java.lang.ClassCastException: java.lang.Boolean cannot be cast to java.lang.Long");
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(6),
                    "java.lang.ClassCastException: java.lang.Float cannot be cast to java.lang.Long");
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(7),
                    "java.lang.ClassCastException: java.lang.Double cannot be cast to java.lang.Long");
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong(8),
                    "java.lang.ClassCastException: java.lang.BigDecimal cannot be cast to java.lang.Long");

            // Get not exist column
            assertThrowsExactly(
                    SQLDataException.class,
                    () -> resultSet.getLong("id1"),
                    "Column[id1] is not exist");
            assertThrowsExactly(
                    SQLException.class, () -> resultSet.getLong(12), "Column[11] is not exist");
            assertThrowsExactly(
                    SQLException.class, () -> resultSet.getLong(-1), "Column[-1] is not exist");
        }
        assertEquals(resultCount, RECORD_SIZE);
    }
}
