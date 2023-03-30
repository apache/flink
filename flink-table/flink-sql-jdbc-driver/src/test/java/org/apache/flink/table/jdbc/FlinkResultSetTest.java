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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkResultSet}. */
public class FlinkResultSetTest {
    private static final int RECORD_SIZE = 5000;

    @Test
    public void testResultSetPrimitiveData() throws Exception {
        CloseableIterator<RowData> data =
                CloseableIterator.adapterForIterator(
                        IntStream.range(0, RECORD_SIZE)
                                .boxed()
                                .map(
                                        v ->
                                                (RowData)
                                                        GenericRowData.of(
                                                                v % 2 == 0,
                                                                v.byteValue(),
                                                                v.shortValue(),
                                                                v,
                                                                v.longValue(),
                                                                (float) (v + 0.1),
                                                                v + 0.22,
                                                                DecimalData.fromBigDecimal(
                                                                        new BigDecimal(
                                                                                v + ".55555"),
                                                                        10,
                                                                        5),
                                                                StringData.fromString(v.toString()),
                                                                v.toString().getBytes()))
                                .iterator());
        int resultCount = 0;
        try (ResultSet resultSet =
                new FlinkResultSet(
                        new TestingStatement(),
                        new StatementResult(
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
                                        Column.physical("v10", DataTypes.BYTES())),
                                data,
                                true,
                                ResultKind.SUCCESS,
                                JobID.generate()))) {
            while (resultSet.next()) {
                Integer val = resultSet.getInt("v4");
                assertEquals(val, resultCount);
                resultCount++;

                // Get and validate each column value
                assertEquals(val % 2 == 0, resultSet.getBoolean(1));
                assertEquals(val % 2 == 0, resultSet.getBoolean("v1"));
                assertEquals(val.byteValue(), resultSet.getByte(2));
                assertEquals(val.byteValue(), resultSet.getByte("v2"));
                assertEquals(val.shortValue(), resultSet.getShort(3));
                assertEquals(val.shortValue(), resultSet.getShort("v3"));
                assertEquals(val, resultSet.getInt(4));
                assertEquals(val, resultSet.getInt("v4"));
                assertEquals(val.longValue(), resultSet.getLong(5));
                assertEquals(val.longValue(), resultSet.getLong("v5"));
                assertTrue(resultSet.getFloat(6) - val - 0.1 < 0.0001);
                assertTrue(resultSet.getFloat("v6") - val - 0.1 < 0.0001);
                assertTrue(resultSet.getDouble(7) - val - 0.22 < 0.0001);
                assertTrue(resultSet.getDouble("v7") - val - 0.22 < 0.0001);
                assertEquals(new BigDecimal(val + ".55555"), resultSet.getBigDecimal(8));
                assertEquals(new BigDecimal(val + ".55555"), resultSet.getBigDecimal("v8"));
                assertEquals(val.toString(), resultSet.getString(9));
                assertEquals(val.toString(), resultSet.getString("v9"));
                assertEquals(val.toString(), new String(resultSet.getBytes(10)));
                assertEquals(val.toString(), new String(resultSet.getBytes("v10")));

                // Get data according to wrong data type
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(1),
                        "java.lang.ClassCastException: java.lang.Boolean cannot be cast to java.lang.Long");
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(2),
                        "java.lang.ClassCastException: java.lang.Byte cannot be cast to java.lang.Long");
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(3),
                        "java.lang.ClassCastException: java.lang.Short cannot be cast to java.lang.Long");
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(4),
                        "java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Long");
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getInt(5),
                        "java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.Int");
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
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(9),
                        "java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.Long");
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(10),
                        "java.lang.ClassCastException: java.lang.byte[] cannot be cast to java.lang.Long");

                // Get not exist column
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong("id1"),
                        "Column[id1] is not exist");
                assertThrowsExactly(
                        SQLException.class, () -> resultSet.getLong(11), "Column[11] is not exist");
                assertThrowsExactly(
                        SQLException.class, () -> resultSet.getLong(-1), "Column[-1] is not exist");
            }
        }
        assertEquals(resultCount, RECORD_SIZE);
    }
}
