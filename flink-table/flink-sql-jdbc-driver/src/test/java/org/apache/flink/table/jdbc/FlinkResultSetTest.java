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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void testResultSetPrimitiveData() throws Exception {
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
    void testStringResultSetNullData() throws Exception {
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
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getBoolean(1)).isFalse();
            assertThat(resultSet.getObject(1)).isNull();
            assertThat(resultSet.getByte(2)).isEqualTo((byte) 0);
            assertThat(resultSet.getObject(2)).isNull();
            assertThat(resultSet.getShort(3)).isEqualTo((short) 0);
            assertThat(resultSet.getObject(3)).isNull();
            assertThat(resultSet.getInt(4)).isEqualTo(0);
            assertThat(resultSet.getObject(4)).isNull();
            assertThat(resultSet.getLong(5)).isEqualTo(0L);
            assertThat(resultSet.getObject(5)).isNull();
            assertThat(resultSet.getFloat(6)).isEqualTo((float) 0.0);
            assertThat(resultSet.getObject(6)).isNull();
            assertThat(resultSet.getDouble(7)).isEqualTo(0.0);
            assertThat(resultSet.getObject(7)).isNull();
            assertThat(resultSet.getBigDecimal(8)).isNull();
            assertThat(resultSet.getObject(8)).isNull();
            assertThat(resultSet.getString(9)).isNull();
            assertThat(resultSet.getObject(9)).isNull();
            assertThat(resultSet.getBytes(10)).isNull();
            assertThat(resultSet.getObject(10)).isNull();
            assertThat(resultSet.getObject(11)).isNull();
            assertThat(resultSet.next()).isFalse();
        }
    }

    private static void validateResultData(ResultSet resultSet) throws SQLException {
        int resultCount = 0;
        while (resultSet.next()) {
            Integer val = resultSet.getInt("v4");
            assertThat(resultCount).isEqualTo(val);
            resultCount++;

            // Get and validate each column value
            assertThat(resultSet.getBoolean(1)).isEqualTo(val % 2 == 0);
            assertThat(resultSet.getBoolean("v1")).isEqualTo(val % 2 == 0);
            assertThat(resultSet.getObject(1)).isEqualTo(val % 2 == 0);
            assertThat(resultSet.getObject("v1")).isEqualTo(val % 2 == 0);
            assertThat(resultSet.getByte(2)).isEqualTo(val.byteValue());
            assertThat(resultSet.getByte("v2")).isEqualTo(val.byteValue());
            assertThat(resultSet.getObject(2)).isEqualTo(val.byteValue());
            assertThat(resultSet.getObject("v2")).isEqualTo(val.byteValue());
            assertThat(resultSet.getShort(3)).isEqualTo(val.shortValue());
            assertThat(resultSet.getShort("v3")).isEqualTo(val.shortValue());
            assertThat(resultSet.getObject(3)).isEqualTo(val.shortValue());
            assertThat(resultSet.getObject("v3")).isEqualTo(val.shortValue());
            assertThat(resultSet.getInt(4)).isEqualTo(val);
            assertThat(resultSet.getInt("v4")).isEqualTo(val);
            assertThat(resultSet.getObject(4)).isEqualTo(val);
            assertThat(resultSet.getObject("v4")).isEqualTo(val);
            assertThat(resultSet.getLong(5)).isEqualTo(val.longValue());
            assertThat(resultSet.getLong("v5")).isEqualTo(val.longValue());
            assertThat(resultSet.getObject(5)).isEqualTo(val.longValue());
            assertThat(resultSet.getObject("v5")).isEqualTo(val.longValue());
            assertThat(resultSet.getFloat(6) - val - 0.1 < 0.0001).isTrue();
            assertThat(resultSet.getFloat("v6") - val - 0.1 < 0.0001).isTrue();
            assertThat((float) resultSet.getObject(6) - val - 0.1 < 0.0001).isTrue();
            assertThat((float) resultSet.getObject("v6") - val - 0.1 < 0.0001).isTrue();
            assertThat(resultSet.getDouble(7) - val - 0.22 < 0.0001).isTrue();
            assertThat(resultSet.getDouble("v7") - val - 0.22 < 0.0001).isTrue();
            assertThat((double) resultSet.getObject(7) - val - 0.22 < 0.0001).isTrue();
            assertThat((double) resultSet.getObject("v7") - val - 0.22 < 0.0001).isTrue();
            assertThat(resultSet.getBigDecimal(8)).isEqualTo(new BigDecimal(val + ".55555"));
            assertThat(resultSet.getBigDecimal("v8")).isEqualTo(new BigDecimal(val + ".55555"));
            assertThat(resultSet.getObject(8)).isEqualTo(new BigDecimal(val + ".55555"));
            assertThat(resultSet.getObject("v8")).isEqualTo(new BigDecimal(val + ".55555"));
            assertThat(resultSet.getString(9)).isEqualTo(val.toString());
            assertThat(resultSet.getString("v9")).isEqualTo(val.toString());
            assertThat(resultSet.getObject(9)).isEqualTo(val.toString());
            assertThat(resultSet.getObject("v9")).isEqualTo(val.toString());
            assertThat(new String(resultSet.getBytes(10))).isEqualTo(val.toString());
            assertThat(new String(resultSet.getBytes("v10"))).isEqualTo(val.toString());
            assertThat(new String((byte[]) resultSet.getObject(10))).isEqualTo(val.toString());
            assertThat(new String((byte[]) resultSet.getObject("v10"))).isEqualTo(val.toString());

            // Validate map data
            Map<String, Map<Integer, Long>> map = new HashMap<>();
            Map<Integer, Long> valueMap = new HashMap<>();
            valueMap.put(val, val.longValue());
            map.put(String.valueOf(val), valueMap);
            assertThat(resultSet.getObject(11)).isEqualTo(map);
            assertThat(resultSet.getObject("v11")).isEqualTo(map);

            // Get data according to wrong data type
            assertThatThrownBy(() -> resultSet.getLong(1))
                    .as(
                            "java.lang.ClassCastException: java.lang.Boolean cannot be cast to java.lang.Long")
                    .isExactlyInstanceOf(SQLDataException.class);
            assertThatThrownBy(() -> resultSet.getLong(6))
                    .as(
                            "java.lang.ClassCastException: java.lang.Float cannot be cast to java.lang.Long")
                    .isExactlyInstanceOf(SQLDataException.class);
            assertThatThrownBy(() -> resultSet.getLong(7))
                    .as(
                            "java.lang.ClassCastException: java.lang.Double cannot be cast to java.lang.Long")
                    .isExactlyInstanceOf(SQLDataException.class);
            assertThatThrownBy(() -> resultSet.getLong(8))
                    .as(
                            "java.lang.ClassCastException: java.lang.BigDecimal cannot be cast to java.lang.Long")
                    .isExactlyInstanceOf(SQLDataException.class);

            // Get not exist column
            assertThatThrownBy(() -> resultSet.getLong("id1"))
                    .as("Column[id1] is not exist")
                    .isExactlyInstanceOf(SQLDataException.class);
            assertThatThrownBy(() -> resultSet.getLong(12))
                    .as("Column[11] is not exist")
                    .isExactlyInstanceOf(SQLException.class);
            assertThatThrownBy(() -> resultSet.getLong(-1))
                    .as("Column[-1] is not exist")
                    .isExactlyInstanceOf(SQLException.class);
        }
        assertThat(RECORD_SIZE).isEqualTo(resultCount);
    }
}
