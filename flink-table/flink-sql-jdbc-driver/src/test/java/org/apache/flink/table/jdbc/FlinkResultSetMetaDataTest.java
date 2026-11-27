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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkResultSetMetaData}. */
public class FlinkResultSetMetaDataTest {
    @Test
    void testResultSetMetaData() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("c1", DataTypes.BOOLEAN()),
                        Column.physical("c2", DataTypes.TINYINT()),
                        Column.physical("c3", DataTypes.SMALLINT()),
                        Column.physical("c4", DataTypes.INT()),
                        Column.physical("c5", DataTypes.BIGINT()),
                        Column.physical("c6", DataTypes.FLOAT()),
                        Column.physical("c7", DataTypes.DOUBLE()),
                        Column.physical("c8", DataTypes.DECIMAL(10, 5)),
                        Column.physical("c9", DataTypes.STRING()),
                        Column.physical("c10", DataTypes.BYTES()),
                        Column.physical("c11", DataTypes.TIME()),
                        Column.physical("c12", DataTypes.DATE()),
                        Column.physical("c13", DataTypes.TIMESTAMP(6)),
                        Column.physical("c14", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(5)),
                        Column.physical("c15", DataTypes.TIMESTAMP_WITH_TIME_ZONE(4)),
                        Column.physical("c16", DataTypes.ARRAY(DataTypes.INT())),
                        Column.physical("c17", DataTypes.ROW()),
                        Column.physical(
                                "c18", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
        FlinkResultSetMetaData metaData =
                new FlinkResultSetMetaData(schema.getColumnNames(), schema.getColumnDataTypes());

        assertThat(metaData.getColumnCount()).isEqualTo(18);
        for (int i = 1; i <= 18; i++) {
            assertThat(metaData.getColumnName(i)).isEqualTo("c" + i);
            LogicalType logicalType = schema.getColumnDataTypes().get(i - 1).getLogicalType();
            assertThat(metaData.getColumnTypeName(i)).isEqualTo(logicalType.asSummaryString());
            assertThat(metaData.isSigned(i)).isEqualTo(logicalType.is(LogicalTypeFamily.NUMERIC));
        }

        assertThat(metaData.getColumnType(1)).isEqualTo(Types.BOOLEAN);
        assertThat(metaData.getColumnClassName(1)).isEqualTo(Boolean.class.getName());
        assertThat(metaData.getColumnType(2)).isEqualTo(Types.TINYINT);
        assertThat(metaData.getColumnClassName(2)).isEqualTo(Byte.class.getName());
        assertThat(metaData.getColumnType(3)).isEqualTo(Types.SMALLINT);
        assertThat(metaData.getColumnClassName(3)).isEqualTo(Short.class.getName());
        assertThat(metaData.getColumnType(4)).isEqualTo(Types.INTEGER);
        assertThat(metaData.getColumnClassName(4)).isEqualTo(Integer.class.getName());
        assertThat(metaData.getColumnType(5)).isEqualTo(Types.BIGINT);
        assertThat(metaData.getColumnClassName(5)).isEqualTo(Long.class.getName());
        assertThat(metaData.getColumnType(6)).isEqualTo(Types.FLOAT);
        assertThat(metaData.getColumnClassName(6)).isEqualTo(Float.class.getName());
        assertThat(metaData.getColumnType(7)).isEqualTo(Types.DOUBLE);
        assertThat(metaData.getColumnClassName(7)).isEqualTo(Double.class.getName());
        assertThat(metaData.getColumnType(8)).isEqualTo(Types.DECIMAL);
        assertThat(metaData.getColumnClassName(8)).isEqualTo(BigDecimal.class.getName());
        assertThat(metaData.getColumnType(9)).isEqualTo(Types.VARCHAR);
        assertThat(metaData.getColumnClassName(9)).isEqualTo(String.class.getName());
        assertThat(metaData.getColumnType(10)).isEqualTo(Types.VARBINARY);
        assertThat(metaData.getColumnClassName(10)).isEqualTo("byte[]");
        assertThat(metaData.getColumnType(11)).isEqualTo(Types.TIME);
        assertThat(metaData.getColumnClassName(11)).isEqualTo(Time.class.getName());
        assertThat(metaData.getColumnType(12)).isEqualTo(Types.DATE);
        assertThat(metaData.getColumnClassName(12)).isEqualTo(Date.class.getName());
        assertThat(metaData.getColumnType(13)).isEqualTo(Types.TIMESTAMP);
        assertThat(metaData.getColumnClassName(13)).isEqualTo(Timestamp.class.getName());
        assertThat(metaData.getColumnType(14)).isEqualTo(Types.TIMESTAMP);
        assertThat(metaData.getColumnClassName(14)).isEqualTo(Timestamp.class.getName());
        assertThat(metaData.getColumnType(15)).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
        assertThat(metaData.getColumnClassName(15)).isEqualTo(OffsetDateTime.class.getName());
        assertThat(metaData.getColumnType(16)).isEqualTo(Types.ARRAY);
        assertThat(metaData.getColumnClassName(16)).isEqualTo(Array.class.getName());
        assertThat(metaData.getColumnType(17)).isEqualTo(Types.STRUCT);
        assertThat(metaData.getColumnClassName(17)).isEqualTo(RowData.class.getName());
        assertThat(metaData.getColumnType(18)).isEqualTo(Types.JAVA_OBJECT);
        assertThat(metaData.getColumnClassName(18)).isEqualTo(Map.class.getName());
    }

    @Test
    void testInvalidType() {
        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("c1", DataTypes.MULTISET(DataTypes.STRING())));
        assertThatThrownBy(
                        () ->
                                new FlinkResultSetMetaData(
                                        schema.getColumnNames(), schema.getColumnDataTypes()))
                .as("Not supported type[MULTISET<STRING>]")
                .isExactlyInstanceOf(RuntimeException.class);
    }
}
