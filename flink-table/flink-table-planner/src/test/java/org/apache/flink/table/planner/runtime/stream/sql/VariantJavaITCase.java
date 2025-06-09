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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.BinaryVariantBuilder;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VariantJavaITCase extends StreamingTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private BinaryVariantBuilder builder;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        builder = new BinaryVariantBuilder();
    }

    @Test
    void testParseJsonOfNull() {
        Table t =
                tEnv.fromChangelogStream(
                                env.fromData(Row.of("1"), new Row(1)),
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING().nullable())
                                        .build())
                        .as("s");
        tEnv.createTemporaryView("T_nullable", t);
        t =
                tEnv.fromChangelogStream(
                                env.fromData(Row.of("1"), Row.of("{")),
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING().notNull())
                                        .build())
                        .as("s");
        tEnv.createTemporaryView("T_notnull", t);

        Table resTable = tEnv.sqlQuery("SELECT PARSE_JSON(s) AS v FROM T_nullable");
        checkColumnType(resTable, "v", DataTypes.VARIANT());
        checkTableResult(resTable, Row.of(builder.of((byte) 1)), new Row(1));

        resTable = tEnv.sqlQuery("SELECT TRY_PARSE_JSON(s) AS v FROM T_nullable");
        checkColumnType(resTable, "v", DataTypes.VARIANT().nullable());
        checkTableResult(resTable, Row.of(builder.of((byte) 1)), new Row(1));

        resTable = tEnv.sqlQuery("SELECT PARSE_JSON(s) AS v FROM T_notnull");
        checkColumnType(resTable, "v", DataTypes.VARIANT().notNull());
        Table finalResTable = resTable;
        assertThatThrownBy(() -> finalResTable.execute().await())
                .hasStackTraceContaining("Failed to parse json string");

        resTable = tEnv.sqlQuery("SELECT TRY_PARSE_JSON(s) AS v FROM T_notnull");
        checkColumnType(resTable, "v", DataTypes.VARIANT().nullable());
        assertThat(CollectionUtil.iteratorToList(resTable.execute().collect()))
                .containsExactly(Row.of(builder.of((byte) 1)), new Row(1));
    }

    @Test
    void testFirstLastWithRetraction() {
        Variant v1 = builder.of(1);
        Variant v2 = builder.of(2);

        Table t =
                tEnv.fromChangelogStream(
                                env.fromData(
                                        Row.of(v1), Row.of(v2), Row.ofKind(RowKind.DELETE, v2)),
                                Schema.newBuilder().column("f0", DataTypes.VARIANT()).build())
                        .as("v");
        tEnv.createTemporaryView("T", t);

        Table resTable = tEnv.sqlQuery("SELECT FIRST_VALUE(v) AS fv, LAST_VALUE(v) AS lv FROM T");
        checkTableResult(
                resTable,
                Row.of(v1, v1),
                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v1),
                Row.ofKind(RowKind.UPDATE_AFTER, v1, v2),
                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v2),
                Row.ofKind(RowKind.UPDATE_AFTER, v1, v1));
    }

    private void checkTableResult(Table table, Row... expectedRows) {
        assertThat(CollectionUtil.iteratorToList(table.execute().collect()))
                .containsExactly(expectedRows);
    }

    private static void checkColumnType(Table table, String columnName, DataType expectedDataType) {
        assertThat(
                        table.getResolvedSchema()
                                .getColumn(columnName)
                                .orElseThrow(RuntimeException::new)
                                .getDataType())
                .isEqualTo(expectedDataType);
    }
}
