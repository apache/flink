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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.assertj.core.api.Assertions.assertThat;

/** Plan tests for the TO_APPEND built-in process table function. */
class ToAppendTest extends TableTestBase {

    private static final List<ExplainDetail> CHANGELOG_MODE = List.of(ExplainDetail.CHANGELOG_MODE);

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsert_source ("
                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UA,D',"
                                + "  'source.produces-delete-by-key' = 'true'"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsert_source_with_full_deletes ("
                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UA,D'"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE retract_source ("
                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
    }

    @Test
    void testUpsertSourceWithPartialDeletes() {
        util.verifyRelPlan("SELECT * FROM TO_APPEND(input => TABLE upsert_source)", CHANGELOG_MODE);
    }

    @Test
    void testUpsertSourceWithFullDeletes() {
        util.verifyRelPlan(
                "SELECT * FROM TO_APPEND(input => TABLE upsert_source_with_full_deletes)",
                CHANGELOG_MODE);
    }

    @Test
    void testRetractSource() {
        util.verifyRelPlan(
                "SELECT * FROM TO_APPEND(input => TABLE retract_source)", CHANGELOG_MODE);
    }

    @Test
    void testInputWithoutUpsertKey() {
        util.tableEnv().executeSql("CREATE VIEW names AS SELECT name FROM upsert_source");
        util.verifyRelPlan("SELECT * FROM TO_APPEND(input => TABLE names)", CHANGELOG_MODE);
    }

    @Test
    void testOutputTypesMatchInput() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE typed_source ("
                                + "  id INT NOT NULL PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  amount DECIMAL(10, 2) NOT NULL,"
                                + "  tags ARRAY<STRING>,"
                                + "  address ROW<city STRING, zip INT NOT NULL>,"
                                + "  ts TIMESTAMP(3),"
                                + "  pt AS PROCTIME(),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UA,D'"
                                + ")");
        final ResolvedSchema input = util.tableEnv().from("typed_source").getResolvedSchema();
        final ResolvedSchema output =
                util.tableEnv()
                        .sqlQuery("SELECT * FROM TO_APPEND(input => TABLE typed_source)")
                        .getResolvedSchema();
        assertThat(output.getColumnNames()).isEqualTo(input.getColumnNames());
        assertThat(output.getColumnDataTypes()).isEqualTo(input.getColumnDataTypes());
        assertThat(isRowtimeAttribute(logicalType(output, "ts"))).isTrue();
        assertThat(isProctimeAttribute(logicalType(output, "pt"))).isTrue();
    }

    @Test
    void testOnTime() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE append_source ("
                                + "  id INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ('connector' = 'values')");
        final ResolvedSchema output =
                util.tableEnv()
                        .sqlQuery(
                                "SELECT * FROM TO_APPEND("
                                        + "input => TABLE append_source, "
                                        + "on_time => DESCRIPTOR(ts))")
                        .getResolvedSchema();
        assertThat(output.getColumnNames()).containsExactly("id", "ts", "rowtime");
        assertThat(isRowtimeAttribute(logicalType(output, "ts"))).isTrue();
        assertThat(isRowtimeAttribute(logicalType(output, "rowtime"))).isTrue();
    }

    @Test
    void testTimeAttributesAfterSameRowTypeWithout() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE changelog_source ("
                                + "  op STRING,"
                                + "  id INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE append_source ("
                                + "  id INT,"
                                + "  ts TIMESTAMP(3),"
                                + "  WATERMARK FOR ts AS ts"
                                + ") WITH ('connector' = 'values')");
        final ResolvedSchema withoutTimeAttributes =
                util.tableEnv()
                        .sqlQuery("SELECT * FROM FROM_CHANGELOG(input => TABLE changelog_source)")
                        .getResolvedSchema();
        final ResolvedSchema withTimeAttributes =
                util.tableEnv()
                        .sqlQuery("SELECT * FROM TO_APPEND(input => TABLE append_source)")
                        .getResolvedSchema();
        assertThat(isRowtimeAttribute(logicalType(withoutTimeAttributes, "ts"))).isFalse();
        assertThat(isRowtimeAttribute(logicalType(withTimeAttributes, "ts"))).isTrue();
    }

    private static LogicalType logicalType(final ResolvedSchema schema, final String column) {
        return schema.getColumn(column).orElseThrow().getDataType().getLogicalType();
    }
}
