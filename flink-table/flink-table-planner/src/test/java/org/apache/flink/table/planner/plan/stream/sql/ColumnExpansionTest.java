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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy.EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS;
import static org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy.EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableConfigOptions#TABLE_COLUMN_EXPANSION_STRATEGY}. */
public class ColumnExpansionTest {

    private TableEnvironment tableEnv;

    @Before
    public void before() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql(
                "CREATE TABLE t1 (\n"
                        + "  t1_i INT,\n"
                        + "  t1_s STRING,\n"
                        + "  t1_m_virtual INT METADATA VIRTUAL,\n"
                        + "  t1_m_aliased_virtual STRING METADATA FROM 'k1' VIRTUAL,\n"
                        + "  t1_m_default INT METADATA,\n"
                        + "  t1_m_aliased STRING METADATA FROM 'k2'\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'readable-metadata' = 't1_m_virtual:INT,k1:STRING,t1_m_default:INT,k2:STRING'\n"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE t2 (\n"
                        + "  t2_i INT,\n"
                        + "  t2_s STRING,\n"
                        + "  t2_m_virtual INT METADATA VIRTUAL,\n"
                        + "  t2_m_aliased_virtual STRING METADATA FROM 'k1' VIRTUAL,\n"
                        + "  t2_m_default INT METADATA,\n"
                        + "  t2_m_aliased STRING METADATA FROM 'k2'\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'readable-metadata' = 't2_m_virtual:INT,k1:STRING,t2_m_default:INT,k2:STRING'\n"
                        + ")");

        tableEnv.executeSql(
                "CREATE TABLE t3 (\n"
                        + "  t3_s STRING,\n"
                        + "  t3_i INT,\n"
                        + "  t3_m_virtual TIMESTAMP_LTZ(3) METADATA VIRTUAL,\n"
                        + "  WATERMARK FOR t3_m_virtual AS t3_m_virtual - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'readable-metadata' = 't3_m_virtual:TIMESTAMP_LTZ(3)'\n"
                        + ")");

        tableEnv.getConfig().set(TABLE_COLUMN_EXPANSION_STRATEGY, Collections.emptyList());
    }

    @Test
    public void testExcludeDefaultVirtualMetadataColumns() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Collections.singletonList(EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS));

        // From one table
        assertColumnNames(
                "SELECT * FROM t1",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From one table with explicit selection of metadata column
        assertColumnNames(
                "SELECT t1_m_virtual, * FROM t1",
                "t1_m_virtual",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From two tables (i.e. implicit join)
        assertColumnNames(
                "SELECT * FROM t1, t2",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased",
                "t2_i",
                "t2_s",
                "t2_m_aliased_virtual",
                "t2_m_default",
                "t2_m_aliased");

        // From two tables (i.e. implicit join) with per table expansion
        assertColumnNames(
                "SELECT t1.*, t2.* FROM t1, t2",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased",
                "t2_i",
                "t2_s",
                "t2_m_aliased_virtual",
                "t2_m_default",
                "t2_m_aliased");

        // Transitive metadata columns are always selected
        assertColumnNames(
                "SELECT * FROM (SELECT t1_m_virtual, t2_m_virtual, * FROM t1, t2)",
                "t1_m_virtual",
                "t2_m_virtual",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased",
                "t2_i",
                "t2_s",
                "t2_m_aliased_virtual",
                "t2_m_default",
                "t2_m_aliased");
    }

    @Test
    public void testExcludeAliasedVirtualMetadataColumns() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Collections.singletonList(EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS));

        // From one table
        assertColumnNames(
                "SELECT * FROM t1", "t1_i", "t1_s", "t1_m_virtual", "t1_m_default", "t1_m_aliased");

        // From one table with explicit selection of metadata column
        assertColumnNames(
                "SELECT t1_m_aliased_virtual, * FROM t1",
                "t1_m_aliased_virtual",
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From two tables (i.e. implicit join)
        assertColumnNames(
                "SELECT * FROM t1, t2",
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased",
                "t2_i",
                "t2_s",
                "t2_m_virtual",
                "t2_m_default",
                "t2_m_aliased");

        // From two tables (i.e. implicit join) with per table expansion
        assertColumnNames(
                "SELECT t1.*, t2.* FROM t1, t2",
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased",
                "t2_i",
                "t2_s",
                "t2_m_virtual",
                "t2_m_default",
                "t2_m_aliased");

        // Transitive metadata columns are always selected
        assertColumnNames(
                "SELECT * FROM (SELECT t1_m_aliased_virtual, t2_m_aliased_virtual, * FROM t1, t2)",
                "t1_m_aliased_virtual",
                "t2_m_aliased_virtual",
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased",
                "t2_i",
                "t2_s",
                "t2_m_virtual",
                "t2_m_default",
                "t2_m_aliased");
    }

    @Test
    public void testExcludeViaView() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Arrays.asList(
                                EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS,
                                EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS));

        tableEnv.executeSql("CREATE VIEW v1 AS SELECT * FROM t1");

        assertColumnNames("SELECT * FROM v1", "t1_i", "t1_s", "t1_m_default", "t1_m_aliased");
    }

    @Test
    public void testExplicitTableWithinTableFunction() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Collections.singletonList(EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS));

        // t3_m_virtual is selected due to expansion of the explicit table expression
        // with hints from descriptor
        assertColumnNames(
                "SELECT * FROM TABLE(TUMBLE(TABLE t3, DESCRIPTOR(t3_m_virtual), INTERVAL '1' MINUTE))",
                "t3_s",
                "t3_i",
                "t3_m_virtual",
                "window_start",
                "window_end",
                "window_time");

        // Test common window TVF syntax
        assertColumnNames(
                "SELECT t3_s, SUM(t3_i) AS agg "
                        + "FROM TABLE(TUMBLE(TABLE t3, DESCRIPTOR(t3_m_virtual), INTERVAL '1' MINUTE)) "
                        + "GROUP BY t3_s, window_start, window_end",
                "t3_s",
                "agg");
    }

    @Test
    public void testExplicitTableWithinTableFunctionWithInsertIntoNamedColumns() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Collections.singletonList(EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS));

        tableEnv.executeSql(
                "CREATE TABLE sink (\n"
                        + "  a STRING,\n"
                        + "  c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")");

        // Test case for FLINK-33327, we can not assert column names of an INSERT INTO query. Make
        // sure the query can be planned.
        tableEnv.explainSql(
                "INSERT INTO sink(a, c) "
                        + "SELECT t3_s, COUNT(t3_i) FROM "
                        + " TABLE(TUMBLE(TABLE t3, DESCRIPTOR(t3_m_virtual), INTERVAL '1' MINUTE)) "
                        + "GROUP BY t3_s;");
    }

    private void assertColumnNames(String sql, String... columnNames) {
        assertThat(tableEnv.sqlQuery(sql).getResolvedSchema().getColumnNames())
                .containsExactly(columnNames);
    }
}
