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

package org.apache.flink.table.planner.plan.stream.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withAllColumns;
import static org.apache.flink.table.api.Expressions.withColumns;
import static org.apache.flink.table.api.Expressions.withoutColumns;
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

        tableEnv.getConfig().set(TABLE_COLUMN_EXPANSION_STRATEGY, Collections.emptyList());
    }

    @Test
    public void testExcludeDefaultVirtualMetadataColumns() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Collections.singletonList(EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS));

        // Special case: 'from()' which does not count as an expansion
        assertColumnNames(
                tableEnv.from("t1"),
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From one table
        assertColumnNames(
                tableEnv.from("t1").select(withAllColumns()),
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From one table with explicit selection of metadata column
        assertColumnNames(
                tableEnv.from("t1").select($("t1_m_virtual"), withAllColumns()),
                "t1_m_virtual",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // Transitive metadata columns are always selected
        assertColumnNames(
                tableEnv.from("t1")
                        .select($("t1_m_virtual"), withAllColumns())
                        .select(withAllColumns()),
                "t1_m_virtual",
                "t1_i",
                "t1_s",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");
    }

    @Test
    public void testExcludeAliasedVirtualMetadataColumns() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Collections.singletonList(EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS));

        // Special case: 'from()' which does not count as an expansion
        assertColumnNames(
                tableEnv.from("t1"),
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_aliased_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From one table
        assertColumnNames(
                tableEnv.from("t1").select(withAllColumns()),
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // From one table with explicit selection of metadata column
        assertColumnNames(
                tableEnv.from("t1").select($("t1_m_aliased_virtual"), withAllColumns()),
                "t1_m_aliased_virtual",
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased");

        // Transitive metadata columns are always selected
        assertColumnNames(
                tableEnv.from("t1")
                        .select($("t1_m_aliased_virtual"), withAllColumns())
                        .select(withAllColumns()),
                "t1_m_aliased_virtual",
                "t1_i",
                "t1_s",
                "t1_m_virtual",
                "t1_m_default",
                "t1_m_aliased");
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

        assertColumnNames(tableEnv.from("v1"), "t1_i", "t1_s", "t1_m_default", "t1_m_aliased");
    }

    @Test
    public void testExcludeViaWithColumn() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Arrays.asList(
                                EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS,
                                EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS));

        assertColumnNames(
                tableEnv.from("t1").select(withColumns(range("t1_i", "t1_m_aliased"))),
                "t1_i",
                "t1_s",
                "t1_m_default",
                "t1_m_aliased");
    }

    @Test
    public void testExcludeViaWithoutColumn() {
        tableEnv.getConfig()
                .set(
                        TABLE_COLUMN_EXPANSION_STRATEGY,
                        Arrays.asList(
                                EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS,
                                EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS));

        assertColumnNames(
                tableEnv.from("t1").select(withoutColumns($("t1_i"), $("t1_s"))),
                "t1_m_default",
                "t1_m_aliased");
    }

    private void assertColumnNames(Table query, String... columnNames) {
        assertThat(query.getResolvedSchema().getColumnNames()).containsExactly(columnNames);
    }
}
