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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for EXPLAIN statements. */
public class ExplainTest extends TableTestBase {

    /**
     * When the {@code PLAN_TEST_FORCE_OVERWRITE} environment variable is set {@link #verifyExplain}
     * overwrites the golden files.
     */
    private static final boolean REGENERATE_FILES =
            "true".equalsIgnoreCase(System.getenv("PLAN_TEST_FORCE_OVERWRITE"));

    private StreamTableTestUtil util;
    private TestInfo testInfo;

    @BeforeEach
    void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
        this.util = streamTestUtil(TableConfig.getDefault());
        this.util
                .getTableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
    }

    @Test
    void testExplainCreateMaterializedTable() {
        verifyExplain(
                "CREATE MATERIALIZED TABLE MyMTTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainCreateMaterializedTableDefaultsToOnConflictErrorWhenEnabled() {
        util.getTableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_SINK_MATERIALIZED_TABLE_FORCES_ON_CONFLICT_ERROR,
                        true);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyWatermarkedTable (\n"
                                + "  a INT,\n"
                                + "  c STRING,\n"
                                + "  ts TIMESTAMP(3),\n"
                                + "  WATERMARK FOR ts AS ts\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
        // Upsert key is (a, c) from GROUP BY; declared PK is (a) alone - a conflict.
        verifyExplain(
                "CREATE MATERIALIZED TABLE MyMTOnConflictTable (\n"
                        + "  a INT,\n"
                        + "  cnt BIGINT,\n"
                        + "  PRIMARY KEY (cnt) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ") AS\n"
                        + "  SELECT a, COUNT(*) AS cnt FROM MyWatermarkedTable GROUP BY a, c");
    }

    @Test
    void testExplainCreateMaterializedTableNoDuplicatesOnConflictErrorWhenEnabled() {
        util.getTableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_SINK_MATERIALIZED_TABLE_FORCES_ON_CONFLICT_ERROR,
                        true);
        // Upsert key (a) matches the declared PK (a): no materializer should be inserted,
        // flag or no flag. COALESCE keeps the grouping key NOT NULL for the PK column.
        verifyExplain(
                "CREATE MATERIALIZED TABLE MyMTNoConflictTable (\n"
                        + "  a INT,\n"
                        + "  cnt BIGINT,\n"
                        + "  PRIMARY KEY (a) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ") AS\n"
                        + "  SELECT COALESCE(a, 0) AS a, COUNT(*) AS cnt FROM MyTable"
                        + " GROUP BY COALESCE(a, 0)");
    }

    @Test
    void testExplainCreateMaterializedTableKeepsOldDefaultWhenSourceHasNoWatermark() {
        util.getTableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_SINK_MATERIALIZED_TABLE_FORCES_ON_CONFLICT_ERROR,
                        true);
        // Isolate from table.exec.sink.require-on-conflict as above.
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, false);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyWatermarkedTable (\n"
                                + "  a INT,\n"
                                + "  c STRING,\n"
                                + "  ts TIMESTAMP(3),\n"
                                + "  WATERMARK FOR ts AS ts\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
        // A join across a watermarked and an unwatermarked source: not every source has a
        // watermark, so the fallback must still apply even though one branch alone would pass.
        verifyExplain(
                "CREATE MATERIALIZED TABLE MyMTJoinedSourcesTable (\n"
                        + "  a INT,\n"
                        + "  cnt BIGINT,\n"
                        + "  PRIMARY KEY (cnt) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ") AS\n"
                        + "  SELECT w.a, COUNT(*) AS cnt FROM MyWatermarkedTable w"
                        + " JOIN MyTable m ON w.a = m.a GROUP BY w.a, w.c");
    }

    @Test
    void testExplainCreateMaterializedTableKeepsOldDefaultWhenDisabled() {
        // Isolate from table.exec.sink.require-on-conflict as above.
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, false);
        verifyExplain(
                "CREATE MATERIALIZED TABLE MyMTNoConflictDefaultTable (\n"
                        + "  a INT,\n"
                        + "  cnt BIGINT,\n"
                        + "  PRIMARY KEY (cnt) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ") AS\n"
                        + "  SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a, c");
    }

    @Test
    void testExplainCreateMaterializedTableErrorMentionsWatermarkWhenBothOptionsEnabled() {
        util.getTableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions
                                .TABLE_EXEC_SINK_MATERIALIZED_TABLE_FORCES_ON_CONFLICT_ERROR,
                        true);
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, true);
        // With both options on and no watermark, the message also points at the missing
        // watermark - the actionable fix for a materialized table, which has no ON CONFLICT
        // syntax to satisfy the generic advice above it.
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .explainSql(
                                                "CREATE MATERIALIZED TABLE MyMTBothOptionsTable (\n"
                                                        + "  a INT,\n"
                                                        + "  cnt BIGINT,\n"
                                                        + "  PRIMARY KEY (cnt) NOT ENFORCED\n"
                                                        + ") WITH (\n"
                                                        + "  'connector' = 'values',\n"
                                                        + "  'sink-insert-only' = 'false'\n"
                                                        + ") AS\n"
                                                        + "  SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a, c"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Please specify an ON CONFLICT clause")
                .hasMessageContaining("table.exec.sink.materialized-table-forces-on-conflict-error")
                .hasMessageContaining("watermark is missing on")
                .hasMessageContaining("MyTable");
    }

    @Test
    void testExplainCreateOrAlterMaterializedTable() {
        verifyExplain(
                "CREATE OR ALTER MATERIALIZED TABLE MyMTTable (\n"
                        + " `b`,\n"
                        + " `a`\n"
                        + " )\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    CAST(`a` AS BIGINT) AS `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainAlterMaterializedTable() {
        util.getTableEnv()
                .executeSql(
                        "CREATE OR ALTER MATERIALIZED TABLE MyMTTable\n"
                                + " WITH (\n"
                                + "   'connector' = 'values'\n"
                                + ") AS\n"
                                + "  SELECT\n"
                                + "    `a`,\n"
                                + "    `b`\n"
                                + "  FROM\n"
                                + "    MyTable");
        verifyExplain(
                "ALTER MATERIALIZED TABLE MyMTTable\n"
                        + "AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`,\n"
                        + "    `c`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainFullAlterMaterializedTable() {
        util.getTableEnv()
                .executeSql(
                        "CREATE OR ALTER MATERIALIZED TABLE MyMTTable\n"
                                + " WITH (\n"
                                + "   'connector' = 'values'\n"
                                + ") AS\n"
                                + "  SELECT\n"
                                + "    `a`,\n"
                                + "    `b`\n"
                                + "  FROM\n"
                                + "    MyTable");
        verifyExplain(
                "CREATE OR ALTER MATERIALIZED TABLE MyMTTable(\n"
                        + " `b`,\n"
                        + " `a`,\n"
                        + " `c`\n"
                        + " )\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ")\n"
                        + "AS\n"
                        + "  SELECT\n"
                        + "    CAST(`a` AS BIGINT) AS `a`,\n"
                        + "    `b`,\n"
                        + "    `c`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainConvertTableToMaterializedTable() {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(
                TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
        util.getTableEnv().getConfig().setRootConfiguration(rootConfiguration);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyConvertTable (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
        verifyExplain(
                "CREATE OR ALTER MATERIALIZED TABLE MyConvertTable\n"
                        + " AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainCreateTableAsSelect() {
        verifyExplain(
                "CREATE TABLE MyCtasTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtas");
    }

    @Test
    void testExplainReplaceTableAsSelect() {
        // Produces the same plan as CREATE TABLE AS SELECT.
        verifyExplain(
                "REPLACE TABLE MyCtasTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtas");
    }

    @Test
    void testExplainCreateTableAsSelectWithColumnsInCreateAndQueryParts() {
        verifyExplain(
                "CREATE TABLE MyCtasTable(\n"
                        + "  `votes` INT,\n"
                        + "  `votes_2x` AS `b` * 2,\n"
                        + "  `metadata_col` BIGINT METADATA,\n"
                        + "  `virtual_col` STRING METADATA VIRTUAL\n"
                        + ")\n"
                        + " WITH (\n"
                        + "   'connector' = 'values',\n"
                        + "   'readable-metadata' = 'metadata_col:BIGINT, virtual_col:STRING',\n"
                        + "   'writable-metadata' = 'metadata_col:BIGINT'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtasWithColumnsInCreateAndQueryParts");
    }

    @Test
    void testExplainReplaceTableAsSelectWithColumnsInCreateAndQueryParts() {
        // Produces the same plan as CREATE TABLE AS SELECT.
        verifyExplain(
                "REPLACE TABLE MyCtasTable(\n"
                        + "  `votes` INT,\n"
                        + "  `votes_2x` AS `b` * 2,\n"
                        + "  `metadata_col` BIGINT METADATA,\n"
                        + "  `virtual_col` STRING METADATA VIRTUAL\n"
                        + ")\n"
                        + " WITH (\n"
                        + "   'connector' = 'values',\n"
                        + "   'readable-metadata' = 'metadata_col:BIGINT, virtual_col:STRING',\n"
                        + "   'writable-metadata' = 'metadata_col:BIGINT'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtasWithColumnsInCreateAndQueryParts");
    }

    private void verifyExplain(final String statement) {
        final String displayName = this.testInfo.getDisplayName();
        verifyExplain(statement, displayName.substring(0, displayName.length() - 2));
    }

    private void verifyExplain(final String statement, final String fileName) {
        final String actual = util.getTableEnv().explainSql(statement);
        final String fullFileName = fileName + ".out";
        if (REGENERATE_FILES) {
            writeToResource(fullFileName, actual);
            return;
        }
        final String expected = TableTestUtil.readFromResource("/explain/" + fullFileName);
        assertThat(TableTestUtil.replaceStageId(actual))
                .isEqualTo(TableTestUtil.replaceStageId(expected));
    }

    private void writeToResource(final String fileName, final String content) {
        try {
            final Path testClassesRoot = Paths.get(getClass().getResource("/").toURI());
            final Path resourcesRoot =
                    Paths.get(
                            testClassesRoot
                                    .toString()
                                    .replace("target/test-classes", "src/test/resources"));
            Files.writeString(resourcesRoot.resolve("explain").resolve(fileName), content);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to regenerate golden file " + fileName, e);
        }
    }
}
