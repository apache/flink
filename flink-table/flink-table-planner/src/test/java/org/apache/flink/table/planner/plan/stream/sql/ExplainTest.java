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

    private void verifyExplain(final String statement) {
        final String actual = util.getTableEnv().explainSql(statement);
        final String displayName = this.testInfo.getDisplayName();
        final String fileName = displayName.substring(0, displayName.length() - 2) + ".out";
        if (REGENERATE_FILES) {
            writeToResource(fileName, actual);
            return;
        }
        final String expected = TableTestUtil.readFromResource("/explain/" + fileName);
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
