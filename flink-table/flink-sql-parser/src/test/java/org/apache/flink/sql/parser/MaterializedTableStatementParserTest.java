/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.parser.SqlParserFixture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Sql parser test for materialized related syntax. * */
@Execution(CONCURRENT)
public class MaterializedTableStatementParserTest {

    @Test
    void testCreateMaterializedTable() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source";
        final String expected =
                "CREATE MATERIALIZED TABLE `TBL1`\n"
                        + "(\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`";
        sql(sql).ok(expected);

        final String sql2 =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, h, t m FROM source";
        final String expected2 =
                "CREATE MATERIALIZED TABLE `TBL1`\n"
                        + "(\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`";

        sql(sql2).ok(expected2);

        final String sql3 =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' DAY\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, h, t m FROM source";
        final String expected3 =
                "CREATE MATERIALIZED TABLE `TBL1`\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' DAY\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`";
        sql(sql3).ok(expected3);
    }

    @Test
    void testCreateMaterializedTableWithUnsupportedFreshnessInterval() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = ^123^\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails(
                        "MATERIALIZED TABLE only supports define interval type FRESHNESS, please refer to the materialized table document.");

        final String sql2 =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "^AS^ SELECT a, b, h, t m FROM source";

        sql(sql2)
                .fails(
                        "Encountered \"AS\" at line 11, column 1.\n"
                                + "Was expecting:\n"
                                + "    \"FRESHNESS\" ...\n"
                                + "    ");
    }

    @Test
    void testCreateMaterializedTableWithoutAsQuery() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL^\n^";
        sql(sql).fails(
                        "Encountered \"<EOF>\" at line 12, column 20.\n"
                                + "Was expecting:\n"
                                + "    \"AS\" ...\n"
                                + "    ");
    }

    @Test
    void testCreateTemporaryMaterializedTable() {
        final String sql =
                "CREATE TEMPORARY ^MATERIALIZED^ TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails("CREATE TEMPORARY MATERIALIZED TABLE is not supported.");
    }

    @Test
    void testReplaceMaterializedTable() {
        final String sql =
                "CREATE OR REPLACE ^MATERIALIZED^ TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails("REPLACE MATERIALIZED TABLE is not supported.");
    }

    @Test
    void testAlterMaterializedTableSuspend() {
        final String sql = "ALTER MATERIALIZED TABLE tb1 SUSPEND";
        final String expect = "ALTER MATERIALIZED TABLE `TB1` SUSPEND";
        sql(sql).ok(expect);

        final String sql2 = "ALTER MATERIALIZED TABLE tb1 SUSPEND ^PARTITION^";
        sql(sql2)
                .fails(
                        "Encountered \"PARTITION\" at line 1, column 38.\n"
                                + "Was expecting:\n"
                                + "    <EOF> \n"
                                + "    ");

        final String sql3 = "ALTER MATERIALIZED TABLE tb^1^";
        sql(sql3)
                .fails(
                        "Encountered \"<EOF>\" at line 1, column 28.\n"
                                + "Was expecting one of:\n"
                                + "    \"RESET\" ...\n"
                                + "    \"SET\" ...\n"
                                + "    \"SUSPEND\" ...\n"
                                + "    \"REFRESH\" ...\n"
                                + "    \"RESUME\" ...\n"
                                + "    \".\" ...\n"
                                + "    ");
    }

    @Test
    void testAlterMaterializedTableResume() {
        final String sql1 =
                "ALTER MATERIALIZED TABLE tb1 RESUME\n"
                        + "WITH (\n"
                        + "  'group.id' = 'testGroup',\n"
                        + "  'topic' = 'test'\n"
                        + ")";
        final String expect1 =
                "ALTER MATERIALIZED TABLE `TB1` RESUME\n"
                        + "WITH (\n"
                        + "  'group.id' = 'testGroup',\n"
                        + "  'topic' = 'test'\n"
                        + ")";
        sql(sql1).ok(expect1);

        final String sql2 = "ALTER MATERIALIZED TABLE tb1 RESUME";
        final String expect2 = "ALTER MATERIALIZED TABLE `TB1` RESUME";
        sql(sql2).ok(expect2);

        final String sql3 = "ALTER MATERIALIZED TABLE tb1 RESUME ^PARTITION^";
        sql(sql3)
                .fails(
                        "Encountered \"PARTITION\" at line 1, column 37.\n"
                                + "Was expecting one of:\n"
                                + "    <EOF> \n"
                                + "    \"WITH\" ...\n"
                                + "    ");
    }

    @Test
    void testAlterMaterializedTableRefresh() {
        final String sql1 = "ALTER MATERIALIZED TABLE tbl1 REFRESH";
        final String expected1 = "ALTER MATERIALIZED TABLE `TBL1` REFRESH";
        sql(sql1).ok(expected1);

        final String sql2 =
                "ALTER MATERIALIZED TABLE tbl1 REFRESH \n"
                        + " PARTITION (part1 = 2023, part2 = 2024)";
        final String expected2 =
                "ALTER MATERIALIZED TABLE `TBL1` REFRESH "
                        + "PARTITION (`PART1` = 2023, `PART2` = 2024)";
        sql(sql2).ok(expected2);

        final String sql3 = "ALTER MATERIALIZED TABLE tbl1 REFRESH PARTITION(^)^";
        sql(sql3)
                .fails(
                        "Encountered \"\\)\" at line 1, column 49.\n"
                                + "Was expecting one of:\n"
                                + "    <BRACKET_QUOTED_IDENTIFIER> ...\n"
                                + "    <QUOTED_IDENTIFIER> ...\n"
                                + "    <BACK_QUOTED_IDENTIFIER> ...\n"
                                + "    <BIG_QUERY_BACK_QUOTED_IDENTIFIER> ...\n"
                                + "    <HYPHENATED_IDENTIFIER> ...\n"
                                + "    <IDENTIFIER> ...\n"
                                + "    <UNICODE_QUOTED_IDENTIFIER> ...\n"
                                + "    ");
    }

    @Test
    void testAlterMaterializedTableRefreshMode() {
        final String sql1 = "ALTER MATERIALIZED TABLE tbl1 SET REFRESH_MODE = FULL";
        final String expect1 = "ALTER MATERIALIZED TABLE `TBL1` SET REFRESH_MODE = FULL";
        sql(sql1).ok(expect1);

        final String sql2 = "ALTER MATERIALIZED TABLE tbl1 SET REFRESH_MODE = CONTINUOUS";
        final String expect2 = "ALTER MATERIALIZED TABLE `TBL1` SET REFRESH_MODE = CONTINUOUS";
        sql(sql2).ok(expect2);

        final String sql3 = "ALTER MATERIALIZED TABLE tbl1 SET REFRESH_MOD^E^";
        sql(sql3)
                .fails(
                        "Encountered \"<EOF>\" at line 1, column 46.\n"
                                + "Was expecting:\n"
                                + "    \"=\" ...\n"
                                + "    ");

        final String sql4 = "ALTER MATERIALIZED TABLE tbl1 SET REFRESH_MODE = ^NONE^";
        sql(sql4)
                .fails(
                        "Encountered \"NONE\" at line 1, column 50.\n"
                                + "Was expecting one of:\n"
                                + "    \"FULL\" ...\n"
                                + "    \"CONTINUOUS\" ...\n"
                                + "    ");
    }

    @Test
    void testAlterMaterializedTableFreshness() {
        final String sql1 = "ALTER MATERIALIZED TABLE tbl1 SET FRESHNESS = INTERVAL '1' DAY";
        final String expect1 = "ALTER MATERIALIZED TABLE `TBL1` SET FRESHNESS = INTERVAL '1' DAY";
        sql(sql1).ok(expect1);

        final String sql2 = "ALTER MATERIALIZED TABLE tbl1 SET FRESHNESS = INTERVAL 1 ^DAY^";
        sql(sql2)
                .fails(
                        "MATERIALIZED TABLE only supports define interval type FRESHNESS, please refer to the materialized table document.");

        final String sql3 = "ALTER MATERIALIZED TABLE tbl1 SET FRESHNES^S^";
        sql(sql3)
                .fails(
                        "Encountered \"<EOF>\" at line 1, column 43.\n"
                                + "Was expecting:\n"
                                + "    \"=\" ...\n"
                                + "    ");
    }

    @Test
    void testAlterMaterializedTableSet() {
        final String sql1 =
                "ALTER MATERIALIZED TABLE tbl1 SET (\n"
                        + "  'key1' = 'val1',\n"
                        + "  'key2' = 'val2'\n"
                        + ")";
        final String expect1 =
                "ALTER MATERIALIZED TABLE `TBL1` SET (\n"
                        + "  'key1' = 'val1',\n"
                        + "  'key2' = 'val2'\n"
                        + ")";
        sql(sql1).ok(expect1);

        final String sql2 = "ALTER MATERIALIZED TABLE tbl1 SET ()";
        final String expect2 = "ALTER MATERIALIZED TABLE `TBL1` SET (\n" + ")";

        sql(sql2).ok(expect2);

        final String sql3 = "ALTER MATERIALIZED TABLE tbl1 SE^T^";
        sql(sql3)
                .fails(
                        "Encountered \"<EOF>\" at line 1, column 33.\n"
                                + "Was expecting one of:\n"
                                + "    \"FRESHNESS\" ...\n"
                                + "    \"REFRESH_MODE\" ...\n"
                                + "    \"\\(\" ...\n"
                                + "    ");
    }

    @Test
    void testAlterMaterializedTableReset() {
        final String sql1 = "ALTER MATERIALIZED TABLE tbl1 RESET ('key1', 'key2')";
        final String expect1 =
                "ALTER MATERIALIZED TABLE `TBL1` RESET (\n" + "  'key1',\n" + "  'key2'\n" + ")";
        sql(sql1).ok(expect1);

        final String sql2 = "ALTER MATERIALIZED TABLE tbl1 RESET ()";
        final String expect2 = "ALTER MATERIALIZED TABLE `TBL1` RESET (\n" + ")";
        sql(sql2).ok(expect2);

        final String sql3 = "ALTER MATERIALIZED TABLE tbl1 RESE^T^";
        sql(sql3)
                .fails(
                        "Encountered \"<EOF>\" at line 1, column 35.\n"
                                + "Was expecting:\n"
                                + "    \"\\(\" ...\n"
                                + "    ");
    }

    @Test
    void testDropMaterializedTable() {
        final String sql = "DROP MATERIALIZED TABLE tbl1";
        final String expected = "DROP MATERIALIZED TABLE `TBL1`";
        sql(sql).ok(expected);

        final String sql2 = "DROP MATERIALIZED TABLE IF EXISTS tbl1";
        sql(sql2).ok("DROP MATERIALIZED TABLE IF EXISTS `TBL1`");

        final String sql3 = "DROP MATERIALIZED TABLE tb1 ^IF^ EXISTS";
        sql(sql3)
                .fails(
                        "Encountered \"IF\" at line 1, column 29.\n"
                                + "Was expecting one of:\n"
                                + "    <EOF> \n"
                                + "    \".\" ...\n"
                                + "    ");
    }

    @Test
    void testDropTemporaryMaterializedTable() {
        final String sql = "DROP TEMPORARY ^MATERIALIZED^ TABLE tbl1";
        sql(sql).fails("DROP TEMPORARY MATERIALIZED TABLE is not supported.");
    }

    public SqlParserFixture fixture() {
        return SqlParserFixture.DEFAULT.withConfig(
                c -> c.withParserFactory(FlinkSqlParserImpl.FACTORY));
    }

    protected SqlParserFixture sql(String sql) {
        return this.fixture().sql(sql);
    }

    protected SqlParserFixture expr(String sql) {
        return this.sql(sql).expression(true);
    }
}
