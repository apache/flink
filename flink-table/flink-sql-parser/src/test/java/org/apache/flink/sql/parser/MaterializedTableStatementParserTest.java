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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Sql parser test for materialized related syntax. * */
@Execution(CONCURRENT)
class MaterializedTableStatementParserTest {

    private static final String CREATE_OPERATION = "CREATE ";
    private static final String CREATE_OR_ALTER_OPERATION = "CREATE OR ALTER ";

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("inputForMaterializedTable")
    void testMaterializedTable(
            final String testName, final Map.Entry<String, String> sqlToExpected) {
        final String sql = sqlToExpected.getKey();
        final String expected = sqlToExpected.getValue();
        sql(sql).ok(expected);
    }

    private static Stream<Arguments> inputForMaterializedTable() {
        return Stream.concat(
                inputForCreateMaterializedTable(), inputForCreateOrAlterMaterializedTable());
    }

    @Test
    void testCreateMaterializedTableWithWrongSchema() {
        final String sql =
                "CREATE MATERIALIZED TABLE tbl1\n"
                        + "(a, b ^STRING^)\n"
                        + "AS SELECT a, b, h, t m FROM source";
        sql(sql).fails("(?s).*Encountered \"STRING\" at line 2, column 7.*");
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
                                + "    \"ADD\" ...\n"
                                + "    \"AS\" ...\n"
                                + "    \"DROP\" ...\n"
                                + "    \"RESET\" ...\n"
                                + "    \"SET\" ...\n"
                                + "    \"MODIFY\" ...\n"
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
    void testAlterMaterializedTableAsQuery() {
        final String sql = "ALTER MATERIALIZED TABLE tbl1 AS SELECT * FROM t";
        final String expected = "ALTER MATERIALIZED TABLE `TBL1`\nAS\nSELECT *\nFROM `T`";
        sql(sql).ok(expected);

        final String sql2 = "ALTER MATERIALIZED TABLE tbl1 AS SELECT * FROM t A^S^";
        sql(sql2)
                .fails(
                        "Encountered \"<EOF>\" at line 1, column 51.\n"
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
    void testAlterMaterializedTableAddSchema() {
        sql("alter materialized table mt1 add constraint ct1 primary key(a, b)")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`)\n"
                                + ")")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                                + "ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));
        sql("alter materialized table mt1 add constraint ct1 primary key(a, b) not enforced")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                                + ")");
        sql("alter materialized table mt1 " + "add unique(a, b)")
                .ok("ALTER MATERIALIZED TABLE `MT1` ADD (\n" + "  UNIQUE (`A`, `B`)\n" + ")")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testAddNestedColumn() {
        // add a row column
        sql("alter materialized table mt1 add new_column array<row(f0 int, f1 bigint)> comment 'new_column docs'")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_COLUMN` ARRAY< ROW(`F0` INTEGER, `F1` BIGINT) > COMMENT 'new_column docs'\n"
                                + ")");

        sql("alter materialized table mt1 add (new_row row(f0 int, f1 bigint) comment 'new_column docs', f2 as new_row.f0 + 1)")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_ROW` ROW(`F0` INTEGER, `F1` BIGINT) COMMENT 'new_column docs',\n"
                                + "  `F2` AS (`NEW_ROW`.`F0` + 1)\n"
                                + ")");

        // add a field to the row
        sql("alter materialized table mt1 add (new_row.f2 array<int>)")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_ROW`.`F2` ARRAY< INTEGER >\n"
                                + ")");

        // add a field to the row with after
        sql("alter materialized table mt1 add (new_row.f2 array<int> after new_row.f0)")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_ROW`.`F2` ARRAY< INTEGER > AFTER `NEW_ROW`.`F0`\n"
                                + ")");
    }

    @Test
    void testAddSingleColumn() {
        sql("alter materialized table mt1 add new_column int not null")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_COLUMN` INTEGER NOT NULL\n"
                                + ")");
        sql("alter materialized table mt1 add new_column string comment 'new_column docs'")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs'\n"
                                + ")");
        sql("alter materialized table mt1 add new_column string comment 'new_column docs' first")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' FIRST\n"
                                + ")");
        sql("alter materialized table mt1 add new_column string comment 'new_column docs' after id")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' AFTER `ID`\n"
                                + ")");
        // add compute column
        sql("alter materialized table mt1 add col_int as col_a - col_b after col_b")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `COL_INT` AS (`COL_A` - `COL_B`) AFTER `COL_B`\n"
                                + ")");
        // add metadata column
        sql("alter materialized table mt1 add col_int int metadata from 'mk1' virtual comment 'comment_metadata' after col_b")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  `COL_INT` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_metadata' AFTER `COL_B`\n"
                                + ")");
    }

    @Test
    void testAddWatermark() {
        sql("alter materialized table mt1 add watermark for ts as ts")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS `TS`\n"
                                + ")");
        sql("alter materialized table mt1 add watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter materialized table default_database.mt1 add watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER MATERIALIZED TABLE `DEFAULT_DATABASE`.`MT1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter materialized table default_catalog.default_database.mt1 add watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER MATERIALIZED TABLE `DEFAULT_CATALOG`.`DEFAULT_DATABASE`.`MT1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");

        sql("alter materialized table default_catalog.default_database.mt1 add (\n"
                        + "watermark for ts as ts - interval '1' second,\n"
                        + "^watermark^ for f1 as now()\n"
                        + ")")
                .fails("Multiple WATERMARK declarations are not supported yet.");
    }

    @Test
    void testModifySingleColumn() {
        sql("alter materialized table mt1 modify new_column string comment 'new_column docs'")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs'\n"
                                + ")");
        sql("alter materialized table mt1 modify new_column string comment 'new_column docs'")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs'\n"
                                + ")");
        sql("alter materialized table mt1 modify new_column string comment 'new_column docs' first")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' FIRST\n"
                                + ")");
        sql("alter materialized table mt1 modify new_column string comment 'new_column docs' after id")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' AFTER `ID`\n"
                                + ")");
        // modify column type
        sql("alter materialized table mt1 modify new_column array<string not null> not null")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `NEW_COLUMN` ARRAY< STRING NOT NULL > NOT NULL\n"
                                + ")");

        // modify compute column
        sql("alter materialized table mt1 modify col_int as col_a - col_b after col_b")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `COL_INT` AS (`COL_A` - `COL_B`) AFTER `COL_B`\n"
                                + ")");
        // modify metadata column
        sql("alter materialized table mt1 modify col_int int metadata from 'mk1' virtual comment 'comment_metadata' after col_b")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `COL_INT` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_metadata' AFTER `COL_B`\n"
                                + ")");

        // modify nested column
        sql("alter materialized table mt1 modify row_column.f0 int not null comment 'change nullability'")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `ROW_COLUMN`.`F0` INTEGER NOT NULL COMMENT 'change nullability'\n"
                                + ")");

        // modify nested column, shift position
        sql("alter materialized table mt1 modify row_column.f0 int after row_column.f2")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  `ROW_COLUMN`.`F0` INTEGER AFTER `ROW_COLUMN`.`F2`\n"
                                + ")");
    }

    @Test
    void testModifyWatermark() {
        sql("alter materialized table mt1 modify watermark for ts as ts")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS `TS`\n"
                                + ")");
        sql("alter materialized table mt1 modify watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter  materialized table default_database.mt1 modify watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER MATERIALIZED TABLE `DEFAULT_DATABASE`.`MT1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter materialized table default_catalog.default_database.mt1 modify watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER MATERIALIZED TABLE `DEFAULT_CATALOG`.`DEFAULT_DATABASE`.`MT1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");

        sql("alter materialized table default_catalog.default_database.mt1 modify (\n"
                        + "watermark for ts as ts - interval '1' second,\n"
                        + "^watermark^ for f1 as now()\n"
                        + ")")
                .fails("Multiple WATERMARK declarations are not supported yet.");
    }

    @Test
    void testModifyConstraint() {
        sql("alter materialized table mt1 modify constraint ct1 primary key(a, b) not enforced")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                                + ")");
        sql("alter materialized table mt1 modify unique(a, b)")
                .ok("ALTER MATERIALIZED TABLE `MT1` MODIFY (\n" + "  UNIQUE (`A`, `B`)\n" + ")");
    }

    @Test
    void testModifyMultipleColumns() {
        final String sql =
                "alter materialized table mt1 modify (\n"
                        + "col_int int,\n"
                        + "log_ts string comment 'log timestamp string' first,\n"
                        + "ts AS to_timestamp(log_ts) after log_ts,\n"
                        + "col_meta int metadata from 'mk1' virtual comment 'comment_str' after col_b,\n"
                        + "primary key (id) not enforced,\n"
                        + "unique(a, b),\n"
                        + "watermark for ts as ts - interval '3' second\n"
                        + ")";
        final String expected =
                "ALTER MATERIALIZED TABLE `MT1` MODIFY (\n"
                        + "  `COL_INT` INTEGER,\n"
                        + "  `LOG_TS` STRING COMMENT 'log timestamp string' FIRST,\n"
                        + "  `TS` AS `TO_TIMESTAMP`(`LOG_TS`) AFTER `LOG_TS`,\n"
                        + "  `COL_META` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_str' AFTER `COL_B`,\n"
                        + "  PRIMARY KEY (`ID`) NOT ENFORCED,\n"
                        + "  UNIQUE (`A`, `B`),\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testAddDistribution() {
        sql("alter materialized table mt1 add distribution by hash(a) into 6 buckets")
                .ok("ALTER MATERIALIZED TABLE `MT1` ADD DISTRIBUTION BY HASH(`A`) INTO 6 BUCKETS");

        sql("alter materialized table mt1 add distribution by hash(a, h) into 6 buckets")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD DISTRIBUTION BY HASH(`A`, `H`) INTO 6 BUCKETS");

        sql("alter materialized table mt1 add distribution by range(a, h) into 6 buckets")
                .ok(
                        "ALTER MATERIALIZED TABLE `MT1` ADD DISTRIBUTION BY RANGE(`A`, `H`) INTO 6 BUCKETS");

        sql("alter materialized table mt1 add distribution by ^RANDOM^(a, h) into 6 buckets")
                .fails("(?s).*Encountered \"RANDOM\" at line 1, column.*");

        sql("alter materialized table mt1 add distribution by (a, h) into 6 buckets")
                .ok("ALTER MATERIALIZED TABLE `MT1` ADD DISTRIBUTION BY (`A`, `H`) INTO 6 BUCKETS");

        sql("alter materialized table mt1 add distribution by range(a, h)")
                .ok("ALTER MATERIALIZED TABLE `MT1` ADD DISTRIBUTION BY RANGE(`A`, `H`)");

        sql("alter materialized table mt1 add distribution by (a, h)")
                .ok("ALTER MATERIALIZED TABLE `MT1` ADD DISTRIBUTION BY (`A`, `H`)");
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

    private static Stream<Arguments> inputForCreateMaterializedTable() {
        return Stream.of(
                Arguments.of("Full example", fullExample(CREATE_OPERATION)),
                Arguments.of("With columns", withColumns(CREATE_OPERATION)),
                Arguments.of(
                        "With columns and watermarks", withColumnsAndWatermark(CREATE_OPERATION)),
                Arguments.of("Without table constraint", withoutTableConstraint(CREATE_OPERATION)),
                Arguments.of("With primary key", withPrimaryKey(CREATE_OPERATION)),
                Arguments.of("Without freshness", withoutFreshness(CREATE_OPERATION)),
                Arguments.of(
                        "With column identifiers only",
                        withColumnsIdentifiersOnly(CREATE_OPERATION)));
    }

    private static Stream<Arguments> inputForCreateOrAlterMaterializedTable() {
        return Stream.of(
                Arguments.of("Full example", fullExample(CREATE_OR_ALTER_OPERATION)),
                Arguments.of("With columns", withColumns(CREATE_OR_ALTER_OPERATION)),
                Arguments.of(
                        "With columns and watermarks",
                        withColumnsAndWatermark(CREATE_OR_ALTER_OPERATION)),
                Arguments.of(
                        "Without table constraint",
                        withoutTableConstraint(CREATE_OR_ALTER_OPERATION)),
                Arguments.of("With primary key", withPrimaryKey(CREATE_OR_ALTER_OPERATION)),
                Arguments.of("Without freshness", withoutFreshness(CREATE_OR_ALTER_OPERATION)),
                Arguments.of(
                        "With column identifiers only",
                        withColumnsIdentifiersOnly(CREATE_OR_ALTER_OPERATION)));
    }

    private static Map.Entry<String, String> fullExample(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "  ts timestamp(3),\n"
                        + "  id varchar,\n"
                        + "  watermark FOR ts AS ts - interval '3' second,\n"
                        + "  PRIMARY KEY (id)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH (a) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTES\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1` (\n"
                        + "  `TS` TIMESTAMP(3),\n"
                        + "  `ID` VARCHAR,\n"
                        + "  PRIMARY KEY (`ID`),\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH(`A`) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }

    private static Map.Entry<String, String> withPrimaryKey(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1` (\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }

    private static Map.Entry<String, String> withoutTableConstraint(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' DAYS\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1`\n"
                        + "COMMENT 'table comment'\n"
                        + "FRESHNESS = INTERVAL '3' DAY\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }

    private static Map.Entry<String, String> withoutFreshness(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "   PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1` (\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }

    private static Map.Entry<String, String> withColumns(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "  a INT, b STRING, h INT, m INT\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH (a) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` STRING,\n"
                        + "  `H` INTEGER,\n"
                        + "  `M` INTEGER\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH(`A`) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }

    private static Map.Entry<String, String> withColumnsAndWatermark(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "(\n"
                        + "  ts timestamp(3),\n"
                        + "  id varchar,\n"
                        + "  watermark FOR ts AS ts - interval '3' second\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH (a) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1` (\n"
                        + "  `TS` TIMESTAMP(3),\n"
                        + "  `ID` VARCHAR,\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH(`A`) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }

    private static Map.Entry<String, String> withColumnsIdentifiersOnly(final String operation) {
        return new AbstractMap.SimpleEntry<>(
                operation
                        + "MATERIALIZED TABLE tbl1\n"
                        + "(a, b, h, m)\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH (a) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest', \n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS SELECT a, b, h, t m FROM source",
                operation
                        + "MATERIALIZED TABLE `TBL1` (\n"
                        + "  `A`,\n"
                        + "  `B`,\n"
                        + "  `H`,\n"
                        + "  `M`\n"
                        + ")\n"
                        + "COMMENT 'table comment'\n"
                        + "DISTRIBUTED BY HASH(`A`) INTO 4 BUCKETS\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'group.id' = 'latest',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '3' MINUTE\n"
                        + "AS\n"
                        + "SELECT `A`, `B`, `H`, `T` AS `M`\n"
                        + "FROM `SOURCE`");
    }
}
