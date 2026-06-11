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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** ALTER TABLE parser tests. */
@Execution(CONCURRENT)
class FlinkSqlParserAlterTableTest extends FlinkSqlParserTestBase {

    @Test
    void testAlterTable() {
        sql("alter table t1 rename to t2").ok("ALTER TABLE `T1` RENAME TO `T2`");
        sql("alter table if exists t1 rename to t2")
                .ok("ALTER TABLE IF EXISTS `T1` RENAME TO `T2`");
        sql("alter table c1.d1.t1 rename to t2").ok("ALTER TABLE `C1`.`D1`.`T1` RENAME TO `T2`");
        sql("alter table if exists c1.d1.t1 rename to t2")
                .ok("ALTER TABLE IF EXISTS `C1`.`D1`.`T1` RENAME TO `T2`");

        sql("alter table t1 set ('key1'='value1')")
                .ok("ALTER TABLE `T1` SET (\n" + "  'key1' = 'value1'\n" + ")");
        sql("alter table if exists t1 set ('key1'='value1')")
                .ok("ALTER TABLE IF EXISTS `T1` SET (\n" + "  'key1' = 'value1'\n" + ")");

        sql("alter table t1 add constraint ct1 primary key(a, b)")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`)\n"
                                + ")")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                                + "ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));
        sql("alter table t1 add constraint ct1 primary key(a, b) not enforced")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                                + ")");
        sql("alter table if exists t1 add constraint ct1 primary key(a, b) not enforced")
                .ok(
                        "ALTER TABLE IF EXISTS `T1` ADD (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                                + ")");
        sql("alter table t1 " + "add unique(a, b)")
                .ok("ALTER TABLE `T1` ADD (\n" + "  UNIQUE (`A`, `B`)\n" + ")")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
        sql("alter table if exists t1 " + "add unique(a, b)")
                .ok("ALTER TABLE IF EXISTS `T1` ADD (\n" + "  UNIQUE (`A`, `B`)\n" + ")");

        sql("alter table t1 drop constraint ct1").ok("ALTER TABLE `T1` DROP CONSTRAINT `CT1`");
        sql("alter table if exists t1 drop constraint ct1")
                .ok("ALTER TABLE IF EXISTS `T1` DROP CONSTRAINT `CT1`");

        sql("alter table t1 rename a to b").ok("ALTER TABLE `T1` RENAME `A` TO `B`");
        sql("alter table if exists t1 rename a to b")
                .ok("ALTER TABLE IF EXISTS `T1` RENAME `A` TO `B`");
        sql("alter table if exists t1 rename a.x to a.y")
                .ok("ALTER TABLE IF EXISTS `T1` RENAME `A`.`X` TO `A`.`Y`");
    }

    @Test
    void testAlterTableAddNestedColumn() {
        // add a row column
        sql("alter table t1 add new_column array<row(f0 int, f1 bigint)> comment 'new_column docs'")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `NEW_COLUMN` ARRAY< ROW(`F0` INTEGER, `F1` BIGINT) > COMMENT 'new_column docs'\n"
                                + ")");

        sql("alter table t1 add (new_row row(f0 int, f1 bigint) comment 'new_column docs', f2 as new_row.f0 + 1)")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `NEW_ROW` ROW(`F0` INTEGER, `F1` BIGINT) COMMENT 'new_column docs',\n"
                                + "  `F2` AS (`NEW_ROW`.`F0` + 1)\n"
                                + ")");

        // add a field to the row
        sql("alter table t1 add (new_row.f2 array<int>)")
                .ok("ALTER TABLE `T1` ADD (\n" + "  `NEW_ROW`.`F2` ARRAY< INTEGER >\n" + ")");

        // add a field to the row with after
        sql("alter table t1 add (new_row.f2 array<int> after new_row.f0)")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `NEW_ROW`.`F2` ARRAY< INTEGER > AFTER `NEW_ROW`.`F0`\n"
                                + ")");
    }

    @Test
    void testAlterTableAddSingleColumn() {
        sql("alter table if exists t1 add new_column int not null")
                .ok(
                        "ALTER TABLE IF EXISTS `T1` ADD (\n"
                                + "  `NEW_COLUMN` INTEGER NOT NULL\n"
                                + ")");
        sql("alter table t1 add new_column string comment 'new_column docs'")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs'\n"
                                + ")");
        sql("alter table t1 add new_column string comment 'new_column docs' first")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' FIRST\n"
                                + ")");
        sql("alter table t1 add new_column string comment 'new_column docs' after id")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' AFTER `ID`\n"
                                + ")");
        // add compute column
        sql("alter table t1 add col_int as col_a - col_b after col_b")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `COL_INT` AS (`COL_A` - `COL_B`) AFTER `COL_B`\n"
                                + ")");
        // add metadata column
        sql("alter table t1 add col_int int metadata from 'mk1' virtual comment 'comment_metadata' after col_b")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  `COL_INT` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_metadata' AFTER `COL_B`\n"
                                + ")");
    }

    @Test
    void testAlterTableAddWatermark() {
        sql("alter table if exists t1 add watermark for ts as ts")
                .ok("ALTER TABLE IF EXISTS `T1` ADD (\n" + "  WATERMARK FOR `TS` AS `TS`\n" + ")");
        sql("alter table t1 add watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER TABLE `T1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter table default_database.t1 add watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER TABLE `DEFAULT_DATABASE`.`T1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter table default_catalog.default_database.t1 add watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER TABLE `DEFAULT_CATALOG`.`DEFAULT_DATABASE`.`T1` ADD (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");

        sql("alter table default_catalog.default_database.t1 add (\n"
                        + "watermark for ts as ts - interval '1' second,\n"
                        + "^watermark^ for f1 as now()\n"
                        + ")")
                .fails("Multiple WATERMARK declarations are not supported yet.");
    }

    @Test
    void testAlterTableAddDistribution() {
        sql("alter table t1 add DISTRIBUTION BY HASH(a) INTO 6 BUCKETS")
                .ok("ALTER TABLE `T1` ADD DISTRIBUTION BY HASH(`A`) INTO 6 BUCKETS");

        sql("alter table t1 add DISTRIBUTION BY HASH(a, h) INTO 6 BUCKETS")
                .ok("ALTER TABLE `T1` ADD DISTRIBUTION BY HASH(`A`, `H`) INTO 6 BUCKETS");

        sql("alter table tbl1 add DISTRIBUTION BY RANGE(a, h) INTO 6 BUCKETS")
                .ok("ALTER TABLE `TBL1` ADD DISTRIBUTION BY RANGE(`A`, `H`) INTO 6 BUCKETS");

        sql("alter table tbl1 add DISTRIBUTION BY ^RANDOM^(a, h) INTO 6 BUCKETS")
                .fails("(?s).*Encountered \"RANDOM\" at line 1, column 38.*");

        sql("alter table tbl1 add DISTRIBUTION BY (a, h) INTO 6 BUCKETS")
                .ok("ALTER TABLE `TBL1` ADD DISTRIBUTION BY (`A`, `H`) INTO 6 BUCKETS");

        sql("alter table tbl1 add DISTRIBUTION BY RANGE(a, h)")
                .ok("ALTER TABLE `TBL1` ADD DISTRIBUTION BY RANGE(`A`, `H`)");

        sql("alter table tbl1 add DISTRIBUTION BY (a, h)")
                .ok("ALTER TABLE `TBL1` ADD DISTRIBUTION BY (`A`, `H`)");
    }

    @Test
    void testAlterTableModifyDistribution() {
        sql("alter table t1 modify DISTRIBUTION BY HASH(a) INTO 6 BUCKETS")
                .ok("ALTER TABLE `T1` MODIFY DISTRIBUTION BY HASH(`A`) INTO 6 BUCKETS");

        sql("alter table tbl1 modify DISTRIBUTION BY HASH(a, h) INTO 6 BUCKETS")
                .ok("ALTER TABLE `TBL1` MODIFY DISTRIBUTION BY HASH(`A`, `H`) INTO 6 BUCKETS");

        sql("alter table tbl1 modify DISTRIBUTION BY RANGE(a, h) INTO 6 BUCKETS")
                .ok("ALTER TABLE `TBL1` MODIFY DISTRIBUTION BY RANGE(`A`, `H`) INTO 6 BUCKETS");

        sql("alter table tbl1 modify DISTRIBUTION BY ^RANDOM^(a, h) INTO 6 BUCKETS")
                .fails("(?s).*Encountered \"RANDOM\" at line 1, column 41.*");

        sql("alter table tbl1 modify DISTRIBUTION BY (a, h) INTO 6 BUCKETS")
                .ok("ALTER TABLE `TBL1` MODIFY DISTRIBUTION BY (`A`, `H`) INTO 6 BUCKETS");

        sql("alter table tbl1 modify DISTRIBUTION BY RANGE(a, h)")
                .ok("ALTER TABLE `TBL1` MODIFY DISTRIBUTION BY RANGE(`A`, `H`)");

        sql("alter table tbl1 modify DISTRIBUTION BY (a, h)")
                .ok("ALTER TABLE `TBL1` MODIFY DISTRIBUTION BY (`A`, `H`)");
    }

    @Test
    void testAlterTableDropDistribution() {
        sql("alter table t1 drop DISTRIBUTION").ok("ALTER TABLE `T1` DROP DISTRIBUTION");
    }

    @Test
    void testAlterTableAddMultipleColumn() {
        final String sql1 =
                "alter table t1 add (\n"
                        + "col_int int,\n"
                        + "log_ts string comment 'log timestamp string' first,\n"
                        + "ts AS to_timestamp(log_ts) after log_ts,\n"
                        + "col_meta int metadata from 'mk1' virtual comment 'comment_str' after col_b,\n"
                        + "primary key (id) not enforced,\n"
                        + "unique(a, b),\n"
                        + "watermark for ts as ts - interval '3' second\n"
                        + ")";
        final String expected1 =
                "ALTER TABLE `T1` ADD (\n"
                        + "  `COL_INT` INTEGER,\n"
                        + "  `LOG_TS` STRING COMMENT 'log timestamp string' FIRST,\n"
                        + "  `TS` AS `TO_TIMESTAMP`(`LOG_TS`) AFTER `LOG_TS`,\n"
                        + "  `COL_META` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_str' AFTER `COL_B`,\n"
                        + "  PRIMARY KEY (`ID`) NOT ENFORCED,\n"
                        + "  UNIQUE (`A`, `B`),\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")";
        sql(sql1).ok(expected1);

        final String sql2 =
                "alter table t1 add (\n"
                        + "col_int int primary key not enforced,\n"
                        + "log_ts string comment 'log timestamp string' first,\n"
                        + "ts AS to_timestamp(log_ts) after log_ts,\n"
                        + "col_meta int metadata from 'mk1' virtual comment 'comment_str' after col_b,\n"
                        + "primary key (id) not enforced,\n"
                        + "unique (a, b),\n"
                        + "watermark for ts as ts - interval '3' second\n"
                        + ")";
        sql(sql2).node(new ValidationMatcher().fails("Duplicate primary key definition"));
    }

    @Test
    public void testAlterTableModifySingleColumn() {
        sql("alter table if exists t1 modify new_column string comment 'new_column docs'")
                .ok(
                        "ALTER TABLE IF EXISTS `T1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs'\n"
                                + ")");
        sql("alter table t1 modify new_column string comment 'new_column docs'")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs'\n"
                                + ")");
        sql("alter table t1 modify new_column string comment 'new_column docs' first")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' FIRST\n"
                                + ")");
        sql("alter table t1 modify new_column string comment 'new_column docs' after id")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `NEW_COLUMN` STRING COMMENT 'new_column docs' AFTER `ID`\n"
                                + ")");
        // modify column type
        sql("alter table t1 modify new_column array<string not null> not null")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `NEW_COLUMN` ARRAY< STRING NOT NULL > NOT NULL\n"
                                + ")");

        // modify compute column
        sql("alter table t1 modify col_int as col_a - col_b after col_b")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `COL_INT` AS (`COL_A` - `COL_B`) AFTER `COL_B`\n"
                                + ")");
        // modify metadata column
        sql("alter table t1 modify col_int int metadata from 'mk1' virtual comment 'comment_metadata' after col_b")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `COL_INT` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_metadata' AFTER `COL_B`\n"
                                + ")");

        // modify nested column
        sql("alter table t1 modify row_column.f0 int not null comment 'change nullability'")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `ROW_COLUMN`.`F0` INTEGER NOT NULL COMMENT 'change nullability'\n"
                                + ")");

        // modify nested column, shift position
        sql("alter table t1 modify row_column.f0 int after row_column.f2")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  `ROW_COLUMN`.`F0` INTEGER AFTER `ROW_COLUMN`.`F2`\n"
                                + ")");
    }

    @Test
    void testAlterTableModifyWatermark() {
        sql("alter table if exists t1 modify watermark for ts as ts")
                .ok(
                        "ALTER TABLE IF EXISTS `T1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS `TS`\n"
                                + ")");
        sql("alter table t1 modify watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter table default_database.t1 modify watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER TABLE `DEFAULT_DATABASE`.`T1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");
        sql("alter table default_catalog.default_database.t1 modify watermark for ts as ts - interval '1' second")
                .ok(
                        "ALTER TABLE `DEFAULT_CATALOG`.`DEFAULT_DATABASE`.`T1` MODIFY (\n"
                                + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '1' SECOND)\n"
                                + ")");

        sql("alter table default_catalog.default_database.t1 modify (\n"
                        + "watermark for ts as ts - interval '1' second,\n"
                        + "^watermark^ for f1 as now()\n"
                        + ")")
                .fails("Multiple WATERMARK declarations are not supported yet.");
    }

    @Test
    void testAlterTableModifyConstraint() {
        sql("alter table t1 modify constraint ct1 primary key(a, b) not enforced")
                .ok(
                        "ALTER TABLE `T1` MODIFY (\n"
                                + "  CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                                + ")");
        sql("alter table t1 modify unique(a, b)")
                .ok("ALTER TABLE `T1` MODIFY (\n" + "  UNIQUE (`A`, `B`)\n" + ")");
    }

    @Test
    public void testAlterTableModifyMultipleColumn() {
        final String sql1 =
                "alter table t1 modify (\n"
                        + "col_int int,\n"
                        + "log_ts string comment 'log timestamp string' first,\n"
                        + "ts AS to_timestamp(log_ts) after log_ts,\n"
                        + "col_meta int metadata from 'mk1' virtual comment 'comment_str' after col_b,\n"
                        + "primary key (id) not enforced,\n"
                        + "unique(a, b),\n"
                        + "watermark for ts as ts - interval '3' second\n"
                        + ")";
        final String expected1 =
                "ALTER TABLE `T1` MODIFY (\n"
                        + "  `COL_INT` INTEGER,\n"
                        + "  `LOG_TS` STRING COMMENT 'log timestamp string' FIRST,\n"
                        + "  `TS` AS `TO_TIMESTAMP`(`LOG_TS`) AFTER `LOG_TS`,\n"
                        + "  `COL_META` INTEGER METADATA FROM 'mk1' VIRTUAL COMMENT 'comment_str' AFTER `COL_B`,\n"
                        + "  PRIMARY KEY (`ID`) NOT ENFORCED,\n"
                        + "  UNIQUE (`A`, `B`),\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ")";
        sql(sql1).ok(expected1);
    }

    @Test
    public void testAlterTableDropSingleColumn() {
        sql("alter table if exists t1 drop id")
                .ok("ALTER TABLE IF EXISTS `T1` DROP (\n" + "  `ID`\n" + ")");
        sql("alter table t1 drop id").ok("ALTER TABLE `T1` DROP (\n" + "  `ID`\n" + ")");

        sql("alter table t1 drop (id)").ok("ALTER TABLE `T1` DROP (\n" + "  `ID`\n" + ")");

        sql("alter table t1 drop tuple.id")
                .ok("ALTER TABLE `T1` DROP (\n" + "  `TUPLE`.`ID`\n" + ")");
    }

    @Test
    public void testAlterTableDropMultipleColumn() {
        sql("alter table if exists t1 drop (id, ts, tuple.f0, tuple.f1)")
                .ok(
                        "ALTER TABLE IF EXISTS `T1` DROP (\n"
                                + "  `ID`,\n"
                                + "  `TS`,\n"
                                + "  `TUPLE`.`F0`,\n"
                                + "  `TUPLE`.`F1`\n"
                                + ")");
        sql("alter table t1 drop (id, ts, tuple.f0, tuple.f1)")
                .ok(
                        "ALTER TABLE `T1` DROP (\n"
                                + "  `ID`,\n"
                                + "  `TS`,\n"
                                + "  `TUPLE`.`F0`,\n"
                                + "  `TUPLE`.`F1`\n"
                                + ")");
    }

    @Test
    public void testAlterTableDropPrimaryKey() {
        sql("alter table if exists t1 drop primary key")
                .ok("ALTER TABLE IF EXISTS `T1` DROP PRIMARY KEY");
        sql("alter table t1 drop primary key").ok("ALTER TABLE `T1` DROP PRIMARY KEY");
    }

    @Test
    public void testAlterTableDropConstraint() {
        sql("alter table if exists t1 drop constraint ct")
                .ok("ALTER TABLE IF EXISTS `T1` DROP CONSTRAINT `CT`");
        sql("alter table t1 drop constraint ct").ok("ALTER TABLE `T1` DROP CONSTRAINT `CT`");

        sql("alter table t1 drop constrain^t^")
                .fails("(?s).*Encountered \"<EOF>\" at line 1, column 30.\n.*");
    }

    @Test
    public void testAlterTableDropWatermark() {
        sql("alter table if exists t1 drop watermark")
                .ok("ALTER TABLE IF EXISTS `T1` DROP WATERMARK");
        sql("alter table t1 drop watermark").ok("ALTER TABLE `T1` DROP WATERMARK");
    }

    @Test
    void testAlterTableReset() {
        sql("alter table if exists t1 reset ('key1')")
                .ok("ALTER TABLE IF EXISTS `T1` RESET (\n  'key1'\n)");

        sql("alter table t1 reset ('key1')").ok("ALTER TABLE `T1` RESET (\n  'key1'\n)");

        sql("alter table t1 reset ('key1', 'key2')")
                .ok("ALTER TABLE `T1` RESET (\n  'key1',\n  'key2'\n)");

        sql("alter table t1 reset()").ok("ALTER TABLE `T1` RESET (\n)");
    }

    @Test
    public void testAddPartition() {
        sql("alter table c1.d1.tbl add partition (p1=1,p2='a')")
                .ok("ALTER TABLE `C1`.`D1`.`TBL`\n" + "ADD\n" + "PARTITION (`P1` = 1, `P2` = 'a')");

        sql("alter table tbl add partition (p1=1,p2='a') with ('k1'='v1')")
                .ok(
                        "ALTER TABLE `TBL`\n"
                                + "ADD\n"
                                + "PARTITION (`P1` = 1, `P2` = 'a') WITH ('k1' = 'v1')");

        sql("alter table tbl add if not exists partition (p=1) partition (p=2) with ('k1' = 'v1')")
                .ok(
                        "ALTER TABLE `TBL`\n"
                                + "ADD IF NOT EXISTS\n"
                                + "PARTITION (`P` = 1)\n"
                                + "PARTITION (`P` = 2) WITH ('k1' = 'v1')");
    }

    @Test
    public void testDropPartition() {
        sql("alter table c1.d1.tbl drop if exists partition (p=1)")
                .ok("ALTER TABLE `C1`.`D1`.`TBL`\n" + "DROP IF EXISTS\n" + "PARTITION (`P` = 1)");
        sql("alter table tbl drop partition (p1='a',p2=1), partition(p1='b',p2=2)")
                .ok(
                        "ALTER TABLE `TBL`\n"
                                + "DROP\n"
                                + "PARTITION (`P1` = 'a', `P2` = 1),\n"
                                + "PARTITION (`P1` = 'b', `P2` = 2)");
        sql("alter table tbl drop partition (p1='a',p2=1), "
                        + "partition(p1='b',p2=2), partition(p1='c',p2=3)")
                .ok(
                        "ALTER TABLE `TBL`\n"
                                + "DROP\n"
                                + "PARTITION (`P1` = 'a', `P2` = 1),\n"
                                + "PARTITION (`P1` = 'b', `P2` = 2),\n"
                                + "PARTITION (`P1` = 'c', `P2` = 3)");
    }
}
