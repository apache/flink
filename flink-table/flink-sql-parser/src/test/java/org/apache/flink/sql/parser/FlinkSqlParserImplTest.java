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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserFixture;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** FlinkSqlParserImpl tests. * */
@Execution(CONCURRENT)
class FlinkSqlParserImplTest extends SqlParserTest {

    public SqlParserFixture fixture() {
        return super.fixture().withConfig(c -> c.withParserFactory(FlinkSqlParserImpl.FACTORY));
    }

    @Test
    void testShowCatalogs() {
        sql("show catalogs").ok("SHOW CATALOGS");
    }

    @Test
    void testShowCurrentCatalog() {
        sql("show current catalog").ok("SHOW CURRENT CATALOG");
    }

    @Test
    void testDescribeCatalog() {
        sql("describe catalog a").ok("DESCRIBE CATALOG `A`");

        sql("desc catalog a").ok("DESCRIBE CATALOG `A`");
    }

    // ignore test methods that we don't support
    // BEGIN
    // ARRAY_AGG
    @Disabled
    @Test
    void testArrayAgg() {}

    // DESCRIBE SCHEMA
    @Disabled
    @Test
    void testDescribeSchema() {}

    // DESCRIBE STATEMENT
    @Disabled
    @Test
    void testDescribeStatement() {}

    // GROUP CONCAT
    @Disabled
    @Test
    void testGroupConcat() {}

    // EXPLAIN AS DOT
    @Disabled
    @Test
    void testExplainAsDot() {}

    // STRING_AGG
    @Disabled
    @Test
    void testStringAgg() {}
    // END

    @Test
    void testUseCatalog() {
        sql("use catalog a").ok("USE CATALOG `A`");
    }

    @Test
    void testCreateCatalog() {
        sql("create catalog c1\n"
                        + " WITH (\n"
                        + "  'key1'='value1',\n"
                        + "  'key2'='value2'\n"
                        + " )\n")
                .ok(
                        "CREATE CATALOG `C1` "
                                + "WITH (\n"
                                + "  'key1' = 'value1',\n"
                                + "  'key2' = 'value2'\n"
                                + ")");
    }

    @Test
    void testDropCatalog() {
        sql("drop catalog c1").ok("DROP CATALOG `C1`");
    }

    @Test
    void testShowDataBases() {
        sql("show databases").ok("SHOW DATABASES");
    }

    @Test
    void testShowCurrentDatabase() {
        sql("show current database").ok("SHOW CURRENT DATABASE");
    }

    @Test
    void testUseDataBase() {
        sql("use default_db").ok("USE `DEFAULT_DB`");
        sql("use defaultCatalog.default_db").ok("USE `DEFAULTCATALOG`.`DEFAULT_DB`");
    }

    @Test
    void testCreateDatabase() {
        sql("create database db1").ok("CREATE DATABASE `DB1`");
        sql("create database if not exists db1").ok("CREATE DATABASE IF NOT EXISTS `DB1`");
        sql("create database catalog1.db1").ok("CREATE DATABASE `CATALOG1`.`DB1`");
        final String sql = "create database db1 comment 'test create database'";
        final String expected = "CREATE DATABASE `DB1`\n" + "COMMENT 'test create database'";
        sql(sql).ok(expected);
        final String sql1 =
                "create database db1 comment 'test create database'"
                        + "with ( 'key1' = 'value1', 'key2.a' = 'value2.a')";
        final String expected1 =
                "CREATE DATABASE `DB1`\n"
                        + "COMMENT 'test create database' WITH (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2.a' = 'value2.a'\n"
                        + ")";
        sql(sql1).ok(expected1);
    }

    @Test
    void testDropDatabase() {
        sql("drop database db1").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database catalog1.db1").ok("DROP DATABASE `CATALOG1`.`DB1` RESTRICT");
        sql("drop database db1 RESTRICT").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database db1 CASCADE").ok("DROP DATABASE `DB1` CASCADE");
    }

    @Test
    void testAlterDatabase() {
        final String sql = "alter database db1 set ('key1' = 'value1','key2.a' = 'value2.a')";
        final String expected =
                "ALTER DATABASE `DB1` SET (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2.a' = 'value2.a'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testDescribeDatabase() {
        sql("describe database db1").ok("DESCRIBE DATABASE `DB1`");
        sql("describe database catalog1.db1").ok("DESCRIBE DATABASE `CATALOG1`.`DB1`");
        sql("describe database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");

        sql("desc database db1").ok("DESCRIBE DATABASE `DB1`");
        sql("desc database catalog1.db1").ok("DESCRIBE DATABASE `CATALOG1`.`DB1`");
        sql("desc database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");
    }

    @Test
    void testAlterFunction() {
        sql("alter function function1 as 'org.apache.flink.function.function1'")
                .ok("ALTER FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("alter temporary function function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("alter temporary function function1 as 'org.apache.flink.function.function1' language scala")
                .ok(
                        "ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA");

        sql("alter temporary system function function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("alter temporary system function function1 as 'org.apache.flink.function.function1' language java")
                .ok(
                        "ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA");
    }

    @Test
    void testShowFunctions() {
        sql("show functions").ok("SHOW FUNCTIONS");
        sql("show user functions").ok("SHOW USER FUNCTIONS");

        sql("show functions like '%'").ok("SHOW FUNCTIONS LIKE '%'");
        sql("show functions not like '%'").ok("SHOW FUNCTIONS NOT LIKE '%'");
        sql("show user functions like '%'").ok("SHOW USER FUNCTIONS LIKE '%'");
        sql("show user functions not like '%'").ok("SHOW USER FUNCTIONS NOT LIKE '%'");

        sql("show functions from db1").ok("SHOW FUNCTIONS FROM `DB1`");
        sql("show user functions from db1").ok("SHOW USER FUNCTIONS FROM `DB1`");
        sql("show functions in db1").ok("SHOW FUNCTIONS IN `DB1`");
        sql("show user functions in db1").ok("SHOW USER FUNCTIONS IN `DB1`");

        sql("show functions from catalog1.db1").ok("SHOW FUNCTIONS FROM `CATALOG1`.`DB1`");
        sql("show user functions from catalog1.db1")
                .ok("SHOW USER FUNCTIONS FROM `CATALOG1`.`DB1`");
        sql("show functions in catalog1.db1").ok("SHOW FUNCTIONS IN `CATALOG1`.`DB1`");
        sql("show user functions in catalog1.db1").ok("SHOW USER FUNCTIONS IN `CATALOG1`.`DB1`");

        sql("show functions from db1 like '%'").ok("SHOW FUNCTIONS FROM `DB1` LIKE '%'");
        sql("show user functions from db1 like '%'").ok("SHOW USER FUNCTIONS FROM `DB1` LIKE '%'");
        sql("show functions in db1 ilike '%'").ok("SHOW FUNCTIONS IN `DB1` ILIKE '%'");
        sql("show user functions in db1 ilike '%'").ok("SHOW USER FUNCTIONS IN `DB1` ILIKE '%'");

        sql("show functions from catalog1.db1 ilike '%'")
                .ok("SHOW FUNCTIONS FROM `CATALOG1`.`DB1` ILIKE '%'");
        sql("show user functions from catalog1.db1 ilike '%'")
                .ok("SHOW USER FUNCTIONS FROM `CATALOG1`.`DB1` ILIKE '%'");
        sql("show functions in catalog1.db1 like '%'")
                .ok("SHOW FUNCTIONS IN `CATALOG1`.`DB1` LIKE '%'");
        sql("show user functions in catalog1.db1 like '%'")
                .ok("SHOW USER FUNCTIONS IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show functions from db1 not like '%'").ok("SHOW FUNCTIONS FROM `DB1` NOT LIKE '%'");
        sql("show user functions from db1 not like '%'")
                .ok("SHOW USER FUNCTIONS FROM `DB1` NOT LIKE '%'");
        sql("show functions in db1 not ilike '%'").ok("SHOW FUNCTIONS IN `DB1` NOT ILIKE '%'");
        sql("show user functions in db1 not ilike '%'")
                .ok("SHOW USER FUNCTIONS IN `DB1` NOT ILIKE '%'");

        sql("show functions from catalog1.db1 not like '%'")
                .ok("SHOW FUNCTIONS FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show user functions from catalog1.db1 not like '%'")
                .ok("SHOW USER FUNCTIONS FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show functions in catalog1.db1 not ilike '%'")
                .ok("SHOW FUNCTIONS IN `CATALOG1`.`DB1` NOT ILIKE '%'");
        sql("show user functions in catalog1.db1 not ilike '%'")
                .ok("SHOW USER FUNCTIONS IN `CATALOG1`.`DB1` NOT ILIKE '%'");

        sql("show functions ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 16.\n.*");
        sql("show functions not ^likes^")
                .fails("(?s).*Encountered \"likes\" at line 1, column 20" + ".\n" + ".*");
        sql("show functions ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 16.\n.*");
        sql("show functions not ^ilikes^")
                .fails("(?s).*Encountered \"ilikes\" at line 1, column 20" + ".\n" + ".*");
    }

    @Test
    void testShowProcedures() {
        sql("show procedures").ok("SHOW PROCEDURES");
        sql("show procedures not like '%'").ok("SHOW PROCEDURES NOT LIKE '%'");

        sql("show procedures from db1").ok("SHOW PROCEDURES FROM `DB1`");
        sql("show procedures in db1").ok("SHOW PROCEDURES IN `DB1`");

        sql("show procedures from catalog1.db1").ok("SHOW PROCEDURES FROM `CATALOG1`.`DB1`");
        sql("show procedures in catalog1.db1").ok("SHOW PROCEDURES IN `CATALOG1`.`DB1`");

        sql("show procedures from db1 like '%'").ok("SHOW PROCEDURES FROM `DB1` LIKE '%'");
        sql("show procedures in db1 ilike '%'").ok("SHOW PROCEDURES IN `DB1` ILIKE '%'");

        sql("show procedures from catalog1.db1 Ilike '%'")
                .ok("SHOW PROCEDURES FROM `CATALOG1`.`DB1` ILIKE '%'");
        sql("show procedures in catalog1.db1 like '%'")
                .ok("SHOW PROCEDURES IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show procedures from db1 not like '%'").ok("SHOW PROCEDURES FROM `DB1` NOT LIKE '%'");
        sql("show procedures in db1 not ilike '%'").ok("SHOW PROCEDURES IN `DB1` NOT ILIKE '%'");

        sql("show procedures from catalog1.db1 not like '%'")
                .ok("SHOW PROCEDURES FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show procedures in catalog1.db1 not ilike '%'")
                .ok("SHOW PROCEDURES IN `CATALOG1`.`DB1` NOT ILIKE '%'");

        sql("show procedures ^db1^").fails("(?s).*Encountered \"db1\" at line 1, column 17.\n.*");
        sql("show procedures ^catalog1^.db1")
                .fails("(?s).*Encountered \"catalog1\" at line 1, column 17.\n.*");

        sql("show procedures ^search^ db1")
                .fails("(?s).*Encountered \"search\" at line 1, column 17.\n.*");

        sql("show procedures from db1 ^likes^ '%t'")
                .fails("(?s).*Encountered \"likes\" at line 1, column 26.\n.*");
    }

    @Test
    void testShowTables() {
        sql("show tables").ok("SHOW TABLES");
        sql("show tables not like '%'").ok("SHOW TABLES NOT LIKE '%'");

        sql("show tables from db1").ok("SHOW TABLES FROM `DB1`");
        sql("show tables in db1").ok("SHOW TABLES IN `DB1`");

        sql("show tables from catalog1.db1").ok("SHOW TABLES FROM `CATALOG1`.`DB1`");
        sql("show tables in catalog1.db1").ok("SHOW TABLES IN `CATALOG1`.`DB1`");

        sql("show tables from db1 like '%'").ok("SHOW TABLES FROM `DB1` LIKE '%'");
        sql("show tables in db1 like '%'").ok("SHOW TABLES IN `DB1` LIKE '%'");

        sql("show tables from catalog1.db1 like '%'")
                .ok("SHOW TABLES FROM `CATALOG1`.`DB1` LIKE '%'");
        sql("show tables in catalog1.db1 like '%'").ok("SHOW TABLES IN `CATALOG1`.`DB1` LIKE '%'");

        sql("show tables from db1 not like '%'").ok("SHOW TABLES FROM `DB1` NOT LIKE '%'");
        sql("show tables in db1 not like '%'").ok("SHOW TABLES IN `DB1` NOT LIKE '%'");

        sql("show tables from catalog1.db1 not like '%'")
                .ok("SHOW TABLES FROM `CATALOG1`.`DB1` NOT LIKE '%'");
        sql("show tables in catalog1.db1 not like '%'")
                .ok("SHOW TABLES IN `CATALOG1`.`DB1` NOT LIKE '%'");

        sql("show tables ^db1^").fails("(?s).*Encountered \"db1\" at line 1, column 13.\n.*");
        sql("show tables ^catalog1^.db1")
                .fails("(?s).*Encountered \"catalog1\" at line 1, column 13.\n.*");

        sql("show tables ^search^ db1")
                .fails("(?s).*Encountered \"search\" at line 1, column 13.\n.*");

        sql("show tables from db1 ^likes^ '%t'")
                .fails("(?s).*Encountered \"likes\" at line 1, column 22.\n.*");
    }

    @Test
    void testShowCreateTable() {
        sql("show create table tbl").ok("SHOW CREATE TABLE `TBL`");
        sql("show create table catalog1.db1.tbl").ok("SHOW CREATE TABLE `CATALOG1`.`DB1`.`TBL`");
    }

    @Test
    void testShowCreateView() {
        sql("show create view v1").ok("SHOW CREATE VIEW `V1`");
        sql("show create view db1.v1").ok("SHOW CREATE VIEW `DB1`.`V1`");
        sql("show create view catalog1.db1.v1").ok("SHOW CREATE VIEW `CATALOG1`.`DB1`.`V1`");
    }

    @Test
    void testDescribeTable() {
        sql("describe tbl").ok("DESCRIBE `TBL`");
        sql("describe catalog1.db1.tbl").ok("DESCRIBE `CATALOG1`.`DB1`.`TBL`");
        sql("describe extended db1").ok("DESCRIBE EXTENDED `DB1`");

        sql("desc tbl").ok("DESCRIBE `TBL`");
        sql("desc catalog1.db1.tbl").ok("DESCRIBE `CATALOG1`.`DB1`.`TBL`");
        sql("desc extended db1").ok("DESCRIBE EXTENDED `DB1`");
    }

    @Test
    void testShowColumns() {
        sql("show columns from tbl").ok("SHOW COLUMNS FROM `TBL`");
        sql("show columns in tbl").ok("SHOW COLUMNS IN `TBL`");

        sql("show columns from db1.tbl").ok("SHOW COLUMNS FROM `DB1`.`TBL`");
        sql("show columns in db1.tbl").ok("SHOW COLUMNS IN `DB1`.`TBL`");

        sql("show columns from catalog1.db1.tbl").ok("SHOW COLUMNS FROM `CATALOG1`.`DB1`.`TBL`");
        sql("show columns in catalog1.db1.tbl").ok("SHOW COLUMNS IN `CATALOG1`.`DB1`.`TBL`");

        sql("show columns from tbl like '%'").ok("SHOW COLUMNS FROM `TBL` LIKE '%'");
        sql("show columns in tbl like '%'").ok("SHOW COLUMNS IN `TBL` LIKE '%'");

        sql("show columns from db1.tbl like '%'").ok("SHOW COLUMNS FROM `DB1`.`TBL` LIKE '%'");
        sql("show columns in db1.tbl like '%'").ok("SHOW COLUMNS IN `DB1`.`TBL` LIKE '%'");

        sql("show columns from catalog1.db1.tbl like '%'")
                .ok("SHOW COLUMNS FROM `CATALOG1`.`DB1`.`TBL` LIKE '%'");
        sql("show columns in catalog1.db1.tbl like '%'")
                .ok("SHOW COLUMNS IN `CATALOG1`.`DB1`.`TBL` LIKE '%'");

        sql("show columns from tbl not like '%'").ok("SHOW COLUMNS FROM `TBL` NOT LIKE '%'");
        sql("show columns in tbl not like '%'").ok("SHOW COLUMNS IN `TBL` NOT LIKE '%'");

        sql("show columns from db1.tbl not like '%'")
                .ok("SHOW COLUMNS FROM `DB1`.`TBL` NOT LIKE '%'");
        sql("show columns in db1.tbl not like '%'").ok("SHOW COLUMNS IN `DB1`.`TBL` NOT LIKE '%'");

        sql("show columns from catalog1.db1.tbl not like '%'")
                .ok("SHOW COLUMNS FROM `CATALOG1`.`DB1`.`TBL` NOT LIKE '%'");
        sql("show columns in catalog1.db1.tbl not like '%'")
                .ok("SHOW COLUMNS IN `CATALOG1`.`DB1`.`TBL` NOT LIKE '%'");
    }

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
                .fails("Multiple WATERMARK statements is not supported yet.");
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
                .fails("Multiple WATERMARK statements is not supported yet.");
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
    void testAlterTableCompact() {
        sql("alter table if exists t1 compact").ok("ALTER TABLE IF EXISTS `T1` COMPACT");

        sql("alter table t1 compact").ok("ALTER TABLE `T1` COMPACT");

        sql("alter table db1.t1 compact").ok("ALTER TABLE `DB1`.`T1` COMPACT");

        sql("alter table cat1.db1.t1 compact").ok("ALTER TABLE `CAT1`.`DB1`.`T1` COMPACT");

        sql("alter table t1 partition(x='y',m='n') compact")
                .ok("ALTER TABLE `T1` PARTITION (`X` = 'y', `M` = 'n') COMPACT");

        sql("alter table t1 partition(^)^ compact")
                .fails("(?s).*Encountered \"\\)\" at line 1, column 26.\n.*");
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

    @Test
    void testCreateTable() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  meta STRING METADATA, \n"
                        + "  my_meta STRING METADATA FROM 'meta', \n"
                        + "  my_meta STRING METADATA FROM 'meta' VIRTUAL, \n"
                        + "  meta STRING METADATA VIRTUAL, \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  `META` STRING METADATA,\n"
                        + "  `MY_META` STRING METADATA FROM 'meta',\n"
                        + "  `MY_META` STRING METADATA FROM 'meta' VIRTUAL,\n"
                        + "  `META` STRING METADATA VIRTUAL,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableIfNotExists() {
        final String sql =
                "CREATE TABLE IF NOT EXISTS tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE IF NOT EXISTS `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithComment() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint comment 'test column comment AAA.',\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  meta STRING METADATA COMMENT 'c1', \n"
                        + "  my_meta STRING METADATA FROM 'meta' COMMENT 'c2', \n"
                        + "  my_meta STRING METADATA FROM 'meta' VIRTUAL COMMENT 'c3', \n"
                        + "  meta STRING METADATA VIRTUAL COMMENT 'c4', \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "comment 'test table comment ABC.'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT COMMENT 'test column comment AAA.',\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  `META` STRING METADATA COMMENT 'c1',\n"
                        + "  `MY_META` STRING METADATA FROM 'meta' COMMENT 'c2',\n"
                        + "  `MY_META` STRING METADATA FROM 'meta' VIRTUAL COMMENT 'c3',\n"
                        + "  `META` STRING METADATA VIRTUAL COMMENT 'c4',\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'test table comment ABC.'\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithCommentOnComputedColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint comment 'test column comment AAA.',\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1) comment 'test computed column.', \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(), \n"
                        + "  PRIMARY KEY (a, b)\n"
                        + ")\n"
                        + "comment 'test table comment ABC.'\n"
                        + "PARTITIONED BY (a, h)\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT COMMENT 'test column comment AAA.',\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)) COMMENT 'test computed column.',\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ")\n"
                        + "COMMENT 'test table comment ABC.'\n"
                        + "PARTITIONED BY (`A`, `H`)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testTableConstraints() {
        final String sql1 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b),\n"
                        + "  UNIQUE (h, g)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected1 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`),\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql1)
                .ok(expected1)
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                                + "ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        final String sql2 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED,\n"
                        + "  UNIQUE (h, g)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected2 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED,\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql2)
                .ok(expected2)
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        final String sql3 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expectParsed =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        final String expectValidated =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT NOT NULL,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR NOT NULL,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql3).ok(expectParsed).node(validated(expectValidated));
    }

    @Test
    void testColumnConstraints() {
        final String sql1 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key,\n"
                        + "  h varchar unique,\n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected1 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY,\n"
                        + "  `H` VARCHAR UNIQUE,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql1)
                .ok(expected1)
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. "
                                                + "ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        final String sql2 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key not enforced,\n"
                        + "  h varchar unique,\n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected2 =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY NOT ENFORCED,\n"
                        + "  `H` VARCHAR UNIQUE,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql2)
                .ok(expected2)
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        final String sql3 =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key not enforced,\n"
                        + "  h varchar,\n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expectParsed =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY NOT ENFORCED,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        final String expectValidated =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql3).ok(expectParsed).node(validated(expectValidated));
    }

    @Test
    void testUniqueTableConstraint() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint,\n"
                        + "  h varchar, \n"
                        + "  g as 2 * (a + 1),\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  PRIMARY KEY (a, b) NOT ENFORCED,\n"
                        + "  UNIQUE (h, g)\n"
                        + ") with (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`) NOT ENFORCED,\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
        sql(sql).node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testTableConstraintsWithEnforcement() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint primary key enforced comment 'test column comment AAA.',\n"
                        + "  h varchar constraint ct1 unique not enforced,\n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar constraint ct2 unique,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  unique (g, ts) not enforced"
                        + ") with (\n"
                        + "    'connector' = 'kafka',\n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT PRIMARY KEY ENFORCED COMMENT 'test column comment AAA.',\n"
                        + "  `H` VARCHAR CONSTRAINT `CT1` UNIQUE NOT ENFORCED,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR CONSTRAINT `CT2` UNIQUE,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  UNIQUE (`G`, `TS`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testDuplicatePk() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a bigint comment 'test column comment AAA.',\n"
                        + "  h varchar constraint ct1 primary key,\n"
                        + "  g as 2 * (a + 1), \n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  b varchar,\n"
                        + "  proc as PROCTIME(),\n"
                        + "  constraint ct2 primary key (b, h)"
                        + ") with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).node(new ValidationMatcher().fails("Duplicate primary key definition"));
    }

    @Test
    void testCreateTableWithWatermark() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  ts timestamp(3),\n"
                        + "  id varchar, \n"
                        + "  watermark FOR ts AS ts - interval '3' second\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `TS` TIMESTAMP(3),\n"
                        + "  `ID` VARCHAR,\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithWatermarkOnComputedColumn() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  log_ts varchar,\n"
                        + "  ts as to_timestamp(log_ts), \n"
                        + "  WATERMARK FOR ts AS ts + interval '1' second\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `LOG_TS` VARCHAR,\n"
                        + "  `TS` AS `TO_TIMESTAMP`(`LOG_TS`),\n"
                        + "  WATERMARK FOR `TS` AS (`TS` + INTERVAL '1' SECOND)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithWatermarkOnNestedField() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  f1 row<q1 bigint, q2 row<t1 timestamp, t2 varchar>, q3 boolean>,\n"
                        + "  WATERMARK FOR f1.q2.t1 AS NOW()\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `F1` ROW< `Q1` BIGINT, `Q2` ROW< `T1` TIMESTAMP, `T2` VARCHAR >, `Q3` BOOLEAN >,\n"
                        + "  WATERMARK FOR `F1`.`Q2`.`T1` AS `NOW`()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithMultipleWatermark() {
        String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  f0 bigint,\n"
                        + "  f1 varchar,\n"
                        + "  f2 boolean,\n"
                        + "  WATERMARK FOR f0 AS NOW(),\n"
                        + "  ^WATERMARK^ FOR f1 AS NOW()\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).fails("Multiple WATERMARK statements is not supported yet.");
    }

    @Test
    void testCreateTableWithQueryWatermarkExpression() {
        String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  f0 bigint,\n"
                        + "  f1 varchar,\n"
                        + "  f2 boolean,\n"
                        + "  WATERMARK FOR f0 AS ^(^SELECT f1 FROM tbl1)\n"
                        + ")\n"
                        + "  with (\n"
                        + "    'connector' = 'kafka', \n"
                        + "    'kafka.topic' = 'log.test'\n"
                        + ")\n";
        sql(sql).fails("Query expression encountered in illegal context");
    }

    @Test
    void testCreateTableWithComplexType() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a ARRAY<bigint>, \n"
                        + "  b MAP<int, varchar>,\n"
                        + "  c ROW<cc0 int, cc1 float, cc2 varchar>,\n"
                        + "  d MULTISET<varchar>,\n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` ARRAY< BIGINT >,\n"
                        + "  `B` MAP< INTEGER, VARCHAR >,\n"
                        + "  `C` ROW< `CC0` INTEGER, `CC1` FLOAT, `CC2` VARCHAR >,\n"
                        + "  `D` MULTISET< VARCHAR >,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ") WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'asd' = 'data'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithNestedComplexType() {
        final String sql =
                "CREATE TABLE tbl1 (\n"
                        + "  a ARRAY<ARRAY<bigint>>, \n"
                        + "  b MAP<MAP<int, varchar>, ARRAY<varchar>>,\n"
                        + "  c ROW<cc0 ARRAY<int>, cc1 float, cc2 varchar>,\n"
                        + "  d MULTISET<ARRAY<int>>,\n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` ARRAY< ARRAY< BIGINT > >,\n"
                        + "  `B` MAP< MAP< INTEGER, VARCHAR >, ARRAY< VARCHAR > >,\n"
                        + "  `C` ROW< `CC0` ARRAY< INTEGER >, `CC1` FLOAT, `CC2` VARCHAR >,\n"
                        + "  `D` MULTISET< ARRAY< INTEGER > >,\n"
                        + "  PRIMARY KEY (`A`, `B`)\n"
                        + ") WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'asd' = 'data'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithUserDefinedType() {
        final String sql =
                "create table t(\n"
                        + "  a catalog1.db1.MyType1,\n"
                        + "  b db2.MyType2\n"
                        + ") with (\n"
                        + "  'k1' = 'v1',\n"
                        + "  'k2' = 'v2'\n"
                        + ")";
        final String expected =
                "CREATE TABLE `T` (\n"
                        + "  `A` `CATALOG1`.`DB1`.`MYTYPE1`,\n"
                        + "  `B` `DB2`.`MYTYPE2`\n"
                        + ") WITH (\n"
                        + "  'k1' = 'v1',\n"
                        + "  'k2' = 'v2'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testInvalidComputedColumn() {
        final String sql0 =
                "CREATE TABLE t1 (\n"
                        + "  a bigint, \n"
                        + "  b varchar,\n"
                        + "  toTimestamp^(^b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expect0 =
                "(?s).*Encountered \"\\(\" at line 4, column 14.\n"
                        + "Was expecting one of:\n"
                        + "    \"AS\" ...\n"
                        + "    \".\" ...\n"
                        + "    \"STRING\" ...\n"
                        + ".*";
        sql(sql0).fails(expect0);
        // Sub-query computed column expression is forbidden.
        final String sql1 =
                "CREATE TABLE t1 (\n"
                        + "  a bigint, \n"
                        + "  b varchar,\n"
                        + "  c as ^(^select max(d) from t2), \n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expect1 = "(?s).*Query expression encountered in illegal context.*";
        sql(sql1).fails(expect1);
    }

    @Test
    void testColumnSqlString() {
        final String sql =
                "CREATE TABLE sls_stream (\n"
                        + "  a bigint, \n"
                        + "  f as a + 1, \n"
                        + "  b varchar,\n"
                        + "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n"
                        + "  proc as PROCTIME(),\n"
                        + "  c int,\n"
                        + "  PRIMARY KEY (a, b) \n"
                        + ") with (\n"
                        + "  'x' = 'y', \n"
                        + "  'asd' = 'data'\n"
                        + ")\n";
        final String expected =
                "`A`, (`A` + 1) AS `F`, `B`, "
                        + "`TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss') AS `TS`, "
                        + "`PROCTIME`() AS `PROC`, `C`";
        sql(sql).node(new ValidationMatcher().expectColumnSql(expected));
    }

    @Test
    void testCreateTableWithMinusInOptionKey() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") with (\n"
                        + "  'a-b-c-d124' = 'ab',\n"
                        + "  'a.b.1.c' = 'aabb',\n"
                        + "  'a.b-c-connector.e-f.g' = 'ada',\n"
                        + "  'a.b-c-d.e-1231.g' = 'ada',\n"
                        + "  'a.b-c-d.*' = 'adad')\n";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ") WITH (\n"
                        + "  'a-b-c-d124' = 'ab',\n"
                        + "  'a.b.1.c' = 'aabb',\n"
                        + "  'a.b-c-connector.e-f.g' = 'ada',\n"
                        + "  'a.b-c-d.e-1231.g' = 'ada',\n"
                        + "  'a.b-c-d.*' = 'adad'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithOptionKeyAsIdentifier() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") with (\n"
                        + "  ^a^.b.c = 'ab',\n"
                        + "  a.b.c1 = 'aabb')\n";
        sql(sql).fails("(?s).*Encountered \"a\" at line 6, column 3.\n.*");
    }

    @Test
    void testCreateTableLikeWithoutOption() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "LIKE `PARENT_TABLE`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableLikeWithConstraints() {
        final String sql1 =
                "create table source_table(\n"
                        + "  a int primary key,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table";
        sql(sql1)
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED "
                                                + "controls if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        final String sql2 =
                "create table source_table(\n"
                        + "  a int primary key,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  primary key(a) not enforced\n"
                        + ")\n"
                        + "LIKE parent_table";

        sql(sql2).node(new ValidationMatcher().fails("Duplicate primary key definition"));

        final String sql3 =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  unique (a)\n"
                        + ")\n"
                        + "LIKE parent_table";

        sql(sql3).node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testCreateTableWithLikeClause() {
        final String sql =
                "create table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ")\n"
                        + "LIKE parent_table (\n"
                        + "   INCLUDING ALL\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   EXCLUDING PARTITIONS\n"
                        + "   INCLUDING GENERATED\n"
                        + "   INCLUDING METADATA\n"
                        + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ")\n"
                        + "LIKE `PARENT_TABLE` (\n"
                        + "  INCLUDING ALL\n"
                        + "  OVERWRITING OPTIONS\n"
                        + "  EXCLUDING PARTITIONS\n"
                        + "  INCLUDING GENERATED\n"
                        + "  INCLUDING METADATA\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithLikeClauseWithoutColumns() {
        final String sql =
                ""
                        + "create TEMPORARY table source_table (\n"
                        + "   WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                        + ") with (\n"
                        + "  'scan.startup.mode' = 'specific-offsets',\n"
                        + "  'scan.startup.specific-offsets' = 'partition:0,offset:1169129'\n"
                        + ") like t_order_course (\n"
                        + "   OVERWRITING  WATERMARKS\n"
                        + "   OVERWRITING OPTIONS\n"
                        + "   EXCLUDING CONSTRAINTS\n"
                        + ")";
        final String expected =
                "CREATE TEMPORARY TABLE `SOURCE_TABLE` (\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '5' SECOND)\n"
                        + ") WITH (\n"
                        + "  'scan.startup.mode' = 'specific-offsets',\n"
                        + "  'scan.startup.specific-offsets' = 'partition:0,offset:1169129'\n"
                        + ")\n"
                        + "LIKE `T_ORDER_COURSE` (\n"
                        + "  OVERWRITING WATERMARKS\n"
                        + "  OVERWRITING OPTIONS\n"
                        + "  EXCLUDING CONSTRAINTS\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTemporaryTable() {
        final String sql =
                "create temporary table source_table(\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") with (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        final String expected =
                "CREATE TEMPORARY TABLE `SOURCE_TABLE` (\n"
                        + "  `A` INTEGER,\n"
                        + "  `B` BIGINT,\n"
                        + "  `C` STRING\n"
                        + ") WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithNoColumns() {
        final String sql =
                "create table source_table with (\n" + "  'x' = 'y',\n" + "  'abc' = 'def'\n" + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTableWithOnlyWaterMark() {
        final String sql =
                "create table source_table (\n"
                        + "  watermark FOR ts AS ts - interval '3' second\n"
                        + ") with (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        final String expected =
                "CREATE TABLE `SOURCE_TABLE` (\n"
                        + "  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n"
                        + ") WITH (\n"
                        + "  'x' = 'y',\n"
                        + "  'abc' = 'def'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    void testDropTable() {
        final String sql = "DROP table catalog1.db1.tbl1";
        final String expected = "DROP TABLE `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropIfExists() {
        final String sql = "DROP table IF EXISTS catalog1.db1.tbl1";
        final String expected = "DROP TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testTemporaryDropTable() {
        final String sql = "DROP temporary table catalog1.db1.tbl1";
        final String expected = "DROP TEMPORARY TABLE `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropTemporaryIfExists() {
        final String sql = "DROP temporary table IF EXISTS catalog1.db1.tbl1";
        final String expected = "DROP TEMPORARY TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    void testInsertPartitionSpecs() {
        final String sql1 = "insert into emps partition (x='ab', y='bc') (x,y) select * from emps";
        final String expected =
                "INSERT INTO `EMPS` "
                        + "PARTITION (`X` = 'ab', `Y` = 'bc')\n"
                        + "(`X`, `Y`)\n"
                        + "(SELECT *\n"
                        + "FROM `EMPS`)";
        sql(sql1).ok(expected);
        final String sql2 =
                "insert into emp\n"
                        + "partition(empno='1', job='job')\n"
                        + "(empno, ename, job, mgr, hiredate,\n"
                        + "  sal, comm, deptno, slacker)\n"
                        + "select 'nom', 0, timestamp '1970-01-01 00:00:00',\n"
                        + "  1, 1, 1, false\n"
                        + "from (values 'a')";
        sql(sql2)
                .ok(
                        "INSERT INTO `EMP` "
                                + "PARTITION (`EMPNO` = '1', `JOB` = 'job')\n"
                                + "(`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,"
                                + " `COMM`, `DEPTNO`, `SLACKER`)\n"
                                + "(SELECT 'nom', 0, TIMESTAMP '1970-01-01 00:00:00', 1, 1, 1, FALSE\n"
                                + "FROM (VALUES (ROW('a'))))");
        final String sql3 =
                "insert into empnullables\n"
                        + "partition(ename='b')\n"
                        + "(empno, ename)\n"
                        + "select 1 from (values 'a')";
        sql(sql3)
                .ok(
                        "INSERT INTO `EMPNULLABLES` "
                                + "PARTITION (`ENAME` = 'b')\n"
                                + "(`EMPNO`, `ENAME`)\n"
                                + "(SELECT 1\n"
                                + "FROM (VALUES (ROW('a'))))");
    }

    @Test
    void testInsertCaseSensitivePartitionSpecs() {
        final String expected =
                "INSERT INTO `emps` "
                        + "PARTITION (`x` = 'ab', `y` = 'bc')\n"
                        + "(`x`, `y`)\n"
                        + "(SELECT *\n"
                        + "FROM `EMPS`)";
        sql("insert into \"emps\" "
                        + "partition (\"x\"='ab', \"y\"='bc')(\"x\",\"y\") select * from emps")
                .ok(expected);
    }

    @Test
    void testInsertExtendedColumnAsStaticPartition1() {
        final String expected =
                "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) "
                        + "PARTITION (`Z` = 'ab')\n"
                        + "(`X`, `Y`)\n"
                        + "(SELECT *\n"
                        + "FROM `EMPS`)";
        sql("insert into emps(z boolean) partition (z='ab') (x,y) select * from emps").ok(expected);
    }

    @Test
    void testInsertExtendedColumnAsStaticPartition2() {
        assertThatThrownBy(
                        () ->
                                sql("insert into emps(x, y, z boolean) partition (z='ab') select * from emps")
                                        .node(
                                                new ValidationMatcher()
                                                        .fails(
                                                                "Extended columns not allowed under the current SQL conformance level")))
                .isInstanceOf(SqlParseException.class);
    }

    @Test
    void testInsertOverwrite() {
        // non-partitioned
        final String sql = "INSERT OVERWRITE myDB.myTbl SELECT * FROM src";
        final String expected = "INSERT OVERWRITE `MYDB`.`MYTBL`\n" + "(SELECT *\n" + "FROM `SRC`)";
        sql(sql).ok(expected);

        // partitioned
        final String sql1 = "INSERT OVERWRITE myTbl PARTITION (p1='v1',p2='v2') SELECT * FROM src";
        final String expected1 =
                "INSERT OVERWRITE `MYTBL` "
                        + "PARTITION (`P1` = 'v1', `P2` = 'v2')\n"
                        + "\n"
                        + "(SELECT *\n"
                        + "FROM `SRC`)";
        sql(sql1).ok(expected1);
    }

    @Test
    void testInvalidUpsertOverwrite() {
        sql("UPSERT ^OVERWRITE^ myDB.myTbl SELECT * FROM src")
                .fails("OVERWRITE expression is only used with INSERT statement.");
    }

    @Test
    void testCreateView() {
        final String sql = "create view v as select col1 from tbl";
        final String expected = "CREATE VIEW `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewWithInvalidFieldList() {
        final String expected =
                "(?s).*Encountered \"\\)\" at line 1, column 15.\n"
                        + "Was expecting one of:\n"
                        + ".*\n"
                        + ".*\n"
                        + ".*\n"
                        + ".*\n"
                        + ".*";
        sql("CREATE VIEW V(^)^ AS SELECT * FROM TBL").fails(expected);
    }

    @Test
    void testCreateViewWithComment() {
        final String sql = "create view v COMMENT 'this is a view' as select col1 from tbl";
        final String expected =
                "CREATE VIEW `V`\n"
                        + "COMMENT 'this is a view'\n"
                        + "AS\n"
                        + "SELECT `COL1`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewWithFieldNames() {
        final String sql = "create view v(col1, col2) as select col3, col4 from tbl";
        final String expected =
                "CREATE VIEW `V` (`COL1`, `COL2`)\n"
                        + "AS\n"
                        + "SELECT `COL3`, `COL4`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewWithInvalidName() {
        final String sql = "create view v(^*^) COMMENT 'this is a view' as select col1 from tbl";
        final String expected = "(?s).*Encountered \"\\*\" at line 1, column 15.*";

        sql(sql).fails(expected);
    }

    @Test
    void testCreateTemporaryView() {
        final String sql = "create temporary view v as select col1 from tbl";
        final String expected =
                "CREATE TEMPORARY VIEW `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateTemporaryViewIfNotExists() {
        final String sql = "create temporary view if not exists v as select col1 from tbl";
        final String expected =
                "CREATE TEMPORARY VIEW IF NOT EXISTS `V`\n"
                        + "AS\n"
                        + "SELECT `COL1`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testCreateViewIfNotExists() {
        final String sql = "create view if not exists v as select col1 from tbl";
        final String expected =
                "CREATE VIEW IF NOT EXISTS `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropView() {
        final String sql = "DROP VIEW IF EXISTS view_name";
        final String expected = "DROP VIEW IF EXISTS `VIEW_NAME`";
        sql(sql).ok(expected);
    }

    @Test
    void testDropTemporaryView() {
        final String sql = "DROP TEMPORARY VIEW IF EXISTS view_name";
        final String expected = "DROP TEMPORARY VIEW IF EXISTS `VIEW_NAME`";
        sql(sql).ok(expected);
    }

    @Test
    void testAlterView() {
        sql("ALTER VIEW v1 RENAME TO v2").ok("ALTER VIEW `V1` RENAME TO `V2`");
        sql("ALTER VIEW v1 AS SELECT c1, c2 FROM tbl")
                .ok("ALTER VIEW `V1`\n" + "AS\n" + "SELECT `C1`, `C2`\n" + "FROM `TBL`");
    }

    @Test
    void testShowViews() {
        sql("show views").ok("SHOW VIEWS");
    }

    @Test
    void testShowPartitions() {
        sql("show partitions c1.d1.tbl").ok("SHOW PARTITIONS `C1`.`D1`.`TBL`");
        sql("show partitions tbl partition (p=1)").ok("SHOW PARTITIONS `TBL` PARTITION (`P` = 1)");
    }

    // Override the test because our ROW field type default is nullable,
    // which is different with Calcite.
    @Test
    void testCastAsRowType() {
        final String expr = "cast(a as row(f0 int, f1 varchar))";
        final String expected = "CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))";
        expr(expr).ok(expected);

        final String expr1 = "cast(a as row(f0 int not null, f1 varchar null))";
        final String expected1 = "CAST(`A` AS ROW(`F0` INTEGER NOT NULL, `F1` VARCHAR))";
        expr(expr1).ok(expected1);

        final String expr2 =
                "cast(a as row(f0 row(ff0 int not null, ff1 varchar null) null,"
                        + " f1 timestamp not null))";
        final String expected2 =
                "CAST(`A` AS ROW(`F0` ROW(`FF0` INTEGER NOT NULL, `FF1` VARCHAR),"
                        + " `F1` TIMESTAMP NOT NULL))";
        expr(expr2).ok(expected2);

        final String expr3 = "cast(a as row(f0 bigint not null, f1 decimal null) array)";
        final String expected3 = "CAST(`A` AS ROW(`F0` BIGINT NOT NULL, `F1` DECIMAL) ARRAY)";
        expr(expr3).ok(expected3);

        final String expr4 = "cast(a as row(f0 varchar not null, f1 timestamp null) multiset)";
        final String expected4 = "CAST(`A` AS ROW(`F0` VARCHAR NOT NULL, `F1` TIMESTAMP) MULTISET)";
        expr(expr4).ok(expected4);
    }

    @Test
    void testCreateTableWithNakedTableName() {
        String sql = "CREATE TABLE tbl1";
        sql(sql).node(new ValidationMatcher());
    }

    @Test
    void testCreateViewWithEmptyFields() {
        String sql = "CREATE VIEW v1 AS SELECT 1";
        sql(sql).ok("CREATE VIEW `V1`\n" + "AS\n" + "SELECT 1");
    }

    @Test
    void testCreateFunction() {
        sql("create function catalog1.db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function catalog1.db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function if not exists catalog1.db1.function1 as 'org.apache.flink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION IF NOT EXISTS `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.flink.function.function1'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language java")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA");

        sql("create temporary system function  function1 as 'org.apache.flink.function.function1' language scala")
                .ok(
                        "CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA");

        // Temporary system function always belongs to the system and current session.
        sql("create temporary system function catalog1^.^db1.function1 as 'org.apache.flink.function.function1'")
                .fails("(?s).*Encountered \".\" at.*");

        sql("create ^system^ function function1 as 'org.apache.flink.function.function1'")
                .fails(
                        "CREATE SYSTEM FUNCTION is not supported, "
                                + "system functions can only be registered as temporary "
                                + "function, you can use CREATE TEMPORARY SYSTEM FUNCTION instead.");

        // test create function using jar
        sql("create temporary function function1 as 'org.apache.flink.function.function1' language java using jar 'file:///path/to/test.jar'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING JAR 'file:///path/to/test.jar'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language scala using jar '/path/to/test.jar'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA USING JAR '/path/to/test.jar'");

        sql("create temporary system function function1 as 'org.apache.flink.function.function1' language scala using jar '/path/to/test.jar'")
                .ok(
                        "CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE SCALA USING JAR '/path/to/test.jar'");

        sql("create function function1 as 'org.apache.flink.function.function1' language java using jar 'file:///path/to/test.jar', jar 'hdfs:///path/to/test2.jar'")
                .ok(
                        "CREATE FUNCTION `FUNCTION1` AS 'org.apache.flink.function.function1' LANGUAGE JAVA USING JAR 'file:///path/to/test.jar', JAR 'hdfs:///path/to/test2.jar'");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language ^sql^ using jar 'file:///path/to/test.jar'")
                .fails("CREATE FUNCTION USING JAR syntax is not applicable to SQL language.");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language ^python^ using jar 'file:///path/to/test.jar'")
                .fails("CREATE FUNCTION USING JAR syntax is not applicable to PYTHON language.");

        sql("create temporary function function1 as 'org.apache.flink.function.function1' language java using ^file^ 'file:///path/to/test'")
                .fails(
                        "Encountered \"file\" at line 1, column 98.\n"
                                + "Was expecting:\n"
                                + "    \"JAR\" ...\n"
                                + "    .*");
    }

    @Test
    void testDropTemporaryFunction() {
        sql("drop temporary function catalog1.db1.function1")
                .ok("DROP TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary system function catalog1.db1.function1")
                .ok("DROP TEMPORARY SYSTEM FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary function if exists catalog1.db1.function1")
                .ok("DROP TEMPORARY FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary system function if exists catalog1.db1.function1")
                .ok("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");
    }

    @Test
    void testLoadModule() {
        sql("load module core").ok("LOAD MODULE `CORE`");

        sql("load module dummy with ('k1' = 'v1', 'k2' = 'v2')")
                .ok(
                        "LOAD MODULE `DUMMY`"
                                + " WITH (\n"
                                + "  'k1' = 'v1',\n"
                                + "  'k2' = 'v2'\n"
                                + ")");

        sql("load module ^'core'^")
                .fails("(?s).*Encountered \"\\\\'core\\\\'\" at line 1, column 13.\n.*");
    }

    @Test
    void testUnloadModule() {
        sql("unload module core").ok("UNLOAD MODULE `CORE`");

        sql("unload module ^'core'^")
                .fails("(?s).*Encountered \"\\\\'core\\\\'\" at line 1, column 15.\n.*");
    }

    @Test
    void testUseModules() {
        sql("use modules core").ok("USE MODULES `CORE`");

        sql("use modules x, y, z").ok("USE MODULES `X`, `Y`, `Z`");

        sql("use modules x^,^").fails("(?s).*Encountered \"<EOF>\" at line 1, column 14.\n.*");

        sql("use modules ^'core'^")
                .fails("(?s).*Encountered \"\\\\'core\\\\'\" at line 1, column 13.\n.*");
    }

    @Test
    void testShowModules() {
        sql("show modules").ok("SHOW MODULES");

        sql("show full modules").ok("SHOW FULL MODULES");
    }

    @Test
    void testBeginStatementSet() {
        sql("begin statement set").ok("BEGIN STATEMENT SET");
    }

    @Test
    void testEnd() {
        sql("end").ok("END");
    }

    @Test
    void testExecuteStatementSet() {
        sql("execute statement set begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "EXECUTE STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "(SELECT *\n"
                                + "FROM `T3`)\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testExplainStatementSet() {
        sql("explain statement set begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "EXPLAIN STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "(SELECT *\n"
                                + "FROM `T3`)\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testExplain() {
        String sql = "explain select * from emps";
        String expected = "EXPLAIN SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExecuteSelect() {
        String sql = "execute select * from emps";
        String expected = "EXECUTE SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainPlanFor() {
        String sql = "explain plan for select * from emps";
        String expected = "EXPLAIN SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainChangelogMode() {
        String sql = "explain changelog_mode select * from emps";
        String expected = "EXPLAIN CHANGELOG_MODE SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainEstimatedCost() {
        String sql = "explain estimated_cost select * from emps";
        String expected = "EXPLAIN ESTIMATED_COST SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainUnion() {
        String sql = "explain estimated_cost select * from emps union all select * from emps";
        String expected =
                "EXPLAIN ESTIMATED_COST (SELECT *\n"
                        + "FROM `EMPS`\n"
                        + "UNION ALL\n"
                        + "SELECT *\n"
                        + "FROM `EMPS`)";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainJsonFormat() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainWithImpl() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainWithoutImpl() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainWithType() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testExplainAsXml() {
        // Unsupported feature. Escape the test.
    }

    @Test
    void testSqlOptions() {
        // SET/RESET are overridden for Flink SQL
    }

    @Test
    void testExplainAsJson() {
        String sql = "explain json_execution_plan select * from emps";
        String expected = "EXPLAIN JSON_EXECUTION_PLAN SELECT *\n" + "FROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainPlanAdvice() {
        String sql = "explain plan_advice select * from emps";
        String expected = "EXPLAIN PLAN_ADVICE SELECT *\nFROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainAllDetails() {
        String sql =
                "explain changelog_mode,json_execution_plan,estimated_cost,plan_advice select * from emps";
        String expected =
                "EXPLAIN JSON_EXECUTION_PLAN, CHANGELOG_MODE, PLAN_ADVICE, ESTIMATED_COST SELECT *\n"
                        + "FROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainInsert() {
        String expected = "EXPLAIN INSERT INTO `EMPS1`\n" + "(SELECT *\n" + "FROM `EMPS2`)";
        this.sql("explain plan for insert into emps1 select * from emps2").ok(expected);
    }

    @Test
    void testExecuteInsert() {
        String expected = "EXECUTE INSERT INTO `EMPS1`\n" + "(SELECT *\n" + "FROM `EMPS2`)";
        this.sql("execute insert into emps1 select * from emps2").ok(expected);
    }

    @Test
    void testExecutePlan() {
        sql("execute plan './test.json'").ok("EXECUTE PLAN './test.json'");
        sql("execute plan '/some/absolute/dir/plan.json'")
                .ok("EXECUTE PLAN '/some/absolute/dir/plan.json'");
        sql("execute plan 'file:///foo/bar/test.json'")
                .ok("EXECUTE PLAN 'file:///foo/bar/test.json'");
    }

    @Test
    void testCompilePlan() {
        sql("compile plan './test.json' for insert into t1 select * from t2")
                .ok(
                        "COMPILE PLAN './test.json' FOR INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)");
        sql("compile plan './test.json' if not exists for insert into t1 select * from t2")
                .ok(
                        "COMPILE PLAN './test.json' IF NOT EXISTS FOR INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)");
        sql("compile plan 'file:///foo/bar/test.json' if not exists for insert into t1 select * from t2")
                .ok(
                        "COMPILE PLAN 'file:///foo/bar/test.json' IF NOT EXISTS FOR INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)");

        sql("compile plan './test.json' for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE PLAN './test.json' FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "(SELECT *\n"
                                + "FROM `T3`)\n"
                                + ";\n"
                                + "END");
        sql("compile plan './test.json' if not exists for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE PLAN './test.json' IF NOT EXISTS FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "(SELECT *\n"
                                + "FROM `T3`)\n"
                                + ";\n"
                                + "END");

        sql("compile plan 'file:///foo/bar/test.json' if not exists for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE PLAN 'file:///foo/bar/test.json' IF NOT EXISTS FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "(SELECT *\n"
                                + "FROM `T3`)\n"
                                + ";\n"
                                + "END");
    }

    @Test
    void testCompileAndExecutePlan() {
        sql("compile and execute plan './test.json' for insert into t1 select * from t2")
                .ok(
                        "COMPILE AND EXECUTE PLAN './test.json' FOR INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)");

        sql("compile and execute plan './test.json' for statement set "
                        + "begin insert into t1 select * from t2; insert into t2 select * from t3; end")
                .ok(
                        "COMPILE AND EXECUTE PLAN './test.json' FOR STATEMENT SET BEGIN\n"
                                + "INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)\n"
                                + ";\n"
                                + "INSERT INTO `T2`\n"
                                + "(SELECT *\n"
                                + "FROM `T3`)\n"
                                + ";\n"
                                + "END");
        sql("compile and execute plan 'file:///foo/bar/test.json' for insert into t1 select * from t2")
                .ok(
                        "COMPILE AND EXECUTE PLAN 'file:///foo/bar/test.json' FOR INSERT INTO `T1`\n"
                                + "(SELECT *\n"
                                + "FROM `T2`)");
    }

    @Test
    void testExplainUpsert() {
        String sql = "explain plan for upsert into emps1 values (1, 2)";
        String expected = "EXPLAIN UPSERT INTO `EMPS1`\n" + "VALUES (ROW(1, 2))";
        this.sql(sql).ok(expected);
    }

    @Test
    void testExplainPlanForWithExplainDetails() {
        String sql = "explain plan for ^json_execution_plan^ upsert into emps1 values (1, 2)";
        this.sql(sql).fails("Non-query expression encountered in illegal context");
    }

    @Test
    void testExplainDuplicateExplainDetails() {
        String sql = "explain changelog_mode,^changelog_mode^ select * from emps";
        this.sql(sql).fails("Duplicate EXPLAIN DETAIL is not allowed.");
    }

    @Test
    void testAddJar() {
        sql("add Jar './test.sql'").ok("ADD JAR './test.sql'");
        sql("add JAR 'file:///path/to/\nwhatever'").ok("ADD JAR 'file:///path/to/\nwhatever'");
        sql("add JAR 'oss://path/helloworld.go'").ok("ADD JAR 'oss://path/helloworld.go'");
    }

    @Test
    void testRemoveJar() {
        sql("remove Jar './test.sql'").ok("REMOVE JAR './test.sql'");
        sql("remove JAR 'file:///path/to/\nwhatever'")
                .ok("REMOVE JAR 'file:///path/to/\nwhatever'");
        sql("remove JAR 'oss://path/helloworld.go'").ok("REMOVE JAR 'oss://path/helloworld.go'");
    }

    @Test
    void testShowJars() {
        sql("show jars").ok("SHOW JARS");
    }

    @Test
    void testSetReset() {
        sql("SET").ok("SET");
        sql("SET 'test-key' = 'test-value'").ok("SET 'test-key' = 'test-value'");
        sql("RESET").ok("RESET");
        sql("RESET 'test-key'").ok("RESET 'test-key'");
    }

    @Test
    void testTryCast() {
        // Simple types
        expr("try_cast(a as timestamp)").ok("TRY_CAST(`A` AS TIMESTAMP)");
        expr("try_cast('abc' as timestamp)").ok("TRY_CAST('abc' AS TIMESTAMP)");

        // Complex types
        expr("try_cast(a as row(f0 int, f1 varchar))")
                .ok("TRY_CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))");
        expr("try_cast(a as row(f0 int array, f1 map<string, decimal(10, 2)>, f2 STRING NOT NULL))")
                .ok(
                        "TRY_CAST(`A` AS ROW(`F0` INTEGER ARRAY, `F1` MAP< STRING, DECIMAL(10, 2) >, `F2` STRING NOT NULL))");
    }

    @Test
    void testAnalyzeTable() {
        sql("analyze table emp^s^").fails("(?s).*Encountered \"<EOF>\" at line 1, column 18.\n.*");
        sql("analyze table emps compute statistics").ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS");
        sql("analyze table emps partition ^compute^ statistics")
                .fails("(?s).*Encountered \"compute\" at line 1, column 30.\n.*");
        sql("analyze table emps partition(^)^ compute statistics")
                .fails("(?s).*Encountered \"\\)\" at line 1, column 30.\n.*");
        sql("analyze table emps partition(x='ab') compute statistics")
                .ok("ANALYZE TABLE `EMPS` PARTITION (`X` = 'ab') COMPUTE STATISTICS");
        sql("analyze table emps partition(x='ab', y='bc') compute statistics")
                .ok("ANALYZE TABLE `EMPS` PARTITION (`X` = 'ab', `Y` = 'bc') COMPUTE STATISTICS");
        sql("analyze table emps compute statistics for column^s^")
                .fails("(?s).*Encountered \"<EOF>\" at line 1, column 49.\n.*");
        sql("analyze table emps compute statistics for columns a")
                .ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS FOR COLUMNS `A`");
        sql("analyze table emps compute statistics for columns a, b")
                .ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS FOR COLUMNS `A`, `B`");
        sql("analyze table emps compute statistics for all columns")
                .ok("ANALYZE TABLE `EMPS` COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x, y) compute statistics for all columns")
                .ok("ANALYZE TABLE `EMPS` PARTITION (`X`, `Y`) COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x='ab', y) compute statistics for all columns")
                .ok(
                        "ANALYZE TABLE `EMPS` PARTITION (`X` = 'ab', `Y`) COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x, y='cd') compute statistics for all columns")
                .ok(
                        "ANALYZE TABLE `EMPS` PARTITION (`X`, `Y` = 'cd') COMPUTE STATISTICS FOR ALL COLUMNS");
        sql("analyze table emps partition(x=^,^ y) compute statistics for all columns")
                .fails("(?s).*Encountered \"\\,\" at line 1, column 32.\n.*");
    }

    @Test
    void testCreateTableAsSelectWithoutOptions() {
        sql("CREATE TABLE t AS SELECT * FROM b").ok("CREATE TABLE `T`\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testCreateTableAsSelectWithOptions() {
        sql("CREATE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                .ok("CREATE TABLE `T` WITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `B`");
    }

    @Test
    void testCreateTableAsSelectWithCreateTableLike() {
        sql("CREATE TABLE t (col1 string) WITH ('test' = 'zm') like b ^AS^ SELECT col1 FROM b")
                .fails("(?s).*Encountered \"AS\" at line 1, column 58.*");
    }

    @Test
    void testCreateTableAsSelectWithTmpTable() {
        sql("CREATE TEMPORARY TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE TABLE AS SELECT syntax does not support to create temporary table yet."));
    }

    @Test
    void testCreateTableAsSelectWithExplicitColumns() {
        sql("CREATE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE TABLE AS SELECT syntax does not support to specify explicit columns yet."));
    }

    @Test
    void testCreateTableAsSelectWithWatermark() {
        sql("CREATE TABLE t (watermark FOR ts AS ts - interval '3' second) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE TABLE AS SELECT syntax does not support to specify explicit watermark yet."));
    }

    @Test
    void testCreateTableAsSelectWithConstraints() {
        sql("CREATE TABLE t (PRIMARY KEY (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls "
                                                + "if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        sql("CREATE TABLE t (PRIMARY KEY (col1), PRIMARY KEY (col2) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("Duplicate primary key definition"));

        sql("CREATE TABLE t (UNIQUE (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));
    }

    @Test
    void testCreateTableAsSelectWithPartitionKey() {
        sql("CREATE TABLE t PARTITIONED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE TABLE AS SELECT syntax does not support to create partitioned table yet."));
    }

    @Test
    void testReplaceTableAsSelect() {
        // test replace table as select without options
        sql("REPLACE TABLE t AS SELECT * FROM b").ok("REPLACE TABLE `T`\nAS\nSELECT *\nFROM `B`");

        // test replace table as select with options
        sql("REPLACE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                .ok("REPLACE TABLE `T` WITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `B`");

        // test replace table as select with tmp table
        sql("REPLACE TEMPORARY TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "REPLACE TABLE AS SELECT syntax does not support temporary table yet."));

        // test replace table as select with explicit columns
        sql("REPLACE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "REPLACE TABLE AS SELECT syntax does not support to specify explicit columns yet."));

        // test replace table as select with watermark
        sql("REPLACE TABLE t (watermark FOR ts AS ts - interval '3' second) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "REPLACE TABLE AS SELECT syntax does not support to specify explicit watermark yet."));

        // test replace table as select with constraints
        sql("REPLACE TABLE t (PRIMARY KEY (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls "
                                                + "if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        sql("REPLACE TABLE t (PRIMARY KEY (col1), PRIMARY KEY (col2) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("Duplicate primary key definition"));

        sql("REPLACE TABLE t (UNIQUE (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        // test replace table as select with partition key
        sql("REPLACE TABLE t PARTITIONED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "REPLACE TABLE AS SELECT syntax does not support to create partitioned table yet."));
    }

    @Test
    void testCreateOrReplaceTableAsSelect() {
        // test create or replace table as select without options
        sql("CREATE OR REPLACE TABLE t AS SELECT * FROM b")
                .ok("CREATE OR REPLACE TABLE `T`\nAS\nSELECT *\nFROM `B`");

        // test create or replace table as select with options
        sql("CREATE OR REPLACE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                .ok(
                        "CREATE OR REPLACE TABLE `T` WITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `B`");

        // test create or replace table as select with create table like
        sql("CREATE OR REPLACE TABLE t (col1 string) WITH ('test' = 'zm') like b ^AS^ SELECT col1 FROM b")
                .fails("(?s).*Encountered \"AS\" at line 1, column 69.*");

        // test create or replace table as select with tmp table
        sql("CREATE OR REPLACE TEMPORARY TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support temporary table yet."));

        // test create or replace table as select with explicit columns
        sql("CREATE OR REPLACE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support to specify explicit columns yet."));

        // test create or replace table as select with watermark
        sql("CREATE OR REPLACE TABLE t (watermark FOR ts AS ts - interval '3' second) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support to specify explicit watermark yet."));
        // test create or replace table as select with constraints
        sql("CREATE OR REPLACE TABLE t (PRIMARY KEY (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls "
                                                + "if the constraint checks are performed on the incoming/outgoing data. "
                                                + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode"));

        sql("CREATE OR REPLACE TABLE t (PRIMARY KEY (col1), PRIMARY KEY (col2) NOT ENFORCED) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("Duplicate primary key definition"));

        sql("CREATE OR REPLACE TABLE t (UNIQUE (col1)) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(new ValidationMatcher().fails("UNIQUE constraint is not supported yet"));

        // test create or replace table as select with partition key
        sql("CREATE OR REPLACE TABLE t PARTITIONED BY(col1) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "CREATE OR REPLACE TABLE AS SELECT syntax does not support to create partitioned table yet."));
    }

    @Test
    void testShowJobs() {
        sql("show jobs").ok("SHOW JOBS");
    }

    @Test
    void testStopJob() {
        sql("STOP JOB 'myjob'").ok("STOP JOB 'myjob'");
        sql("STOP JOB 'myjob' WITH SAVEPOINT").ok("STOP JOB 'myjob' WITH SAVEPOINT");
        sql("STOP JOB 'myjob' WITH SAVEPOINT WITH DRAIN")
                .ok("STOP JOB 'myjob' WITH SAVEPOINT WITH DRAIN");
        sql("STOP JOB 'myjob' ^WITH DRAIN^")
                .fails("WITH DRAIN could only be used after WITH SAVEPOINT.");
        sql("STOP JOB 'myjob' ^WITH DRAIN^ WITH SAVEPOINT")
                .fails("WITH DRAIN could only be used after WITH SAVEPOINT.");
    }

    @Test
    void testTruncateTable() {
        sql("truncate table t1").ok("TRUNCATE TABLE `T1`");
    }

    public static BaseMatcher<SqlNode> validated(String validatedSql) {
        return new TypeSafeDiagnosingMatcher<SqlNode>() {
            @Override
            protected boolean matchesSafely(SqlNode item, Description mismatchDescription) {
                if (item instanceof ExtendedSqlNode) {
                    try {
                        ((ExtendedSqlNode) item).validate();
                    } catch (SqlValidateException e) {
                        mismatchDescription.appendText(
                                "Could not validate the node. Exception: \n");
                        mismatchDescription.appendValue(e);
                    }

                    String actual = item.toSqlString(null, true).getSql();
                    return actual.equals(validatedSql);
                }
                mismatchDescription.appendText(
                        "This matcher can be applied only to ExtendedSqlNode.");
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                        "The validated node string representation should be equal to: \n");
                description.appendText(validatedSql);
            }
        };
    }

    /** Matcher that invokes the #validate() of the {@link ExtendedSqlNode} instance. * */
    private static class ValidationMatcher extends BaseMatcher<SqlNode> {
        private String expectedColumnSql;
        private String failMsg;

        public ValidationMatcher expectColumnSql(String s) {
            this.expectedColumnSql = s;
            return this;
        }

        public ValidationMatcher fails(String failMsg) {
            this.failMsg = failMsg;
            return this;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("test");
        }

        @Override
        public boolean matches(Object item) {
            if (item instanceof ExtendedSqlNode) {
                ExtendedSqlNode createTable = (ExtendedSqlNode) item;
                if (failMsg != null) {
                    try {
                        createTable.validate();
                        fail("expected exception");
                    } catch (SqlValidateException e) {
                        assertThat(e).hasMessage(failMsg);
                    }
                }
                if (expectedColumnSql != null && item instanceof SqlCreateTable) {
                    assertThat(((SqlCreateTable) createTable).getColumnSqlString())
                            .isEqualTo(expectedColumnSql);
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
