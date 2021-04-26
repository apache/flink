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
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** FlinkSqlParserImpl tests. * */
public class FlinkSqlParserImplTest extends SqlParserTest {

    @Override
    protected SqlParserImplFactory parserImplFactory() {
        return FlinkSqlParserImpl.FACTORY;
    }

    @Test
    public void testShowCatalogs() {
        sql("show catalogs").ok("SHOW CATALOGS");
    }

    @Test
    public void testShowCurrentCatalog() {
        sql("show current catalog").ok("SHOW CURRENT CATALOG");
    }

    @Test
    public void testDescribeCatalog() {
        sql("describe catalog a").ok("DESCRIBE CATALOG `A`");
    }

    /**
     * Here we override the super method to avoid test error from `describe schema` supported in
     * original calcite.
     */
    @Ignore
    @Test
    public void testDescribeSchema() {}

    @Test
    public void testUseCatalog() {
        sql("use catalog a").ok("USE CATALOG `A`");
    }

    @Test
    public void testCreateCatalog() {
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
    public void testDropCatalog() {
        sql("drop catalog c1").ok("DROP CATALOG `C1`");
    }

    @Test
    public void testShowDataBases() {
        sql("show databases").ok("SHOW DATABASES");
    }

    @Test
    public void testShowCurrentDatabase() {
        sql("show current database").ok("SHOW CURRENT DATABASE");
    }

    @Test
    public void testUseDataBase() {
        sql("use default_db").ok("USE `DEFAULT_DB`");
        sql("use defaultCatalog.default_db").ok("USE `DEFAULTCATALOG`.`DEFAULT_DB`");
    }

    @Test
    public void testCreateDatabase() {
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
    public void testDropDatabase() {
        sql("drop database db1").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database catalog1.db1").ok("DROP DATABASE `CATALOG1`.`DB1` RESTRICT");
        sql("drop database db1 RESTRICT").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database db1 CASCADE").ok("DROP DATABASE `DB1` CASCADE");
    }

    @Test
    public void testAlterDatabase() {
        final String sql = "alter database db1 set ('key1' = 'value1','key2.a' = 'value2.a')";
        final String expected =
                "ALTER DATABASE `DB1` SET (\n"
                        + "  'key1' = 'value1',\n"
                        + "  'key2.a' = 'value2.a'\n"
                        + ")";
        sql(sql).ok(expected);
    }

    @Test
    public void testDescribeDatabase() {
        sql("describe database db1").ok("DESCRIBE DATABASE `DB1`");
        sql("describe database catlog1.db1").ok("DESCRIBE DATABASE `CATLOG1`.`DB1`");
        sql("describe database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");
    }

    @Test
    public void testAlterFunction() {
        sql("alter function function1 as 'org.apache.fink.function.function1'")
                .ok("ALTER FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("alter temporary function function1 as 'org.apache.fink.function.function1'")
                .ok("ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("alter temporary function function1 as 'org.apache.fink.function.function1' language scala")
                .ok(
                        "ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE SCALA");

        sql("alter temporary system function function1 as 'org.apache.fink.function.function1'")
                .ok(
                        "ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("alter temporary system function function1 as 'org.apache.fink.function.function1' language java")
                .ok(
                        "ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE JAVA");
    }

    @Test
    public void testShowFuntions() {
        sql("show functions").ok("SHOW FUNCTIONS");
        sql("show functions db1").ok("SHOW FUNCTIONS `DB1`");
        sql("show functions catalog1.db1").ok("SHOW FUNCTIONS `CATALOG1`.`DB1`");
    }

    @Test
    public void testShowTables() {
        sql("show tables").ok("SHOW TABLES");
    }

    @Test
    public void testDescribeTable() {
        sql("describe tbl").ok("DESCRIBE `TBL`");
        sql("describe catlog1.db1.tbl").ok("DESCRIBE `CATLOG1`.`DB1`.`TBL`");
        sql("describe extended db1").ok("DESCRIBE EXTENDED `DB1`");
    }

    /**
     * Here we override the super method to avoid test error from `describe statement` supported in
     * original calcite.
     */
    @Ignore
    @Test
    public void testDescribeStatement() {}

    @Test
    public void testAlterTable() {
        sql("alter table t1 rename to t2").ok("ALTER TABLE `T1` RENAME TO `T2`");
        sql("alter table c1.d1.t1 rename to t2").ok("ALTER TABLE `C1`.`D1`.`T1` RENAME TO `T2`");
        final String sql0 = "alter table t1 set ('key1'='value1')";
        final String expected0 = "ALTER TABLE `T1` SET (\n" + "  'key1' = 'value1'\n" + ")";
        sql(sql0).ok(expected0);
        final String sql1 = "alter table t1 " + "add constraint ct1 primary key(a, b) not enforced";
        final String expected1 =
                "ALTER TABLE `T1` " + "ADD CONSTRAINT `CT1` PRIMARY KEY (`A`, `B`) NOT ENFORCED";
        sql(sql1).ok(expected1);
        final String sql2 = "alter table t1 " + "add unique(a, b)";
        final String expected2 = "ALTER TABLE `T1` " + "ADD UNIQUE (`A`, `B`)";
        sql(sql2).ok(expected2);
        final String sql3 = "alter table t1 drop constraint ct1";
        final String expected3 = "ALTER TABLE `T1` DROP CONSTRAINT `CT1`";
        sql(sql3).ok(expected3);
    }

    @Test
    public void testCreateTable() {
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
    public void testCreateTableIfNotExists() {
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
    public void testCreateTableWithComment() {
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
    public void testCreateTableWithCommentOnComputedColumn() {
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
    public void testTableConstraints() {
        final String sql =
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
        final String expected =
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
        sql(sql).ok(expected);
    }

    @Test
    public void testTableConstraintsValidated() {
        final String sql =
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
        final String expected =
                "CREATE TABLE `TBL1` (\n"
                        + "  `A` BIGINT NOT NULL,\n"
                        + "  `H` VARCHAR,\n"
                        + "  `G` AS (2 * (`A` + 1)),\n"
                        + "  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  `B` VARCHAR NOT NULL,\n"
                        + "  `PROC` AS `PROCTIME`(),\n"
                        + "  PRIMARY KEY (`A`, `B`),\n"
                        + "  UNIQUE (`H`, `G`)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'kafka.topic' = 'log.test'\n"
                        + ")";
        sql(sql).node(validated(expected));
    }

    @Test
    public void testTableConstraintsWithEnforcement() {
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
    public void testDuplicatePk() {
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
    public void testCreateTableWithWatermark() {
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
    public void testCreateTableWithWatermarkOnComputedColumn() {
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
    public void testCreateTableWithWatermarkOnNestedField() {
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
    public void testCreateTableWithMultipleWatermark() {
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
    public void testCreateTableWithQueryWatermarkExpression() {
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
    public void testCreateTableWithComplexType() {
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
    public void testCreateTableWithNestedComplexType() {
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
    public void testCreateTableWithUserDefinedType() {
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
    public void testInvalidComputedColumn() {
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
    public void testColumnSqlString() {
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
    public void testCreateTableWithMinusInOptionKey() {
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
    public void testCreateTableWithOptionKeyAsIdentifier() {
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
    public void testCreateTableWithLikeClause() {
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
    public void testCreateTemporaryTable() {
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
    public void testCreateTableWithNoColumns() {
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
    public void testDropTable() {
        final String sql = "DROP table catalog1.db1.tbl1";
        final String expected = "DROP TABLE `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    public void testDropIfExists() {
        final String sql = "DROP table IF EXISTS catalog1.db1.tbl1";
        final String expected = "DROP TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    public void testTemporaryDropTable() {
        final String sql = "DROP temporary table catalog1.db1.tbl1";
        final String expected = "DROP TEMPORARY TABLE `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    public void testDropTemporaryIfExists() {
        final String sql = "DROP temporary table IF EXISTS catalog1.db1.tbl1";
        final String expected = "DROP TEMPORARY TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`";
        sql(sql).ok(expected);
    }

    @Test
    public void testInsertPartitionSpecs() {
        final String sql1 = "insert into emps(x,y) partition (x='ab', y='bc') select * from emps";
        final String expected =
                "INSERT INTO `EMPS` (`X`, `Y`)\n"
                        + "PARTITION (`X` = 'ab', `Y` = 'bc')\n"
                        + "(SELECT *\n"
                        + "FROM `EMPS`)";
        sql(sql1).ok(expected);
        final String sql2 =
                "insert into emp (empno, ename, job, mgr, hiredate,\n"
                        + "  sal, comm, deptno, slacker)\n"
                        + "partition(empno='1', job='job')\n"
                        + "select 'nom', 0, timestamp '1970-01-01 00:00:00',\n"
                        + "  1, 1, 1, false\n"
                        + "from (values 'a')";
        sql(sql2)
                .ok(
                        "INSERT INTO `EMP` (`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,"
                                + " `COMM`, `DEPTNO`, `SLACKER`)\n"
                                + "PARTITION (`EMPNO` = '1', `JOB` = 'job')\n"
                                + "(SELECT 'nom', 0, TIMESTAMP '1970-01-01 00:00:00', 1, 1, 1, FALSE\n"
                                + "FROM (VALUES (ROW('a'))))");
        final String sql3 =
                "insert into empnullables (empno, ename)\n"
                        + "partition(ename='b')\n"
                        + "select 1 from (values 'a')";
        sql(sql3)
                .ok(
                        "INSERT INTO `EMPNULLABLES` (`EMPNO`, `ENAME`)\n"
                                + "PARTITION (`ENAME` = 'b')\n"
                                + "(SELECT 1\n"
                                + "FROM (VALUES (ROW('a'))))");
    }

    @Test
    public void testInsertCaseSensitivePartitionSpecs() {
        final String expected =
                "INSERT INTO `emps` (`x`, `y`)\n"
                        + "PARTITION (`x` = 'ab', `y` = 'bc')\n"
                        + "(SELECT *\n"
                        + "FROM `EMPS`)";
        sql("insert into \"emps\"(\"x\",\"y\") "
                        + "partition (\"x\"='ab', \"y\"='bc') select * from emps")
                .ok(expected);
    }

    @Test
    public void testInsertExtendedColumnAsStaticPartition1() {
        final String expected =
                "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) (`X`, `Y`)\n"
                        + "PARTITION (`Z` = 'ab')\n"
                        + "(SELECT *\n"
                        + "FROM `EMPS`)";
        sql("insert into emps(z boolean)(x,y) partition (z='ab') select * from emps").ok(expected);
    }

    @Test(expected = SqlParseException.class)
    public void testInsertExtendedColumnAsStaticPartition2() {
        sql("insert into emps(x, y, z boolean) partition (z='ab') select * from emps")
                .node(
                        new ValidationMatcher()
                                .fails(
                                        "Extended columns not allowed under the current SQL conformance level"));
    }

    @Test
    public void testInsertOverwrite() {
        // non-partitioned
        final String sql = "INSERT OVERWRITE myDB.myTbl SELECT * FROM src";
        final String expected = "INSERT OVERWRITE `MYDB`.`MYTBL`\n" + "(SELECT *\n" + "FROM `SRC`)";
        sql(sql).ok(expected);

        // partitioned
        final String sql1 = "INSERT OVERWRITE myTbl PARTITION (p1='v1',p2='v2') SELECT * FROM src";
        final String expected1 =
                "INSERT OVERWRITE `MYTBL`\n"
                        + "PARTITION (`P1` = 'v1', `P2` = 'v2')\n"
                        + "(SELECT *\n"
                        + "FROM `SRC`)";
        sql(sql1).ok(expected1);
    }

    @Test
    public void testInvalidUpsertOverwrite() {
        sql("UPSERT ^OVERWRITE^ myDB.myTbl SELECT * FROM src")
                .fails("OVERWRITE expression is only used with INSERT statement.");
    }

    @Test
    public void testCreateView() {
        final String sql = "create view v as select col1 from tbl";
        final String expected = "CREATE VIEW `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    public void testCreateViewWithInvalidFieldList() {
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
    public void testCreateViewWithComment() {
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
    public void testCreateViewWithFieldNames() {
        final String sql = "create view v(col1, col2) as select col3, col4 from tbl";
        final String expected =
                "CREATE VIEW `V` (`COL1`, `COL2`)\n"
                        + "AS\n"
                        + "SELECT `COL3`, `COL4`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    public void testCreateViewWithInvalidName() {
        final String sql = "create view v(^*^) COMMENT 'this is a view' as select col1 from tbl";
        final String expected = "(?s).*Encountered \"\\*\" at line 1, column 15.*";

        sql(sql).fails(expected);
    }

    @Test
    public void testCreateTemporaryView() {
        final String sql = "create temporary view v as select col1 from tbl";
        final String expected =
                "CREATE TEMPORARY VIEW `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    public void testCreateTemporaryViewIfNotExists() {
        final String sql = "create temporary view if not exists v as select col1 from tbl";
        final String expected =
                "CREATE TEMPORARY VIEW IF NOT EXISTS `V`\n"
                        + "AS\n"
                        + "SELECT `COL1`\n"
                        + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    public void testCreateViewIfNotExists() {
        final String sql = "create view if not exists v as select col1 from tbl";
        final String expected =
                "CREATE VIEW IF NOT EXISTS `V`\n" + "AS\n" + "SELECT `COL1`\n" + "FROM `TBL`";
        sql(sql).ok(expected);
    }

    @Test
    public void testDropView() {
        final String sql = "DROP VIEW IF EXISTS view_name";
        final String expected = "DROP VIEW IF EXISTS `VIEW_NAME`";
        sql(sql).ok(expected);
    }

    @Test
    public void testDropTemporaryView() {
        final String sql = "DROP TEMPORARY VIEW IF EXISTS view_name";
        final String expected = "DROP TEMPORARY VIEW IF EXISTS `VIEW_NAME`";
        sql(sql).ok(expected);
    }

    @Test
    public void testShowViews() {
        sql("show views").ok("SHOW VIEWS");
    }

    // Override the test because our ROW field type default is nullable,
    // which is different with Calcite.
    @Test
    public void testCastAsRowType() {
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
    public void testCreateTableWithNakedTableName() {
        String sql = "CREATE TABLE tbl1";
        sql(sql).node(new ValidationMatcher());
    }

    @Test
    public void testCreateViewWithEmptyFields() {
        String sql = "CREATE VIEW v1 AS SELECT 1";
        sql(sql).ok("CREATE VIEW `V1`\n" + "AS\n" + "SELECT 1");
    }

    @Test
    public void testCreateFunction() {
        sql("create function catalog1.db1.function1 as 'org.apache.fink.function.function1'")
                .ok(
                        "CREATE FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("create temporary function catalog1.db1.function1 as 'org.apache.fink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("create temporary function db1.function1 as 'org.apache.fink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("create temporary function function1 as 'org.apache.fink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("create temporary function if not exists catalog1.db1.function1 as 'org.apache.fink.function.function1'")
                .ok(
                        "CREATE TEMPORARY FUNCTION IF NOT EXISTS `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

        sql("create temporary function function1 as 'org.apache.fink.function.function1' language java")
                .ok(
                        "CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE JAVA");

        sql("create temporary system function  function1 as 'org.apache.fink.function.function1' language scala")
                .ok(
                        "CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE SCALA");

        // Temporary system function always belongs to the system and current session.
        sql("create temporary system function catalog1^.^db1.function1 as 'org.apache.fink.function.function1'")
                .fails("(?s).*Encountered \".\" at.*");

        // TODO: FLINK-17957: Forbidden syntax "CREATE SYSTEM FUNCTION" for sql parser
        sql("create system function function1 as 'org.apache.fink.function.function1'")
                .ok("CREATE SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");
    }

    @Test
    public void testDropTemporaryFunction() {
        sql("drop temporary function catalog1.db1.function1")
                .ok("DROP TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary system function catalog1.db1.function1")
                .ok("DROP TEMPORARY SYSTEM FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary function if exists catalog1.db1.function1")
                .ok("DROP TEMPORARY FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");

        sql("drop temporary system function if exists catalog1.db1.function1")
                .ok("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");
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
                        assertEquals(failMsg, e.getMessage());
                    }
                }
                if (expectedColumnSql != null && item instanceof SqlCreateTable) {
                    assertEquals(
                            expectedColumnSql, ((SqlCreateTable) createTable).getColumnSqlString());
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
