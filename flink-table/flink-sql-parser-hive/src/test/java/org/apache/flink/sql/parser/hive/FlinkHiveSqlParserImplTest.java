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

package org.apache.flink.sql.parser.hive;

import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl;

import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.junit.Ignore;
import org.junit.Test;

/** Tests for {@link FlinkHiveSqlParserImpl}. */
public class FlinkHiveSqlParserImplTest extends SqlParserTest {

    @Override
    protected SqlParserImplFactory parserImplFactory() {
        return FlinkHiveSqlParserImpl.FACTORY;
    }

    // ignore test methods that we don't support
    @Ignore
    @Test
    public void testDescribeStatement() {}

    @Ignore
    @Test
    public void testTableHintsInInsert() {}

    @Ignore
    @Test
    public void testDescribeSchema() {}

    @Test
    public void testShowDatabases() {
        sql("show databases").ok("SHOW DATABASES");
    }

    @Test
    public void testShowCurrentDatabase() {
        sql("show current database").ok("SHOW CURRENT DATABASE");
    }

    @Test
    public void testUseDatabase() {
        // use database
        sql("use db1").ok("USE `DB1`");
    }

    @Test
    public void testCreateDatabase() {
        sql("create database db1").ok("CREATE DATABASE `DB1`");
        sql("create database db1 comment 'comment db1' location '/path/to/db1'")
                .ok(
                        "CREATE DATABASE `DB1`\n"
                                + "COMMENT 'comment db1'\n"
                                + "LOCATION '/path/to/db1'");
        sql("create database db1 with dbproperties ('k1'='v1','k2'='v2')")
                .ok(
                        "CREATE DATABASE `DB1` WITH DBPROPERTIES (\n"
                                + "  'k1' = 'v1',\n"
                                + "  'k2' = 'v2'\n"
                                + ")");
    }

    @Test
    public void testAlterDatabase() {
        sql("alter database db1 set dbproperties('k1'='v1')")
                .ok("ALTER DATABASE `DB1` SET DBPROPERTIES (\n" + "  'k1' = 'v1'\n" + ")");
        sql("alter database db1 set location '/new/path'")
                .ok("ALTER DATABASE `DB1` SET LOCATION '/new/path'");
        sql("alter database db1 set owner user user1")
                .ok("ALTER DATABASE `DB1` SET OWNER USER `USER1`");
        sql("alter database db1 set owner role role1")
                .ok("ALTER DATABASE `DB1` SET OWNER ROLE `ROLE1`");
    }

    @Test
    public void testDropDatabase() {
        sql("drop schema db1").ok("DROP DATABASE `DB1` RESTRICT");
        sql("drop database db1 cascade").ok("DROP DATABASE `DB1` CASCADE");
    }

    @Test
    public void testDescribeDatabase() {
        sql("describe schema db1").ok("DESCRIBE DATABASE `DB1`");
        sql("describe database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");

        sql("desc schema db1").ok("DESCRIBE DATABASE `DB1`");
        sql("desc database extended db1").ok("DESCRIBE DATABASE EXTENDED `DB1`");
    }

    @Test
    public void testShowTables() {
        // TODO: support SHOW TABLES IN 'db_name' 'regex_pattern'
        sql("show tables").ok("SHOW TABLES");
    }

    @Test
    public void testDescribeTable() {
        // TODO: support describe partition and columns
        sql("describe tbl").ok("DESCRIBE `TBL`");
        sql("describe extended tbl").ok("DESCRIBE EXTENDED `TBL`");
        sql("describe formatted tbl").ok("DESCRIBE FORMATTED `TBL`");

        sql("desc tbl").ok("DESCRIBE `TBL`");
        sql("desc extended tbl").ok("DESCRIBE EXTENDED `TBL`");
        sql("desc formatted tbl").ok("DESCRIBE FORMATTED `TBL`");
    }

    @Test
    public void testCreateTable() {
        sql("create table tbl (x int) row format delimited fields terminated by ',' escaped by '\\' "
                        + "collection items terminated by ',' map keys terminated by ':' lines terminated by '\n' "
                        + "null defined as 'null' location '/path/to/table'")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  INTEGER\n"
                                + ")\n"
                                + "ROW FORMAT DELIMITED\n"
                                + "  FIELDS TERMINATED BY ',' ESCAPED BY '\\'\n"
                                + "  COLLECTION ITEMS TERMINATED BY ','\n"
                                + "  MAP KEYS TERMINATED BY ':'\n"
                                + "  LINES TERMINATED BY '\n'\n"
                                + "  NULL DEFINED AS 'null'\n"
                                + "LOCATION '/path/to/table'");
        sql("create table tbl (x double) stored as orc tblproperties ('k1'='v1')")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  DOUBLE\n"
                                + ")\n"
                                + "STORED AS `ORC`\n"
                                + "TBLPROPERTIES (\n"
                                + "  'k1' = 'v1'\n"
                                + ")");
        sql("create table tbl (x decimal(5,2)) row format serde 'serde.class.name' with serdeproperties ('serde.k1'='v1')")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  DECIMAL(5, 2)\n"
                                + ")\n"
                                + "ROW FORMAT SERDE 'serde.class.name' WITH SERDEPROPERTIES (\n"
                                + "  'serde.k1' = 'v1'\n"
                                + ")");
        sql("create table tbl (x date) row format delimited fields terminated by '\u0001' "
                        + "stored as inputformat 'input.format.class' outputformat 'output.format.class'")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  DATE\n"
                                + ")\n"
                                + "ROW FORMAT DELIMITED\n"
                                + "  FIELDS TERMINATED BY u&'\\0001'\n"
                                + "STORED AS INPUTFORMAT 'input.format.class' OUTPUTFORMAT 'output.format.class'");
        sql("create table tbl (x struct<f1:timestamp,f2:int>) partitioned by (p1 string,p2 bigint) stored as rcfile")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  STRUCT< `F1` TIMESTAMP, `F2` INTEGER >\n"
                                + ")\n"
                                + "PARTITIONED BY (\n"
                                + "  `P1`  STRING,\n"
                                + "  `P2`  BIGINT\n"
                                + ")\n"
                                + "STORED AS `RCFILE`");
        sql("create external table tbl (x map<timestamp,array<timestamp>>) location '/table/path'")
                .ok(
                        "CREATE EXTERNAL TABLE `TBL` (\n"
                                + "  `X`  MAP< TIMESTAMP, ARRAY< TIMESTAMP > >\n"
                                + ")\n"
                                + "LOCATION '/table/path'");
        sql("create temporary table tbl (x varchar(50)) partitioned by (p timestamp)")
                .ok(
                        "CREATE TEMPORARY TABLE `TBL` (\n"
                                + "  `X`  VARCHAR(50)\n"
                                + ")\n"
                                + "PARTITIONED BY (\n"
                                + "  `P`  TIMESTAMP\n"
                                + ")");
        sql("create table tbl (v varchar)").fails("VARCHAR precision is mandatory");

        sql("create table if not exists tbl (x int)")
                .ok("CREATE TABLE IF NOT EXISTS `TBL` (\n" + "  `X`  INTEGER\n" + ")");
        // TODO: support CLUSTERED BY, SKEWED BY, STORED BY, col constraints
    }

    @Test
    public void testConstraints() {
        sql("create table tbl (x int not null enable rely, y string not null disable novalidate norely)")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  INTEGER NOT NULL ENABLE NOVALIDATE RELY,\n"
                                + "  `Y`  STRING NOT NULL DISABLE NOVALIDATE NORELY\n"
                                + ")");
        sql("create table tbl (x int,y timestamp not null,z string,primary key (x,z) disable novalidate rely)")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  INTEGER,\n"
                                + "  `Y`  TIMESTAMP NOT NULL ENABLE NOVALIDATE RELY,\n"
                                + "  `Z`  STRING,\n"
                                + "  PRIMARY KEY (`X`, `Z`) DISABLE NOVALIDATE RELY\n"
                                + ")");
        sql("create table tbl (x binary,y date,z string,constraint pk_cons primary key(x))")
                .ok(
                        "CREATE TABLE `TBL` (\n"
                                + "  `X`  BINARY,\n"
                                + "  `Y`  DATE,\n"
                                + "  `Z`  STRING,\n"
                                + "  CONSTRAINT `PK_CONS` PRIMARY KEY (`X`) ENABLE NOVALIDATE RELY\n"
                                + ")");
    }

    @Test
    public void testDropTable() {
        sql("drop table tbl").ok("DROP TABLE `TBL`");
        sql("drop table if exists cat.tbl").ok("DROP TABLE IF EXISTS `CAT`.`TBL`");
    }

    @Test
    public void testInsert() {
        sql("insert into tbl partition(p1=1,p2,p3) select * from src")
                .ok(
                        "INSERT INTO `TBL`\n"
                                + "PARTITION (`P1` = 1, `P2`, `P3`)\n"
                                + "(SELECT *\n"
                                + "FROM `SRC`)");
        sql("insert overwrite table tbl select * from src")
                .ok("INSERT OVERWRITE `TBL`\n" + "(SELECT *\n" + "FROM `SRC`)");
        sql("insert into table tbl(x,y) select * from src")
                .ok("INSERT INTO `TBL` (`X`, `Y`)\n" + "(SELECT *\n" + "FROM `SRC`)");
    }

    @Test
    public void testCreateFunction() {
        sql("create function func as 'class.name'").ok("CREATE FUNCTION `FUNC` AS 'class.name'");
        sql("create temporary function func as 'class.name'")
                .ok("CREATE TEMPORARY FUNCTION `FUNC` AS 'class.name'");
    }

    @Test
    public void testDropFunction() {
        sql("drop function if exists func").ok("DROP FUNCTION IF EXISTS `FUNC`");
        sql("drop temporary function func").ok("DROP TEMPORARY FUNCTION `FUNC`");
    }

    @Test
    public void testShowFunctions() {
        sql("show functions").ok("SHOW FUNCTIONS");
        sql("show user functions").ok("SHOW USER FUNCTIONS");
    }

    @Test
    public void testCreateCatalog() {
        sql("create catalog cat").ok("CREATE CATALOG `CAT`");
        sql("create catalog cat with ('k1'='v1')")
                .ok("CREATE CATALOG `CAT` WITH (\n" + "  'k1' = 'v1'\n" + ")");
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
    public void testUseCatalog() {
        sql("use catalog cat").ok("USE CATALOG `CAT`");
    }

    @Test
    public void testDescribeCatalog() {
        sql("describe catalog cat").ok("DESCRIBE CATALOG `CAT`");

        sql("desc catalog cat").ok("DESCRIBE CATALOG `CAT`");
    }

    @Test
    public void testAlterTableRename() {
        sql("alter table tbl rename to tbl1").ok("ALTER TABLE `TBL` RENAME TO `TBL1`");
    }

    @Test
    public void testAlterTableSerDe() {
        sql("alter table tbl set serde 'serde.class' with serdeproperties ('field.delim'='\u0001')")
                .ok(
                        "ALTER TABLE `TBL` SET SERDE 'serde.class' WITH SERDEPROPERTIES (\n"
                                + "  'field.delim' = u&'\\0001'\n"
                                + ")");
        sql("alter table tbl set serdeproperties('line.delim'='\n')")
                .ok("ALTER TABLE `TBL` SET SERDEPROPERTIES (\n" + "  'line.delim' = '\n'\n" + ")");
    }

    @Test
    public void testAlterTableLocation() {
        sql("alter table tbl set location '/new/table/path'")
                .ok("ALTER TABLE `TBL` SET LOCATION '/new/table/path'");
        sql("alter table tbl partition (p1=1,p2='a') set location '/new/partition/location'")
                .ok(
                        "ALTER TABLE `TBL` PARTITION (`P1` = 1, `P2` = 'a') SET LOCATION '/new/partition/location'");
    }

    // TODO: support ALTER CLUSTERED BY, SKEWED, STORED AS DIRECTORIES, column constraints

    @Test
    public void testAlterPartitionRename() {
        sql("alter table tbl partition (p=1) rename to partition (p=2)")
                .ok(
                        "ALTER TABLE `TBL` PARTITION (`P` = 1)\n"
                                + "RENAME TO\n"
                                + "PARTITION (`P` = 2)");
    }

    @Test
    public void testAlterTableProperties() {
        sql("alter table tbl set tblproperties('k1'='v1','k2'='v2')")
                .ok(
                        "ALTER TABLE `TBL` SET TBLPROPERTIES (\n"
                                + "  'k1' = 'v1',\n"
                                + "  'k2' = 'v2'\n"
                                + ")");
    }

    // TODO: support EXCHANGE PARTITION, RECOVER PARTITIONS

    // TODO: support (UN)ARCHIVE PARTITION

    @Test
    public void testAlterFileFormat() {
        sql("alter table tbl set fileformat rcfile")
                .ok("ALTER TABLE `TBL` SET FILEFORMAT `RCFILE`");
        sql("alter table tbl partition (p=1) set fileformat sequencefile")
                .ok("ALTER TABLE `TBL` PARTITION (`P` = 1) SET FILEFORMAT `SEQUENCEFILE`");
    }

    // TODO: support ALTER TABLE/PARTITION TOUCH, PROTECTION, COMPACT, CONCATENATE, UPDATE COLUMNS

    @Test
    public void testChangeColumn() {
        sql("alter table tbl change c c1 struct<f0:timestamp,f1:array<char(5)>> restrict")
                .ok(
                        "ALTER TABLE `TBL` CHANGE COLUMN `C` `C1` STRUCT< `F0` TIMESTAMP, `F1` ARRAY< CHAR(5) > > RESTRICT");
        sql("alter table tbl change column c c decimal(5,2) comment 'new comment' first cascade")
                .ok(
                        "ALTER TABLE `TBL` CHANGE COLUMN `C` `C` DECIMAL(5, 2) COMMENT 'new comment' FIRST CASCADE");
    }

    @Test
    public void testAddReplaceColumn() {
        sql("alter table tbl add columns (a float,b timestamp,c binary) cascade")
                .ok(
                        "ALTER TABLE `TBL` ADD COLUMNS (\n"
                                + "  `A` FLOAT,\n"
                                + "  `B` TIMESTAMP,\n"
                                + "  `C` BINARY\n"
                                + ") CASCADE");
        sql("alter table tbl replace columns (a char(100),b tinyint comment 'tiny comment',c smallint) restrict")
                .ok(
                        "ALTER TABLE `TBL` REPLACE COLUMNS (\n"
                                + "  `A` CHAR(100),\n"
                                + "  `B` TINYINT COMMENT 'tiny comment',\n"
                                + "  `C` SMALLINT\n"
                                + ") RESTRICT");
    }

    @Test
    public void testCreateView() {
        sql("create view db1.v1 as select x,y from tbl")
                .ok("CREATE VIEW `DB1`.`V1`\n" + "AS\n" + "SELECT `X`, `Y`\n" + "FROM `TBL`");
        sql("create view if not exists v1 (c1,c2) as select * from tbl")
                .ok(
                        "CREATE VIEW IF NOT EXISTS `V1` (`C1`, `C2`)\n"
                                + "AS\n"
                                + "SELECT *\n"
                                + "FROM `TBL`");
        sql("create view v1 comment 'v1 comment' tblproperties('k1'='v1','k2'='v2') as select * from tbl")
                .ok(
                        "CREATE VIEW `V1`\n"
                                + "COMMENT 'v1 comment'\n"
                                + "TBLPROPERTIES (\n"
                                + "  'k1' = 'v1',\n"
                                + "  'k2' = 'v2'\n"
                                + ")\n"
                                + "AS\n"
                                + "SELECT *\n"
                                + "FROM `TBL`");
        // TODO: support column comments
    }

    @Test
    public void testDropView() {
        sql("drop view v1").ok("DROP VIEW `V1`");
        sql("drop view if exists v1").ok("DROP VIEW IF EXISTS `V1`");
    }

    @Test
    public void testAlterView() {
        sql("alter view v1 rename to v2").ok("ALTER VIEW `V1` RENAME TO `V2`");
        sql("alter view v1 set tblproperties ('k1'='v1')")
                .ok("ALTER VIEW `V1` SET TBLPROPERTIES (\n" + "  'k1' = 'v1'\n" + ")");
        sql("alter view v1 as select c1,c2 from tbl")
                .ok("ALTER VIEW `V1`\n" + "AS\n" + "SELECT `C1`, `C2`\n" + "FROM `TBL`");
    }

    @Test
    public void testAddPartition() {
        sql("alter table tbl add partition (p1=1,p2='a') location '/part1/location'")
                .ok(
                        "ALTER TABLE `TBL`\n"
                                + "ADD\n"
                                + "PARTITION (`P1` = 1, `P2` = 'a') LOCATION '/part1/location'");
        sql("alter table tbl add if not exists partition (p=1) partition (p=2) location '/part2/location'")
                .ok(
                        "ALTER TABLE `TBL`\n"
                                + "ADD IF NOT EXISTS\n"
                                + "PARTITION (`P` = 1)\n"
                                + "PARTITION (`P` = 2) LOCATION '/part2/location'");
    }

    @Test
    public void testDropPartition() {
        sql("alter table tbl drop if exists partition (p=1)")
                .ok("ALTER TABLE `TBL`\n" + "DROP IF EXISTS\n" + "PARTITION (`P` = 1)");
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
        // TODO: support IGNORE PROTECTION, PURGE
    }

    @Test
    public void testShowPartitions() {
        sql("show partitions tbl").ok("SHOW PARTITIONS `TBL`");
        sql("show partitions tbl partition (p=1)").ok("SHOW PARTITIONS `TBL` PARTITION (`P` = 1)");
    }

    @Test
    public void testLoadModule() {
        sql("load module hive").ok("LOAD MODULE `HIVE`");

        sql("load module hive with ('hive-version' = '3.1.2')")
                .ok("LOAD MODULE `HIVE` WITH (\n  'hive-version' = '3.1.2'\n)");
    }

    @Test
    public void testUnloadModule() {
        sql("unload module hive").ok("UNLOAD MODULE `HIVE`");
    }

    @Test
    public void testUseModules() {
        sql("use modules hive").ok("USE MODULES `HIVE`");

        sql("use modules x, y, z").ok("USE MODULES `X`, `Y`, `Z`");

        sql("use modules x^,^").fails("(?s).*Encountered \"<EOF>\" at line 1, column 14.\n.*");

        sql("use modules ^'hive'^")
                .fails("(?s).*Encountered \"\\\\'hive\\\\'\" at line 1, column 13.\n.*");
    }

    @Test
    public void testShowModules() {
        sql("show modules").ok("SHOW MODULES");

        sql("show full modules").ok("SHOW FULL MODULES");
    }

    @Test
    public void testExplain() {
        String sql = "explain plan for select * from emps";
        String expected = "EXPLAIN SELECT *\n" + "FROM `EMPS`";
        this.sql(sql).ok(expected);
    }

    @Test
    public void testExplainJsonFormat() {
        // Unsupported feature. Escape the test.
    }

    @Test
    public void testExplainWithImpl() {
        // Unsupported feature. Escape the test.
    }

    @Test
    public void testExplainWithoutImpl() {
        // Unsupported feature. Escape the test.
    }

    @Test
    public void testExplainWithType() {
        // Unsupported feature. Escape the test.
    }

    @Test
    public void testExplainAsXml() {
        // Unsupported feature. Escape the test.
    }

    @Test
    public void testExplainAsJson() {
        // TODO: FLINK-20562
    }

    @Test
    public void testExplainInsert() {
        String expected = "EXPLAIN INSERT INTO `EMPS1`\n" + "(SELECT *\n" + "FROM `EMPS2`)";
        this.sql("explain plan for insert into emps1 select * from emps2").ok(expected);
    }

    @Test
    public void testExplainUpsert() {
        String sql = "explain plan for upsert into emps1 values (1, 2)";
        String expected = "EXPLAIN UPSERT INTO `EMPS1`\n" + "VALUES (ROW(1, 2))";
        this.sql(sql).ok(expected);
    }
}
