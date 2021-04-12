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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserContext;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for the AST parser. */
public class HiveASTParserTest {

    private static final HiveConf hiveConf = HiveTestUtils.createHiveConf();

    @Test
    public void testDatabase() throws Exception {
        assertDDLType(HiveASTParser.TOK_SHOWDATABASES, "show databases");
        assertDDLType(HiveASTParser.TOK_SWITCHDATABASE, "use db1");
        assertDDLType(
                HiveASTParser.TOK_CREATEDATABASE,
                "create database db1",
                "create database db1 comment 'comment db1' location '/path/to/db1'",
                "create database db1 with dbproperties ('k1'='v1','k2'='v2')");
        assertDDLType(
                HiveASTParser.TOK_ALTERDATABASE_PROPERTIES,
                "alter database db1 set dbproperties('k1'='v1')");
        assertDDLType(
                HiveASTParser.TOK_ALTERDATABASE_LOCATION,
                "alter database db1 set location '/new/path'");
        assertDDLType(
                HiveASTParser.TOK_ALTERDATABASE_OWNER,
                "alter database db1 set owner user user1",
                "alter database db1 set owner role role1");
        assertDDLType(
                HiveASTParser.TOK_DROPDATABASE, "drop schema db1", "drop database db1 cascade");
        assertDDLType(
                HiveASTParser.TOK_DESCDATABASE,
                "describe schema db1",
                "describe database extended db1");
    }

    @Test
    public void testTable() throws Exception {
        assertDDLType(HiveASTParser.TOK_SHOWTABLES, "show tables");
        assertDDLType(
                HiveASTParser.TOK_DESCTABLE,
                "describe tbl",
                "describe extended tbl",
                "describe formatted tbl");
        assertDDLType(
                HiveASTParser.TOK_CREATETABLE,
                "create table tbl (x int) row format delimited fields terminated by ',' escaped by '\\\\' "
                        + "collection items terminated by ',' map keys terminated by ':' lines terminated by '\\n' "
                        + "null defined as 'null' location '/path/to/table'",
                "create table tbl (x double) stored as orc tblproperties ('k1'='v1')",
                "create table tbl (x decimal(5,2)) row format serde 'serde.class.name' with serdeproperties ('serde.k1'='v1')",
                "create table tbl (x date) row format delimited fields terminated by '\\u0001' "
                        + "stored as inputformat 'input.format.class' outputformat 'output.format.class'",
                "create table tbl (x struct<f1:timestamp,f2:int>) partitioned by (p1 string,p2 bigint) stored as rcfile",
                "create external table tbl (x map<timestamp,array<timestamp>>) location '/table/path'",
                "create temporary table tbl (x varchar(50)) partitioned by (p timestamp)",
                "create table if not exists tbl (x int)",
                "create table tbl (x int not null enable rely, y string not null disable novalidate norely)",
                "create table tbl (x int,y timestamp not null,z string,primary key (x,z) disable novalidate rely)",
                "create table tbl (x binary,y date,z string,constraint pk_cons primary key(x))");
        assertDDLType(
                HiveASTParser.TOK_DROPTABLE, "drop table tbl", "drop table if exists cat.tbl");
        assertDDLType(
                HiveASTParser.TOK_ALTERTABLE,
                "alter table tbl rename to tbl1",
                "alter table tbl set serde 'serde.class' with serdeproperties ('field.delim'='\\u0001')",
                "alter table tbl set serdeproperties('line.delim'='\\n')",
                "alter table tbl set location '/new/table/path'",
                "alter table tbl partition (p1=1,p2='a') set location '/new/partition/location'",
                "alter table tbl partition (p=1) rename to partition (p=2)",
                "alter table tbl set tblproperties('k1'='v1','k2'='v2')",
                "alter table tbl set fileformat rcfile",
                "alter table tbl partition (p=1) set fileformat sequencefile",
                "alter table tbl change c c1 struct<f0:timestamp,f1:array<char(5)>> restrict",
                "alter table tbl change column c c decimal(5,2) comment 'new comment' first cascade",
                "alter table tbl add columns (a float,b timestamp,c binary) cascade",
                "alter table tbl replace columns (a char(100),b tinyint comment 'tiny comment',c smallint) restrict",
                "alter table tbl add partition (p1=1,p2='a') location '/part1/location'",
                "alter table tbl add if not exists partition (p=1) partition (p=2) location '/part2/location'",
                "alter table tbl drop if exists partition (p=1)",
                "alter table tbl drop partition (p1='a',p2=1), partition(p1='b',p2=2)");
        assertDDLType(
                HiveASTParser.TOK_SHOWPARTITIONS,
                "show partitions tbl",
                "show partitions tbl partition (p=1)");
    }

    @Test
    public void testView() throws Exception {
        assertDDLType(
                HiveASTParser.TOK_CREATEVIEW,
                "create view db1.v1 as select x,y from tbl",
                "create view if not exists v1 (c1,c2) as select * from tbl",
                "create view v1 comment 'v1 comment' tblproperties('k1'='v1','k2'='v2') as select * from tbl");
        assertDDLType(HiveASTParser.TOK_DROPVIEW, "drop view v1", "drop view if exists v1");
        assertDDLType(
                HiveASTParser.TOK_ALTERVIEW,
                "alter view v1 rename to v2",
                "alter view v1 set tblproperties ('k1'='v1')",
                "alter view v1 as select c1,c2 from tbl");
    }

    @Test
    public void testFunction() throws Exception {
        assertDDLType(
                HiveASTParser.TOK_CREATEFUNCTION,
                "create function func as 'class.name'",
                "create temporary function func as 'class.name'");
        assertDDLType(
                HiveASTParser.TOK_DROPFUNCTION,
                "drop function if exists func",
                "drop temporary function func");
        assertDDLType(HiveASTParser.TOK_SHOWFUNCTIONS, "show functions");
    }

    @Test
    public void testConstraints() throws Exception {
        assertDDLType(
                HiveASTParser.TOK_CREATETABLE,
                "create table foo (x int not null norely,y string not null disable validate)",
                "create table foo(x int,y int, primary key(x) enable rely)",
                "create table foo(x int,y decimal, constraint pk primary key (x,y))");
    }

    private void assertDDLType(int type, String... sqls) throws Exception {
        for (String sql : sqls) {
            HiveParserContext parserContext = new HiveParserContext(hiveConf);
            assertEquals(type, HiveASTParseUtils.parse(sql, parserContext).getType());
        }
    }
}
