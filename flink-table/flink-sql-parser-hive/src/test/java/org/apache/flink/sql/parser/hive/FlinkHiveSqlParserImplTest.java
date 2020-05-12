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
import org.junit.Test;

/**
 * Tests for {@link FlinkHiveSqlParserImpl}.
 */
public class FlinkHiveSqlParserImplTest extends SqlParserTest {

	@Override
	protected SqlParserImplFactory parserImplFactory() {
		return FlinkHiveSqlParserImpl.FACTORY;
	}

	// overrides test methods that we don't support
	@Override
	public void testDescribeStatement() {
	}

	@Override
	public void testTableHintsInInsert() {
	}

	@Override
	public void testDescribeSchema() {
	}

	@Test
	public void testShowDatabases() {
		sql("show databases").ok("SHOW DATABASES");
	}

	@Test
	public void testUseDatabase() {
		// use database
		sql("use db1").ok("USE `DB1`");
	}

	@Test
	public void testCreateDatabase() {
		sql("create database db1")
				.ok("CREATE DATABASE `DB1`");
		sql("create database db1 comment 'comment db1' location '/path/to/db1'")
				.ok("CREATE DATABASE `DB1`\n" +
						"COMMENT 'comment db1'\n" +
						"LOCATION '/path/to/db1'");
		sql("create database db1 with dbproperties ('k1'='v1','k2'='v2')")
				.ok("CREATE DATABASE `DB1` WITH DBPROPERTIES (\n" +
						"  'k1' = 'v1',\n" +
						"  'k2' = 'v2'\n" +
						")");
	}

	@Test
	public void testAlterDatabase() {
		sql("alter database db1 set dbproperties('k1'='v1')")
				.ok("ALTER DATABASE `DB1` SET DBPROPERTIES (\n" +
						"  'k1' = 'v1'\n" +
						")");
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
	}

	@Test
	public void testCreateTable() {
		sql("create table tbl (x int) row format delimited fields terminated by ',' escaped by '\\' " +
				"collection items terminated by ',' map keys terminated by ':' lines terminated by '\n' " +
				"null defined as 'null' location '/path/to/table'")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  INTEGER\n" +
						")\n" +
						"ROW FORMAT DELIMITED\n" +
						"  FIELDS TERMINATED BY ',' ESCAPED BY '\\'\n" +
						"  COLLECTION ITEMS TERMINATED BY ','\n" +
						"  MAP KEYS TERMINATED BY ':'\n" +
						"  LINES TERMINATED BY '\n'\n" +
						"  NULL DEFINED AS 'null'\n" +
						"LOCATION '/path/to/table'");
		sql("create table tbl (x double) stored as orc tblproperties ('k1'='v1')")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  DOUBLE\n" +
						")\n" +
						"STORED AS `ORC`\n" +
						"TBLPROPERTIES (\n" +
						"  'k1' = 'v1'\n" +
						")");
		sql("create table tbl (x decimal(5,2)) row format serde 'serde.class.name' with serdeproperties ('serde.k1'='v1')")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  DECIMAL(5, 2)\n" +
						")\n" +
						"ROW FORMAT SERDE 'serde.class.name' WITH SERDEPROPERTIES (\n" +
						"  'serde.k1' = 'v1'\n" +
						")");
		sql("create table tbl (x date) row format delimited fields terminated by '\u0001' " +
				"stored as inputformat 'input.format.class' outputformat 'output.format.class'")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  DATE\n" +
						")\n" +
						"ROW FORMAT DELIMITED\n" +
						"  FIELDS TERMINATED BY u&'\\0001'\n" +
						"STORED AS INPUTFORMAT 'input.format.class' OUTPUTFORMAT 'output.format.class'");
		sql("create table tbl (x struct<f1:timestamp,f2:int>) partitioned by (p1 string,p2 bigint) stored as rcfile")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  STRUCT< `F1` TIMESTAMP, `F2` INTEGER >\n" +
						")\n" +
						"PARTITIONED BY (\n" +
						"  `P1`  STRING,\n" +
						"  `P2`  BIGINT\n" +
						")\n" +
						"STORED AS `RCFILE`");
		sql("create external table tbl (x map<timestamp,array<timestamp>>) location '/table/path'")
				.ok("CREATE EXTERNAL TABLE `TBL` (\n" +
						"  `X`  MAP< TIMESTAMP, ARRAY< TIMESTAMP > >\n" +
						")\n" +
						"LOCATION '/table/path'");
		sql("create temporary table tbl (x varchar(50)) partitioned by (p timestamp)")
				.ok("CREATE TEMPORARY TABLE `TBL` (\n" +
						"  `X`  VARCHAR(50)\n" +
						")\n" +
						"PARTITIONED BY (\n" +
						"  `P`  TIMESTAMP\n" +
						")");
		sql("create table tbl (v varchar)").fails("VARCHAR precision is mandatory");
		// TODO: support CLUSTERED BY, SKEWED BY, STORED BY, col constraints
	}

	@Test
	public void testConstraints() {
		sql("create table tbl (x int not null enable rely, y string not null disable novalidate norely)")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  INTEGER NOT NULL ENABLE NOVALIDATE RELY,\n" +
						"  `Y`  STRING NOT NULL DISABLE NOVALIDATE NORELY\n" +
						")");
		sql("create table tbl (x int,y timestamp not null,z string,primary key (x,z) disable novalidate rely)")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  INTEGER,\n" +
						"  `Y`  TIMESTAMP NOT NULL ENABLE NOVALIDATE RELY,\n" +
						"  `Z`  STRING,\n" +
						"  PRIMARY KEY (`X`, `Z`) DISABLE NOVALIDATE RELY\n" +
						")");
		sql("create table tbl (x binary,y date,z string,constraint pk_cons primary key(x))")
				.ok("CREATE TABLE `TBL` (\n" +
						"  `X`  BINARY,\n" +
						"  `Y`  DATE,\n" +
						"  `Z`  STRING,\n" +
						"  CONSTRAINT `PK_CONS` PRIMARY KEY (`X`) ENABLE NOVALIDATE RELY\n" +
						")");
	}

	@Test
	public void testDropTable() {
		sql("drop table tbl").ok("DROP TABLE `TBL`");
		sql("drop table if exists cat.tbl").ok("DROP TABLE IF EXISTS `CAT`.`TBL`");
	}
}
