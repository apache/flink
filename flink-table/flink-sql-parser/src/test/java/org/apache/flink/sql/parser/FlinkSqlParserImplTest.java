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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import java.io.Reader;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/** FlinkSqlParserImpl tests. **/
public class FlinkSqlParserImplTest extends SqlParserTest {

	@Override
	protected SqlParserImplFactory parserImplFactory() {
		return FlinkSqlParserImpl.FACTORY;
	}

	protected SqlParser getSqlParser(Reader source,
			UnaryOperator<SqlParser.ConfigBuilder> transform) {
		return super.getSqlParser(source, transform);
	}

	@Test
	public void testShowCatalogs() {
		sql("show catalogs").ok("SHOW CATALOGS");
	}

	@Test
	public void testDescribeCatalog() {
		sql("describe catalog a").ok("DESCRIBE CATALOG `A`");
	}

	/**
	 * Here we override the super method to avoid test error from `describe schema` supported in original calcite.
	 */
	@Override
	public void testDescribeSchema() {
	}

	@Test
	public void testUseCatalog() {
		sql("use catalog a").ok("USE CATALOG `A`");
	}

	@Test
	public void testCreateCatalog() {
		check(
			"create catalog c1\n" +
				" WITH (\n" +
				"  'key1'='value1',\n" +
				"  'key2'='value2'\n" +
				" )\n",
			"CREATE CATALOG `C1` " +
				"WITH (\n" +
				"  'key1' = 'value1',\n" +
				"  'key2' = 'value2'\n" +
				")");
	}

	@Test
	public void testShowDataBases() {
		sql("show databases").ok("SHOW DATABASES");
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
		final String expected = "CREATE DATABASE `DB1`\n"
				+ "COMMENT 'test create database'";
		sql(sql).ok(expected);
		final String sql1 = "create database db1 comment 'test create database'"
				+ "with ( 'key1' = 'value1', 'key2.a' = 'value2.a')";
		final String expected1 = "CREATE DATABASE `DB1`\n"
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
		final String expected = "ALTER DATABASE `DB1` SET (\n"
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
				.ok("ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE SCALA");

		sql("alter temporary system function function1 as 'org.apache.fink.function.function1'")
				.ok("ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("alter temporary system function function1 as 'org.apache.fink.function.function1' language java")
				.ok("ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE JAVA");
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
	 * Here we override the super method to avoid test error from `describe statement` supported in original calcite.
	 */
	@Override
	public void testDescribeStatement() {
	}

	@Test
	public void testAlterTable() {
		sql("alter table t1 rename to t2").ok("ALTER TABLE `T1` RENAME TO `T2`");
		sql("alter table c1.d1.t1 rename to t2").ok("ALTER TABLE `C1`.`D1`.`T1` RENAME TO `T2`");
		final String sql = "alter table t1 set ('key1'='value1')";
		final String expected = "ALTER TABLE `T1` SET (\n"
				+ "  'key1' = 'value1'\n"
				+ ")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTable() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  h varchar, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  PRIMARY KEY (a, b)\n" +
				")\n" +
				"PARTITIONED BY (a, h)\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `H`  VARCHAR,\n" +
				"  `G` AS (2 * (`A` + 1)),\n" +
				"  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `B`  VARCHAR,\n" +
				"  `PROC` AS `PROCTIME`(),\n" +
				"  PRIMARY KEY (`A`, `B`)\n" +
				")\n" +
				"PARTITIONED BY (`A`, `H`)\n" +
				"WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithComment() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a bigint comment 'test column comment AAA.',\n" +
				"  h varchar, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  PRIMARY KEY (a, b)\n" +
				")\n" +
				"comment 'test table comment ABC.'\n" +
				"PARTITIONED BY (a, h)\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT  COMMENT 'test column comment AAA.',\n" +
				"  `H`  VARCHAR,\n" +
				"  `G` AS (2 * (`A` + 1)),\n" +
				"  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `B`  VARCHAR,\n" +
				"  `PROC` AS `PROCTIME`(),\n" +
				"  PRIMARY KEY (`A`, `B`)\n" +
				")\n" +
				"COMMENT 'test table comment ABC.'\n" +
				"PARTITIONED BY (`A`, `H`)\n" +
				"WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithPrimaryKeyAndUniqueKey() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a bigint comment 'test column comment AAA.',\n" +
				"  h varchar, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  PRIMARY KEY (a, b), \n" +
				"  UNIQUE (h, g)\n" +
				")\n" +
				"comment 'test table comment ABC.'\n" +
				"PARTITIONED BY (a, h)\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT  COMMENT 'test column comment AAA.',\n" +
				"  `H`  VARCHAR,\n" +
				"  `G` AS (2 * (`A` + 1)),\n" +
				"  `TS` AS `TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `B`  VARCHAR,\n" +
				"  `PROC` AS `PROCTIME`(),\n" +
				"  PRIMARY KEY (`A`, `B`),\n" +
				"  UNIQUE (`H`, `G`)\n" +
				")\n" +
				"COMMENT 'test table comment ABC.'\n" +
				"PARTITIONED BY (`A`, `H`)\n" +
				"WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithWatermark() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  ts timestamp(3),\n" +
			"  id varchar, \n" +
			"  watermark FOR ts AS ts - interval '3' second\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `TS`  TIMESTAMP(3),\n" +
				"  `ID`  VARCHAR,\n" +
				"  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithWatermarkOnComputedColumn() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  log_ts varchar,\n" +
			"  ts as to_timestamp(log_ts), \n" +
			"  WATERMARK FOR ts AS ts + interval '1' second\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `LOG_TS`  VARCHAR,\n" +
				"  `TS` AS `TO_TIMESTAMP`(`LOG_TS`),\n" +
				"  WATERMARK FOR `TS` AS (`TS` + INTERVAL '1' SECOND)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithWatermarkOnNestedField() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  f1 row<q1 bigint, q2 row<t1 timestamp, t2 varchar>, q3 boolean>,\n" +
				"  WATERMARK FOR f1.q2.t1 AS NOW()\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `F1`  ROW< `Q1` BIGINT, `Q2` ROW< `T1` TIMESTAMP, `T2` VARCHAR >, `Q3` BOOLEAN >,\n" +
				"  WATERMARK FOR `F1`.`Q2`.`T1` AS `NOW`()\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithInvalidWatermark() {
		String sql = "CREATE TABLE tbl1 (\n" +
			"  f1 row<q1 bigint, q2 varchar, q3 boolean>,\n" +
			"  WATERMARK FOR f1.q0 AS NOW()\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		sql(sql).node(new ValidationMatcher()
			.fails("The rowtime attribute field \"F1.Q0\" is not defined in columns, at line 3, column 17"));

	}

	@Test
	public void testCreateTableWithMultipleWatermark() {
		String sql = "CREATE TABLE tbl1 (\n" +
			"  f0 bigint,\n" +
			"  f1 varchar,\n" +
			"  f2 boolean,\n" +
			"  WATERMARK FOR f0 AS NOW(),\n" +
			"  ^WATERMARK^ FOR f1 AS NOW()\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		sql(sql)
			.fails("Multiple WATERMARK statements is not supported yet.");
	}

	@Test
	public void testCreateTableWithQueryWatermarkExpression() {
		String sql = "CREATE TABLE tbl1 (\n" +
			"  f0 bigint,\n" +
			"  f1 varchar,\n" +
			"  f2 boolean,\n" +
			"  WATERMARK FOR f0 AS ^(^SELECT f1 FROM tbl1)\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		sql(sql)
			.fails("Query expression encountered in illegal context");
	}

	@Test
	public void testCreateTableWithComplexType() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a ARRAY<bigint>, \n" +
				"  b MAP<int, varchar>,\n" +
				"  c ROW<cc0 int, cc1 float, cc2 varchar>,\n" +
				"  d MULTISET<varchar>,\n" +
				"  PRIMARY KEY (a, b) \n" +
				") with (\n" +
				"  'x' = 'y', \n" +
				"  'asd' = 'data'\n" +
				")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `A`  ARRAY< BIGINT >,\n" +
				"  `B`  MAP< INTEGER, VARCHAR >,\n" +
				"  `C`  ROW< `CC0` INTEGER, `CC1` FLOAT, `CC2` VARCHAR >,\n" +
				"  `D`  MULTISET< VARCHAR >,\n" +
				"  PRIMARY KEY (`A`, `B`)\n" +
				") WITH (\n" +
				"  'x' = 'y',\n" +
				"  'asd' = 'data'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithNestedComplexType() {
		final String sql = "CREATE TABLE tbl1 (\n" +
				"  a ARRAY<ARRAY<bigint>>, \n" +
				"  b MAP<MAP<int, varchar>, ARRAY<varchar>>,\n" +
				"  c ROW<cc0 ARRAY<int>, cc1 float, cc2 varchar>,\n" +
				"  d MULTISET<ARRAY<int>>,\n" +
				"  PRIMARY KEY (a, b) \n" +
				") with (\n" +
				"  'x' = 'y', \n" +
				"  'asd' = 'data'\n" +
				")\n";
		final String expected = "CREATE TABLE `TBL1` (\n" +
				"  `A`  ARRAY< ARRAY< BIGINT > >,\n" +
				"  `B`  MAP< MAP< INTEGER, VARCHAR >, ARRAY< VARCHAR > >,\n" +
				"  `C`  ROW< `CC0` ARRAY< INTEGER >, `CC1` FLOAT, `CC2` VARCHAR >,\n" +
				"  `D`  MULTISET< ARRAY< INTEGER > >,\n" +
				"  PRIMARY KEY (`A`, `B`)\n" +
				") WITH (\n" +
				"  'x' = 'y',\n" +
				"  'asd' = 'data'\n" +
				")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithUserDefinedType() {
		final String sql = "create table t(\n" +
			"  a catalog1.db1.MyType1,\n" +
			"  b db2.MyType2\n" +
			") with (\n" +
			"  'k1' = 'v1',\n" +
			"  'k2' = 'v2'\n" +
			")";
		final String expected = "CREATE TABLE `T` (\n" +
			"  `A`  `CATALOG1`.`DB1`.`MYTYPE1`,\n" +
			"  `B`  `DB2`.`MYTYPE2`\n" +
			") WITH (\n" +
			"  'k1' = 'v1',\n" +
			"  'k2' = 'v2'\n" +
			")";
		sql(sql).ok(expected);
	}

	@Test
	public void testInvalidComputedColumn() {
		final String sql0 = "CREATE TABLE t1 (\n" +
			"  a bigint, \n" +
			"  b varchar,\n" +
			"  toTimestamp^(^b, 'yyyy-MM-dd HH:mm:ss'), \n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  'x' = 'y', \n" +
			"  'asd' = 'data'\n" +
			")\n";
		final String expect0 = "(?s).*Encountered \"\\(\" at line 4, column 14.\n" +
			"Was expecting one of:\n" +
			"    \"AS\" ...\n" +
			"    \"STRING\" ...\n" +
			".*";
		sql(sql0).fails(expect0);
		// Sub-query computed column expression is forbidden.
		final String sql1 = "CREATE TABLE t1 (\n" +
			"  a bigint, \n" +
			"  b varchar,\n" +
			"  c as ^(^select max(d) from t2), \n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  'x' = 'y', \n" +
			"  'asd' = 'data'\n" +
			")\n";
		final String expect1 = "(?s).*Query expression encountered in illegal context.*";
		sql(sql1).fails(expect1);
	}

	@Test
	public void testColumnSqlString() {
		final String sql = "CREATE TABLE sls_stream (\n" +
			"  a bigint, \n" +
			"  f as a + 1, \n" +
			"  b varchar,\n" +
			"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
			"  proc as PROCTIME(),\n" +
			"  c int,\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  'x' = 'y', \n" +
			"  'asd' = 'data'\n" +
			")\n";
		final String expected = "`A`, (`A` + 1) AS `F`, `B`, "
			+ "`TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss') AS `TS`, "
			+ "`PROCTIME`() AS `PROC`, `C`";
		sql(sql).node(new ValidationMatcher()
			.expectColumnSql(expected));
	}

	@Test
	public void testCreateInvalidPartitionedTable() {
		final String sql = "create table sls_stream1(\n" +
			"  a bigint,\n" +
			"  b VARCHAR,\n" +
			"  PRIMARY KEY(a, b)\n" +
			") PARTITIONED BY (\n" +
			"  c,\n" +
			"  d\n" +
			") with ( 'x' = 'y', 'asd' = 'dada')";
		sql(sql).node(new ValidationMatcher()
			.fails("Partition column [C] not defined in columns, at line 6, column 3"));
	}

	@Test
	public void testCreateTableWithMinusInOptionKey() {
		final String sql = "create table source_table(\n" +
			"  a int,\n" +
			"  b bigint,\n" +
			"  c string\n" +
			") with (\n" +
			"  'a-b-c-d124' = 'ab',\n" +
			"  'a.b.1.c' = 'aabb',\n" +
			"  'a.b-c-connector.e-f.g' = 'ada',\n" +
			"  'a.b-c-d.e-1231.g' = 'ada',\n" +
			"  'a.b-c-d.*' = 'adad')\n";
		final String expected = "CREATE TABLE `SOURCE_TABLE` (\n" +
			"  `A`  INTEGER,\n" +
			"  `B`  BIGINT,\n" +
			"  `C`  STRING\n" +
			") WITH (\n" +
			"  'a-b-c-d124' = 'ab',\n" +
			"  'a.b.1.c' = 'aabb',\n" +
			"  'a.b-c-connector.e-f.g' = 'ada',\n" +
			"  'a.b-c-d.e-1231.g' = 'ada',\n" +
			"  'a.b-c-d.*' = 'adad'\n" +
			")";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateTableWithOptionKeyAsIdentifier() {
		final String sql = "create table source_table(\n" +
			"  a int,\n" +
			"  b bigint,\n" +
			"  c string\n" +
			") with (\n" +
			"  ^a^.b.c = 'ab',\n" +
			"  a.b.c1 = 'aabb')\n";
		sql(sql).fails("(?s).*Encountered \"a\" at line 6, column 3.\n.*");
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
	public void testInsertPartitionSpecs() {
		final String sql1 = "insert into emps(x,y) partition (x='ab', y='bc') select * from emps";
		final String expected = "INSERT INTO `EMPS` (`X`, `Y`)\n"
			+ "PARTITION (`X` = 'ab', `Y` = 'bc')\n"
			+ "(SELECT *\n"
			+ "FROM `EMPS`)";
		sql(sql1).ok(expected);
		final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
			+ "  sal, comm, deptno, slacker)\n"
			+ "partition(empno='1', job='job')\n"
			+ "select 'nom', 0, timestamp '1970-01-01 00:00:00',\n"
			+ "  1, 1, 1, false\n"
			+ "from (values 'a')";
		sql(sql2).node(new ValidationMatcher());
		final String sql3 = "insert into empnullables (empno, ename)\n"
			+ "partition(ename='b')\n"
			+ "select 1 from (values 'a')";
		sql(sql3).node(new ValidationMatcher());
	}

	@Test
	public void testInsertCaseSensitivePartitionSpecs() {
		final String expected = "INSERT INTO `emps` (`x`, `y`)\n"
			+ "PARTITION (`x` = 'ab', `y` = 'bc')\n"
			+ "(SELECT *\n"
			+ "FROM `EMPS`)";
		sql("insert into \"emps\"(\"x\",\"y\") "
			+ "partition (\"x\"='ab', \"y\"='bc') select * from emps")
			.ok(expected);
	}

	@Test
	public void testInsertExtendedColumnAsStaticPartition1() {
		final String expected = "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) (`X`, `Y`)\n"
			+ "PARTITION (`Z` = 'ab')\n"
			+ "(SELECT *\n"
			+ "FROM `EMPS`)";
		sql("insert into emps(z boolean)(x,y) partition (z='ab') select * from emps")
			.ok(expected);
	}

	@Test(expected = SqlParseException.class)
	public void testInsertExtendedColumnAsStaticPartition2() {
		sql("insert into emps(x, y, z boolean) partition (z='ab') select * from emps")
			.node(new ValidationMatcher()
				.fails("Extended columns not allowed under the current SQL conformance level"));
	}

	@Test
	public void testInsertOverwrite() {
		// non-partitioned
		final String sql = "INSERT OVERWRITE myDB.myTbl SELECT * FROM src";
		final String expected = "INSERT OVERWRITE `MYDB`.`MYTBL`\n"
				+ "(SELECT *\n"
				+ "FROM `SRC`)";
		sql(sql).ok(expected);

		// partitioned
		final String sql1 = "INSERT OVERWRITE myTbl PARTITION (p1='v1',p2='v2') SELECT * FROM src";
		final String expected1 = "INSERT OVERWRITE `MYTBL`\n"
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
		final String expected = "CREATE VIEW `V`\n" +
			"AS\n" +
			"SELECT `COL1`\n" +
			"FROM `TBL`";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateViewWithComment() {
		final String sql = "create view v COMMENT 'this is a view' as select col1 from tbl";
		final String expected = "CREATE VIEW `V`\n" +
			"COMMENT 'this is a view'\n" +
			"AS\n" +
			"SELECT `COL1`\n" +
			"FROM `TBL`";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateViewWithFieldNames() {
		final String sql = "create view v(col1, col2) as select col3, col4 from tbl";
		final String expected = "CREATE VIEW `V` (`COL1`, `COL2`)\n" +
			"AS\n" +
			"SELECT `COL3`, `COL4`\n" +
			"FROM `TBL`";
		sql(sql).ok(expected);
	}

	@Test
	public void testCreateViewWithInvalidName() {
		final String sql = "create view v(^*^) COMMENT 'this is a view' as select col1 from tbl";
		final String expected = "(?s).*Encountered \"\\*\" at line 1, column 15.*";

		sql(sql).fails(expected);
	}

	@Test
	public void testDropView() {
		final String sql = "DROP VIEW IF EXISTS view_name";
		final String expected = "DROP VIEW IF EXISTS `VIEW_NAME`";
		sql(sql).ok(expected);
	}

	// Override the test because our ROW field type default is nullable,
	// which is different with Calcite.
	@Override
	public void testCastAsRowType() {
		final String expr = "cast(a as row(f0 int, f1 varchar))";
		final String expected = "CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))";
		expr(expr).ok(expected);

		final String expr1 = "cast(a as row(f0 int not null, f1 varchar null))";
		final String expected1 = "CAST(`A` AS ROW(`F0` INTEGER NOT NULL, `F1` VARCHAR))";
		expr(expr1).ok(expected1);

		final String expr2 = "cast(a as row(f0 row(ff0 int not null, ff1 varchar null) null,"
				+ " f1 timestamp not null))";
		final String expected2 = "CAST(`A` AS ROW(`F0` ROW(`FF0` INTEGER NOT NULL, `FF1` VARCHAR),"
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
		sql(sql).node(new ValidationMatcher());
	}

	@Test
	public void testCreateFunction() {
		sql("create function catalog1.db1.function1 as 'org.apache.fink.function.function1'")
				.ok("CREATE FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("create temporary function catalog1.db1.function1 as 'org.apache.fink.function.function1'")
				.ok("CREATE TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("create temporary system function catalog1.db1.function1 as 'org.apache.fink.function.function1'")
				.ok("CREATE TEMPORARY SYSTEM FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("create temporary function db1.function1 as 'org.apache.fink.function.function1'")
				.ok("CREATE TEMPORARY FUNCTION `DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("create temporary function function1 as 'org.apache.fink.function.function1'")
				.ok("CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("create temporary function if not exists catalog1.db1.function1 as 'org.apache.fink.function.function1'")
				.ok("CREATE TEMPORARY FUNCTION IF NOT EXISTS `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		sql("create temporary function function1 as 'org.apache.fink.function.function1' language java")
				.ok("CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE JAVA");

		sql("create temporary system function  function1 as 'org.apache.fink.function.function1' language scala")
				.ok("CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE SCALA");
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

	/** Matcher that invokes the #validate() of the {@link ExtendedSqlNode} instance. **/
	private static class ValidationMatcher extends BaseMatcher<SqlNode> {
		private String expectedColumnSql;
		private String failMsg;

		public ValidationMatcher expectColumnSql(String s) {
			this.expectedColumnSql =  s;
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
					assertEquals(expectedColumnSql,
						((SqlCreateTable) createTable).getColumnSqlString());
				}
				return true;
			} else {
				return false;
			}
		}
	}
}
