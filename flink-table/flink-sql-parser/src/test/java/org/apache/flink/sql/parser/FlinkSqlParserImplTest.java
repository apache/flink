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
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.validate.SqlConformance;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;

import java.io.Reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/** FlinkSqlParserImpl tests. **/
public class FlinkSqlParserImplTest extends SqlParserTest {
	private SqlConformance conformance0;

	@Override
	protected SqlParserImplFactory parserImplFactory() {
		return FlinkSqlParserImpl.FACTORY;
	}

	protected SqlParser getSqlParser(Reader source) {
		if (conformance0 == null) {
			return super.getSqlParser(source);
		} else {
			// overwrite the default sql conformance.
			return SqlParser.create(source,
				SqlParser.configBuilder()
					.setParserFactory(parserImplFactory())
					.setQuoting(Quoting.DOUBLE_QUOTE)
					.setUnquotedCasing(Casing.TO_UPPER)
					.setQuotedCasing(Casing.UNCHANGED)
					.setConformance(conformance0)
					.build());
		}
	}

	@Before
	public void before() {
		// clear the custom sql conformance.
		conformance0 = null;
	}

	@Test
	public void testShowCatalogs() {
		check("show catalogs", "SHOW CATALOGS");
	}

	@Test
	public void testDescribeCatalog() {
		check("describe catalog a", "DESCRIBE CATALOG `A`");
	}

	/**
	 * Here we override the super method to avoid test error from `describe schema` supported in original calcite.
	 */
	@Override
	public void testDescribeSchema() {
	}

	@Test
	public void testUseCatalog() {
		check("use catalog a", "USE CATALOG `A`");
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
		check("show databases", "SHOW DATABASES");
	}

	@Test
	public void testUseDataBase() {
		check("use default_db", "USE `DEFAULT_DB`");
		check("use defaultCatalog.default_db", "USE `DEFAULTCATALOG`.`DEFAULT_DB`");
	}

	@Test
	public void testCreateDatabase() {
		check("create database db1", "CREATE DATABASE `DB1`");
		check("create database if not exists db1", "CREATE DATABASE IF NOT EXISTS `DB1`");
		check("create database catalog1.db1", "CREATE DATABASE `CATALOG1`.`DB1`");
		check("create database db1 comment 'test create database'",
			"CREATE DATABASE `DB1`\n" +
			"COMMENT 'test create database'");
		check("create database db1 comment 'test create database'" +
			"with ( 'key1' = 'value1', 'key2.a' = 'value2.a')",
			"CREATE DATABASE `DB1`\n" +
			"COMMENT 'test create database' WITH (\n" +
			"  'key1' = 'value1',\n" +
			"  'key2.a' = 'value2.a'\n" +
			")");
	}

	@Test
	public void testDropDatabase() {
		check("drop database db1", "DROP DATABASE `DB1` RESTRICT");
		check("drop database catalog1.db1", "DROP DATABASE `CATALOG1`.`DB1` RESTRICT");
		check("drop database db1 RESTRICT", "DROP DATABASE `DB1` RESTRICT");
		check("drop database db1 CASCADE", "DROP DATABASE `DB1` CASCADE");
	}

	@Test
	public void testAlterDatabase() {
		check("alter database db1 set ('key1' = 'value1','key2.a' = 'value2.a')",
			"ALTER DATABASE `DB1` SET (\n" +
			"  'key1' = 'value1',\n" +
			"  'key2.a' = 'value2.a'\n" +
			")");
	}

	@Test
	public void testDescribeDatabase() {
		check("describe database db1", "DESCRIBE DATABASE `DB1`");
		check("describe database catlog1.db1", "DESCRIBE DATABASE `CATLOG1`.`DB1`");
		check("describe database extended db1", "DESCRIBE DATABASE EXTENDED `DB1`");
	}

	@Test
	public void testAlterFunction() {
		check("alter function function1 as 'org.apache.fink.function.function1'",
			"ALTER FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("alter temporary function function1 as 'org.apache.fink.function.function1'",
			"ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("alter temporary function function1 as 'org.apache.fink.function.function1' language scala",
			"ALTER TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE SCALA");

		check ("alter temporary system function function1 as 'org.apache.fink.function.function1'",
			"ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("alter temporary system function function1 as 'org.apache.fink.function.function1' language java",
			"ALTER TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE JAVA");
	}

	@Test
	public void testShowFuntions() {
		check("show functions", "SHOW FUNCTIONS");
		check("show functions db1", "SHOW FUNCTIONS `DB1`");
		check("show functions catalog1.db1", "SHOW FUNCTIONS `CATALOG1`.`DB1`");
	}

	@Test
	public void testShowTables() {
		check("show tables", "SHOW TABLES");
	}

	@Test
	public void testDescribeTable() {
		check("describe tbl", "DESCRIBE `TBL`");
		check("describe catlog1.db1.tbl", "DESCRIBE `CATLOG1`.`DB1`.`TBL`");
		check("describe extended db1", "DESCRIBE EXTENDED `DB1`");
	}

	/**
	 * Here we override the super method to avoid test error from `describe statement` supported in original calcite.
	 */
	@Override
	public void testDescribeStatement() {
	}

	@Test
	public void testAlterTable() {
		check("alter table t1 rename to t2", "ALTER TABLE `T1` RENAME TO `T2`");
		check("alter table c1.d1.t1 rename to t2", "ALTER TABLE `C1`.`D1`.`T1` RENAME TO `T2`");
		check("alter table t1 set ('key1'='value1')",
			"ALTER TABLE `T1` SET (\n" +
			"  'key1' = 'value1'\n" +
			")");
	}

	@Test
	public void testCreateTable() {
		conformance0 = FlinkSqlConformance.HIVE;
		check("CREATE TABLE tbl1 (\n" +
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
				")\n",
			"CREATE TABLE `TBL1` (\n" +
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
				")");
	}

	@Test
	public void testCreateTableWithComment() {
		conformance0 = FlinkSqlConformance.HIVE;
		check("CREATE TABLE tbl1 (\n" +
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
				")\n",
			"CREATE TABLE `TBL1` (\n" +
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
				")");
	}

	@Test
	public void testCreateTableWithPrimaryKeyAndUniqueKey() {
		conformance0 = FlinkSqlConformance.HIVE;
		check("CREATE TABLE tbl1 (\n" +
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
				")\n",
			"CREATE TABLE `TBL1` (\n" +
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
				")");
	}

	@Test
	public void testCreateTableWithWatermark() {
		String sql = "CREATE TABLE tbl1 (\n" +
			"  ts timestamp(3),\n" +
			"  id varchar, \n" +
			"  watermark FOR ts AS ts - interval '3' second\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		check(sql,
			"CREATE TABLE `TBL1` (\n" +
				"  `TS`  TIMESTAMP(3),\n" +
				"  `ID`  VARCHAR,\n" +
				"  WATERMARK FOR `TS` AS (`TS` - INTERVAL '3' SECOND)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Test
	public void testCreateTableWithWatermarkOnComputedColumn() {
		String sql = "CREATE TABLE tbl1 (\n" +
			"  log_ts varchar,\n" +
			"  ts as to_timestamp(log_ts), \n" +
			"  WATERMARK FOR ts AS ts + interval '1' second\n" +
			")\n" +
			"  with (\n" +
			"    'connector' = 'kafka', \n" +
			"    'kafka.topic' = 'log.test'\n" +
			")\n";
		check(sql,
			"CREATE TABLE `TBL1` (\n" +
				"  `LOG_TS`  VARCHAR,\n" +
				"  `TS` AS `TO_TIMESTAMP`(`LOG_TS`),\n" +
				"  WATERMARK FOR `TS` AS (`TS` + INTERVAL '1' SECOND)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Test
	public void testCreateTableWithWatermarkOnNestedField() {
		check("CREATE TABLE tbl1 (\n" +
				"  f1 row<q1 bigint, q2 row<t1 timestamp, t2 varchar>, q3 boolean>,\n" +
				"  WATERMARK FOR f1.q2.t1 AS NOW()\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `F1`  ROW< `Q1` BIGINT, `Q2` ROW< `T1` TIMESTAMP, `T2` VARCHAR >, `Q3` BOOLEAN >,\n" +
				"  WATERMARK FOR `F1`.`Q2`.`T1` AS `NOW`()\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
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
		check("CREATE TABLE tbl1 (\n" +
			"  a ARRAY<bigint>, \n" +
			"  b MAP<int, varchar>,\n" +
			"  c ROW<cc0 int, cc1 float, cc2 varchar>,\n" +
			"  d MULTISET<varchar>,\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  'x' = 'y', \n" +
			"  'asd' = 'data'\n" +
			")\n", "CREATE TABLE `TBL1` (\n" +
			"  `A`  ARRAY< BIGINT >,\n" +
			"  `B`  MAP< INTEGER, VARCHAR >,\n" +
			"  `C`  ROW< `CC0` INTEGER, `CC1` FLOAT, `CC2` VARCHAR >,\n" +
			"  `D`  MULTISET< VARCHAR >,\n" +
			"  PRIMARY KEY (`A`, `B`)\n" +
			") WITH (\n" +
			"  'x' = 'y',\n" +
			"  'asd' = 'data'\n" +
			")");
	}

	@Test
	public void testCreateTableWithNestedComplexType() {
		check("CREATE TABLE tbl1 (\n" +
			"  a ARRAY<ARRAY<bigint>>, \n" +
			"  b MAP<MAP<int, varchar>, ARRAY<varchar>>,\n" +
			"  c ROW<cc0 ARRAY<int>, cc1 float, cc2 varchar>,\n" +
			"  d MULTISET<ARRAY<int>>,\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  'x' = 'y', \n" +
			"  'asd' = 'data'\n" +
			")\n", "CREATE TABLE `TBL1` (\n" +
			"  `A`  ARRAY< ARRAY< BIGINT > >,\n" +
			"  `B`  MAP< MAP< INTEGER, VARCHAR >, ARRAY< VARCHAR > >,\n" +
			"  `C`  ROW< `CC0` ARRAY< INTEGER >, `CC1` FLOAT, `CC2` VARCHAR >,\n" +
			"  `D`  MULTISET< ARRAY< INTEGER > >,\n" +
			"  PRIMARY KEY (`A`, `B`)\n" +
			") WITH (\n" +
			"  'x' = 'y',\n" +
			"  'asd' = 'data'\n" +
			")");
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
		check(sql, expected);
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
		String sql = "CREATE TABLE sls_stream (\n" +
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
		String expected = "`A`, (`A` + 1) AS `F`, `B`, "
			+ "`TOTIMESTAMP`(`B`, 'yyyy-MM-dd HH:mm:ss') AS `TS`, "
			+ "`PROCTIME`() AS `PROC`, `C`";
		sql(sql).node(new ValidationMatcher()
			.expectColumnSql(expected));
	}

	@Test
	public void testCreateInvalidPartitionedTable() {
		conformance0 = FlinkSqlConformance.HIVE;
		String sql = "create table sls_stream1(\n" +
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
	public void testNotAllowedCreatePartition() {
		conformance0 = FlinkSqlConformance.DEFAULT;
		String sql = "create table sls_stream1(\n" +
				"  a bigint,\n" +
				"  b VARCHAR\n" +
				") PARTITIONED BY (a^)^ with ( 'x' = 'y', 'asd' = 'dada')";
		sql(sql).fails("Creating partitioned table is only allowed for HIVE dialect.");
	}

	@Test
	public void testCreateTableWithMinusInOptionKey() {
		String sql = "create table source_table(\n" +
			"  a int,\n" +
			"  b bigint,\n" +
			"  c string\n" +
			") with (\n" +
			"  'a-b-c-d124' = 'ab',\n" +
			"  'a.b.1.c' = 'aabb',\n" +
			"  'a.b-c-connector.e-f.g' = 'ada',\n" +
			"  'a.b-c-d.e-1231.g' = 'ada',\n" +
			"  'a.b-c-d.*' = 'adad')\n";
		String expected = "CREATE TABLE `SOURCE_TABLE` (\n" +
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
		check(sql, expected);
	}

	@Test
	public void testCreateTableWithOptionKeyAsIdentifier() {
		String sql = "create table source_table(\n" +
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
		String sql = "DROP table catalog1.db1.tbl1";
		check(sql, "DROP TABLE `CATALOG1`.`DB1`.`TBL1`");
	}

	@Test
	public void testDropIfExists() {
		String sql = "DROP table IF EXISTS catalog1.db1.tbl1";
		check(sql, "DROP TABLE IF EXISTS `CATALOG1`.`DB1`.`TBL1`");
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
		String expected = "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) (`X`, `Y`)\n"
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
		check("INSERT OVERWRITE myDB.myTbl SELECT * FROM src",
			"INSERT OVERWRITE `MYDB`.`MYTBL`\n"
				+ "(SELECT *\n"
				+ "FROM `SRC`)");

		// partitioned
		check("INSERT OVERWRITE myTbl PARTITION (p1='v1',p2='v2') SELECT * FROM src",
			"INSERT OVERWRITE `MYTBL`\n"
				+ "PARTITION (`P1` = 'v1', `P2` = 'v2')\n"
				+ "(SELECT *\n"
				+ "FROM `SRC`)");
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
		check(sql, expected);
	}

	@Test
	public void testCreateViewWithComment() {
		final String sql = "create view v COMMENT 'this is a view' as select col1 from tbl";
		final String expected = "CREATE VIEW `V`\n" +
			"COMMENT 'this is a view'\n" +
			"AS\n" +
			"SELECT `COL1`\n" +
			"FROM `TBL`";
		check(sql, expected);
	}

	@Test
	public void testCreateViewWithFieldNames() {
		final String sql = "create view v(col1, col2) as select col3, col4 from tbl";
		final String expected = "CREATE VIEW `V` (`COL1`, `COL2`)\n" +
			"AS\n" +
			"SELECT `COL3`, `COL4`\n" +
			"FROM `TBL`";
		check(sql, expected);
	}

	@Test
	public void testCreateViewWithInvalidName() {
		final String sql = "create view v(^*^) COMMENT 'this is a view' as select col1 from tbl";
		final String expected = "(?s).*Encountered \"\\*\" at line 1, column 15.*";

		checkFails(sql, expected);
	}

	@Test
	public void testDropView() {
		final String sql = "DROP VIEW IF EXISTS view_name";
		check(sql, "DROP VIEW IF EXISTS `VIEW_NAME`");
	}

	// Override the test because our ROW field type default is nullable,
	// which is different with Calcite.
	@Override
	public void testCastAsRowType() {
		checkExp("cast(a as row(f0 int, f1 varchar))",
			"CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))");
		checkExp("cast(a as row(f0 int not null, f1 varchar null))",
			"CAST(`A` AS ROW(`F0` INTEGER NOT NULL, `F1` VARCHAR))");
		checkExp("cast(a as row(f0 row(ff0 int not null, ff1 varchar null) null," +
				" f1 timestamp not null))",
			"CAST(`A` AS ROW(`F0` ROW(`FF0` INTEGER NOT NULL, `FF1` VARCHAR)," +
				" `F1` TIMESTAMP NOT NULL))");
		checkExp("cast(a as row(f0 bigint not null, f1 decimal null) array)",
			"CAST(`A` AS ROW(`F0` BIGINT NOT NULL, `F1` DECIMAL) ARRAY)");
		checkExp("cast(a as row(f0 varchar not null, f1 timestamp null) multiset)",
			"CAST(`A` AS ROW(`F0` VARCHAR NOT NULL, `F1` TIMESTAMP) MULTISET)");
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
		check("create function catalog1.db1.function1 as 'org.apache.fink.function.function1'",
			"CREATE FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("create temporary function catalog1.db1.function1 as 'org.apache.fink.function.function1'",
			"CREATE TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("create temporary system function catalog1.db1.function1 as 'org.apache.fink.function.function1'",
			"CREATE TEMPORARY SYSTEM FUNCTION `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("create temporary function db1.function1 as 'org.apache.fink.function.function1'",
			"CREATE TEMPORARY FUNCTION `DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("create temporary function function1 as 'org.apache.fink.function.function1'",
			"CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("create temporary function if not exists catalog1.db1.function1 as 'org.apache.fink.function.function1'",
			"CREATE TEMPORARY FUNCTION IF NOT EXISTS `CATALOG1`.`DB1`.`FUNCTION1` AS 'org.apache.fink.function.function1'");

		check("create temporary function function1 as 'org.apache.fink.function.function1' language java",
			"CREATE TEMPORARY FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE JAVA");

		check("create temporary system function  function1 as 'org.apache.fink.function.function1' language scala",
			"CREATE TEMPORARY SYSTEM FUNCTION `FUNCTION1` AS 'org.apache.fink.function.function1' LANGUAGE SCALA");
	}

	@Test
	public void testDropTemporaryFunction() {
		check("drop temporary function catalog1.db1.function1",
			"DROP TEMPORARY FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

		check("drop temporary system function catalog1.db1.function1",
			"DROP TEMPORARY SYSTEM FUNCTION `CATALOG1`.`DB1`.`FUNCTION1`");

		check("drop temporary function if exists catalog1.db1.function1",
			"DROP TEMPORARY FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");

		check("drop temporary system function if exists catalog1.db1.function1",
			"DROP TEMPORARY SYSTEM FUNCTION IF EXISTS `CATALOG1`.`DB1`.`FUNCTION1`");
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
