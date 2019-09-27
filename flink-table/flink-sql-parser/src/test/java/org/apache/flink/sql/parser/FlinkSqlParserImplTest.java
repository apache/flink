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
import org.junit.Ignore;
import org.junit.Test;

import java.io.Reader;

import static org.junit.Assert.assertEquals;


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
	public void testCreateTable() {
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

	@Ignore // need to implement
	@Test
	public void testCreateTableWithoutWatermarkFieldName() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK FOR a AS BOUNDED WITH DELAY 1000 MILLISECOND\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK FOR `A` AS BOUNDED WITH DELAY 1000 MILLISECOND\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithWatermarkBoundedDelay() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS BOUNDED WITH DELAY 1000 DAY\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 DAY\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithWatermarkBoundedDelay1() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS BOUNDED WITH DELAY 1000 HOUR\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithWatermarkBoundedDelay2() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS BOUNDED WITH DELAY 1000 MINUTE\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 MINUTE\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithWatermarkBoundedDelay3() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS BOUNDED WITH DELAY 1000 SECOND\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 SECOND\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithNegativeWatermarkOffsetDelay() {
		checkFails("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS BOUNDED WITH DELAY ^-^1000 SECOND\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"(?s).*Encountered \"-\" at line 5, column 44.\n" +
				"Was expecting:\n" +
				"    <UNSIGNED_INTEGER_LITERAL> ...\n" +
				".*");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithWatermarkStrategyAscending() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS ASCENDING\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS ASCENDING\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
	}

	@Ignore // need to implement
	@Test
	public void testCreateTableWithWatermarkStrategyFromSource() {
		check("CREATE TABLE tbl1 (\n" +
				"  a bigint,\n" +
				"  b varchar, \n" +
				"  c as 2 * (a + 1), \n" +
				"  WATERMARK wk FOR a AS FROM_SOURCE\n" +
				")\n" +
				"  with (\n" +
				"    'connector' = 'kafka', \n" +
				"    'kafka.topic' = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS FROM_SOURCE\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'kafka.topic' = 'log.test'\n" +
				")");
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
		final String errMsg = "UDT in DDL is not supported yet.";
		checkFails(sql, errMsg);
	}

	@Test
	public void testInvalidComputedColumn() {
		checkFails("CREATE TABLE sls_stream (\n" +
			"  a bigint, \n" +
			"  b varchar,\n" +
			"  toTimestamp^(^b, 'yyyy-MM-dd HH:mm:ss'), \n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  'x' = 'y', \n" +
			"  'asd' = 'data'\n" +
			")\n", "(?s).*Encountered \"\\(\" at line 4, column 14.\n" +
			"Was expecting one of:\n" +
			"    \"AS\" ...\n" +
			"    \"ARRAY\" ...\n" +
			".*");
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
		conformance0 = FlinkSqlConformance.HIVE;
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
		conformance0 = FlinkSqlConformance.HIVE;
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
		conformance0 = FlinkSqlConformance.HIVE;
		String expected = "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) (`X`, `Y`)\n"
			+ "PARTITION (`Z` = 'ab')\n"
			+ "(SELECT *\n"
			+ "FROM `EMPS`)";
		sql("insert into emps(z boolean)(x,y) partition (z='ab') select * from emps")
			.ok(expected);
	}

	@Test(expected = SqlParseException.class)
	public void testInsertExtendedColumnAsStaticPartition2() {
		conformance0 = FlinkSqlConformance.HIVE;
		sql("insert into emps(x, y, z boolean) partition (z='ab') select * from emps")
			.node(new ValidationMatcher()
				.fails("Extended columns not allowed under the current SQL conformance level"));
	}

	@Test
	public void testInsertWithInvalidPartitionColumns() {
		conformance0 = FlinkSqlConformance.HIVE;
		final String sql2 = "insert into emp (empno, ename, job, mgr, hiredate,\n"
			+ "  sal, comm, deptno, slacker)\n"
			+ "partition(^xxx^='1', job='job')\n"
			+ "select 'nom', 0, timestamp '1970-01-01 00:00:00',\n"
			+ "  1, 1, 1, false\n"
			+ "from (values 'a')";
		sql(sql2).node(new ValidationMatcher().fails("Unknown target column 'XXX'"));
		final String sql3 = "insert into ^empnullables^ (ename, empno, deptno)\n"
			+ "partition(empno='1')\n"
			+ "values ('Pat', null)";
		sql(sql3).node(new ValidationMatcher().fails(
			"\"Number of INSERT target columns \\\\(3\\\\) does not \"\n"
				+ "\t\t\t\t+ \"equal number of source items \\\\(2\\\\)\""));
	}

	@Test
	public void testInsertOverwrite() {
		conformance0 = FlinkSqlConformance.HIVE;
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
		conformance0 = FlinkSqlConformance.HIVE;
		checkFails("UPSERT OVERWRITE myDB.myTbl SELECT * FROM src",
			"OVERWRITE expression is only used with INSERT mode");
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

	/** Matcher that invokes the #validate() of the produced SqlNode. **/
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
				try {
					createTable.validate();
				} catch (Exception e) {
					assertEquals(failMsg, e.getMessage());
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
