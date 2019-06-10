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
import org.apache.flink.sql.parser.error.SqlParseException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/** FlinkSqlParserImpl tests. **/
public class FlinkSqlParserImplTest extends SqlParserTest {

	protected SqlParserImplFactory parserImplFactory() {
		return FlinkSqlParserImpl.FACTORY;
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
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
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
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
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
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
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK FOR `A` AS BOUNDED WITH DELAY 1000 MILLISECOND\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 DAY\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 HOUR\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 MINUTE\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS BOUNDED WITH DELAY 1000 SECOND\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS ASCENDING\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
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
				"    connector = 'kafka', \n" +
				"    kafka.topic = 'log.test'\n" +
				")\n",
			"CREATE TABLE `TBL1` (\n" +
				"  `A`  BIGINT,\n" +
				"  `B`  VARCHAR,\n" +
				"  `C` AS (2 * (`A` + 1)),\n" +
				"  WATERMARK `WK` FOR `A` AS FROM_SOURCE\n" +
				") WITH (\n" +
				"  `CONNECTOR` = 'kafka',\n" +
				"  `KAFKA`.`TOPIC` = 'log.test'\n" +
				")");
	}

	@Test
	public void testCreateTableWithComplexType() {
		check("CREATE TABLE tbl1 (\n" +
			"  a ARRAY<bigint>, \n" +
			"  b MAP<int, varchar>,\n" +
			"  c ROW<cc0:int, cc1: float, cc2: varchar>,\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
			")\n", "CREATE TABLE `TBL1` (\n" +
			"  `A`  ARRAY< BIGINT >,\n" +
			"  `B`  MAP< INTEGER, VARCHAR >,\n" +
			"  `C`  ROW< `CC0` : INTEGER, `CC1` : FLOAT, `CC2` : VARCHAR >,\n" +
			"  PRIMARY KEY (`A`, `B`)\n" +
			") WITH (\n" +
			"  `X` = 'y',\n" +
			"  `ASD` = 'data'\n" +
			")");
	}

	@Test
	public void testCreateTableWithDecimalType() {
		check("CREATE TABLE tbl1 (\n" +
			"  a decimal, \n" +
			"  b decimal(10, 0),\n" +
			"  c decimal(38, 38),\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
			")\n", "CREATE TABLE `TBL1` (\n" +
			"  `A`  DECIMAL,\n" +
			"  `B`  DECIMAL(10, 0),\n" +
			"  `C`  DECIMAL(38, 38),\n" +
			"  PRIMARY KEY (`A`, `B`)\n" +
			") WITH (\n" +
			"  `X` = 'y',\n" +
			"  `ASD` = 'data'\n" +
			")");
	}

	@Test
	public void testCreateTableWithNestedComplexType() {
		check("CREATE TABLE tbl1 (\n" +
			"  a ARRAY<ARRAY<bigint>>, \n" +
			"  b MAP<MAP<int, varchar>, ARRAY<varchar>>,\n" +
			"  c ROW<cc0:ARRAY<int>, cc1: float, cc2: varchar>,\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
			")\n", "CREATE TABLE `TBL1` (\n" +
			"  `A`  ARRAY< ARRAY< BIGINT > >,\n" +
			"  `B`  MAP< MAP< INTEGER, VARCHAR >, ARRAY< VARCHAR > >,\n" +
			"  `C`  ROW< `CC0` : ARRAY< INTEGER >, `CC1` : FLOAT, `CC2` : VARCHAR >,\n" +
			"  PRIMARY KEY (`A`, `B`)\n" +
			") WITH (\n" +
			"  `X` = 'y',\n" +
			"  `ASD` = 'data'\n" +
			")");
	}

	@Test
	public void testInvalidComputedColumn() {
		checkFails("CREATE TABLE sls_stream (\n" +
			"  a bigint, \n" +
			"  b varchar,\n" +
			"  ^toTimestamp^(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
			")\n", "(?s).*Encountered \"toTimestamp \\(\" at line 4, column 3.\n" +
			"Was expecting one of:\n" +
			"    <IDENTIFIER> \"CHARACTER\" ...\n" +
			"    <IDENTIFIER> \"CHAR\" ...\n" +
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
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
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
			") with ( x = 'y', asd = 'dada')";
		sql(sql).node(new ValidationMatcher()
			.fails("Partition column [C] not defined in columns, at line 6, column 3"));

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
			if (item instanceof SqlCreateTable) {
				SqlCreateTable createTable = (SqlCreateTable) item;
				try {
					createTable.validate();
				} catch (SqlParseException e) {
					assertEquals(failMsg, e.getMessage());
				}
				if (expectedColumnSql != null) {
					assertEquals(expectedColumnSql, createTable.getColumnSqlString());
				}
				return true;
			} else {
				return false;
			}
		}
	}
}
