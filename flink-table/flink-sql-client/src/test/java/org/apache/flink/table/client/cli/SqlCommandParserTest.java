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

package org.apache.flink.table.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommand;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.cli.utils.SqlParserHelper;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.delegation.Parser;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SqlCommandParser}.
 */
public class SqlCommandParserTest {

	private Parser parser;
	private TableEnvironment tableEnv;

	@Before
	public void setup() {
		SqlParserHelper helper = new SqlParserHelper();
		helper.registerTables();
		parser = helper.getSqlParser();
		tableEnv = helper.getTableEnv();
	}

	@Test
	public void testCommands() throws Exception {
		List<TestItem> testItems = Arrays.asList(
				TestItem.validSql("QUIT;", SqlCommand.QUIT).cannotParseComment(),
				TestItem.validSql("eXiT;", SqlCommand.QUIT).cannotParseComment(),
				TestItem.validSql("CLEAR;", SqlCommand.CLEAR).cannotParseComment(),
				// desc xx
				TestItem.validSql("DESC MyTable", SqlCommand.DESC, "MyTable").cannotParseComment(),
				TestItem.validSql("DESC         MyTable     ", SqlCommand.DESC, "MyTable").cannotParseComment(),
				TestItem.invalidSql("DESC ", // no table name
						SqlExecutionException.class,
						"Non-query expression encountered in illegal context"),
				// describe xx
				TestItem.validSql("DESCRIBE MyTable",
						SqlCommand.DESCRIBE,
						"`default_catalog`.`default_database`.`MyTable`"),
				TestItem.validSql("DESCRIBE         MyTable     ",
						SqlCommand.DESCRIBE,
						"`default_catalog`.`default_database`.`MyTable`"),
				TestItem.invalidSql("DESCRIBE ", // no table name
						SqlExecutionException.class,
						"Encountered \"<EOF>\" "),
				// explain xx
				TestItem.validSql("EXPLAIN SELECT a FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR SELECT a FROM MyTable").cannotParseComment(),
				TestItem.validSql("EXPLAIN INSERT INTO MySink(a) SELECT a FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR INSERT INTO MySink(a) SELECT a FROM MyTable").cannotParseComment(),
				TestItem.invalidSql("EXPLAIN ", // no query
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				// explain plan for xx
				TestItem.validSql("EXPLAIN PLAN FOR SELECT a FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR SELECT a FROM MyTable"),
				TestItem.validSql("EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable"),
				TestItem.validSql("EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable"),
				TestItem.invalidSql("EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT xxx FROM MyTable",
						SqlExecutionException.class,
						"Column 'xxx' not found in any table"),
				// select xx
				TestItem.validSql("SELECT a FROM MyTable", SqlCommand.SELECT, "SELECT a FROM MyTable"),
				TestItem.validSql("WITH t as (select a from MyTable) select a from t",
						SqlCommand.SELECT,
						"WITH t as (select a from MyTable) select a from t"),
				// insert xx
				TestItem.validSql("INSERT INTO other SELECT 1+1",
						SqlCommand.INSERT_INTO,
						"INSERT INTO other SELECT 1+1"),
				TestItem.validSql("INSERT OVERWRITE other SELECT 1+1",
						SqlCommand.INSERT_OVERWRITE,
						"INSERT OVERWRITE other SELECT 1+1"),
				// create view xx
				TestItem.validSql("CREATE VIEW x AS SELECT 1+1",
						SqlCommand.CREATE_VIEW,
						"CREATE VIEW x AS SELECT 1+1"),
				TestItem.validSql("CREATE   VIEW    x   AS     SELECT 1+1 FROM MyTable",
						SqlCommand.CREATE_VIEW,
						"CREATE   VIEW    x   AS     SELECT 1+1 FROM MyTable"),
				TestItem.invalidSql("CREATE VIEW x SELECT 1+1 ", // missing AS
						SqlExecutionException.class,
						"Encountered \"SELECT\""),
				// drop view xx
				TestItem.validSql("DROP VIEW TestView1",
						SqlCommand.DROP_VIEW,
						"DROP VIEW TestView1"),
				TestItem.invalidSql("DROP VIEW ", // missing name
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				// alter view
				TestItem.validSql(SqlDialect.HIVE,
						"ALTER VIEW MyView RENAME TO MyView1",
						SqlCommand.ALTER_VIEW,
						"ALTER VIEW MyView RENAME TO MyView1"),
				// set
				TestItem.validSql("SET", SqlCommand.SET).cannotParseComment(),
				TestItem.validSql("SET x=y", SqlCommand.SET, "x", "y").cannotParseComment(),
				TestItem.validSql("SET      x  = y", SqlCommand.SET, "x", " y").cannotParseComment(),
				// reset
				TestItem.validSql("reset;", SqlCommand.RESET).cannotParseComment(),
				// source xx
				TestItem.validSql("source /my/file;", SqlCommand.SOURCE, "/my/file").cannotParseComment(),
				// create catalog xx
				TestItem.validSql("create CATALOG c1 with('type'='generic_in_memory')",
						SqlCommand.CREATE_CATALOG,
						"create CATALOG c1 with('type'='generic_in_memory')"),
				TestItem.validSql("create CATALOG c1 WITH ('type'='simple-catalog', 'default-database'='db1')",
						SqlCommand.CREATE_CATALOG,
						"create CATALOG c1 WITH ('type'='simple-catalog', 'default-database'='db1')"),
				// drop catalog xx
				TestItem.validSql("drop CATALOG c1", SqlCommand.DROP_CATALOG, "drop CATALOG c1"),
				// use xx
				TestItem.validSql("USE CATALOG catalog1;", SqlCommand.USE_CATALOG, "catalog1"),
				TestItem.validSql("use `default`;", SqlCommand.USE, "default"),
				TestItem.invalidSql("use catalog ", // no catalog name
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				// create database xx
				TestItem.validSql("create database db1;", SqlCommand.CREATE_DATABASE, "create database db1"),
				// drop database xx
				TestItem.validSql("drop database db1;", SqlCommand.DROP_DATABASE, "drop database db1"),
				// alter database xx
				TestItem.validSql("alter database default_database set ('k1' = 'a')",
						SqlCommand.ALTER_DATABASE,
						"alter database default_database set ('k1' = 'a')"),
				// alter table xx
				TestItem.validSql("alter table default_catalog.default_database.MyTable rename to tb2",
						SqlCommand.ALTER_TABLE, "alter table default_catalog.default_database.MyTable rename to tb2"),
				TestItem.validSql("alter table MyTable set ('k1'='v1', 'k2'='v2')",
						SqlCommand.ALTER_TABLE,
						"alter table MyTable set ('k1'='v1', 'k2'='v2')"),
				// create table xx
				TestItem.invalidSql("CREATE table",
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				TestItem.invalidSql("CREATE    tables",
						SqlExecutionException.class,
						"Encountered \"tables\""),
				TestItem.validSql("create Table hello", SqlCommand.CREATE_TABLE, "create Table hello"),
				TestItem.validSql("create Table hello(a int)", SqlCommand.CREATE_TABLE, "create Table hello(a int)"),
				TestItem.validSql("CREATE TABLE T(\n"
								+ "  a int,\n"
								+ "  b varchar(20),\n"
								+ "  c as to_timestamp(b),\n"
								+ "  watermark for c as c - INTERVAL '5' second\n"
								+ ") WITH (\n"
								+ "  'k1' = 'v1',\n"
								+ "  'k2' = 'v2')\n",
						SqlCommand.CREATE_TABLE,
						"CREATE TABLE T(\n"
								+ "  a int,\n"
								+ "  b varchar(20),\n"
								+ "  c as to_timestamp(b),\n"
								+ "  watermark for c as c - INTERVAL '5' second\n"
								+ ") WITH (\n"
								+ "  'k1' = 'v1',\n"
								+ "  'k2' = 'v2')"),
				// drop table xx
				TestItem.invalidSql("DROP table",
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				TestItem.invalidSql("DROP   tables",
						SqlExecutionException.class,
						"Encountered \"tables\""),
				TestItem.validSql("DROP TABLE t1", SqlCommand.DROP_TABLE, "DROP TABLE t1"),
				TestItem.validSql("DROP TABLE IF EXISTS t1", SqlCommand.DROP_TABLE, "DROP TABLE IF EXISTS t1"),
				TestItem.validSql("DROP TABLE IF EXISTS catalog1.db1.t1", SqlCommand.DROP_TABLE,
						"DROP TABLE IF EXISTS catalog1.db1.t1"),
				TestItem.validSql("DROP TABLE IF EXISTS db1.t1", SqlCommand.DROP_TABLE, "DROP TABLE IF EXISTS db1.t1"),
				// show catalogs
				TestItem.validSql("SHOW CATALOGS;", SqlCommand.SHOW_CATALOGS),
				TestItem.validSql("  SHOW   CATALOGS   ;", SqlCommand.SHOW_CATALOGS),
				// show current catalog
				TestItem.validSql("show current catalog", SqlCommand.SHOW_CURRENT_CATALOG),
				TestItem.validSql("show 	current 	catalog", SqlCommand.SHOW_CURRENT_CATALOG),
				// show databases
				TestItem.validSql("SHOW DATABASES;", SqlCommand.SHOW_DATABASES),
				TestItem.validSql("  SHOW   DATABASES   ;", SqlCommand.SHOW_DATABASES),
				// show current database
				TestItem.validSql("show current database", SqlCommand.SHOW_CURRENT_DATABASE),
				TestItem.validSql("show 	current 	database", SqlCommand.SHOW_CURRENT_DATABASE),
				// show tables
				TestItem.validSql("SHOW TABLES;", SqlCommand.SHOW_TABLES),
				TestItem.validSql("  SHOW   TABLES   ;", SqlCommand.SHOW_TABLES),
				// show functions
				TestItem.validSql("SHOW FUNCTIONS;", SqlCommand.SHOW_FUNCTIONS),
				TestItem.validSql("  SHOW    FUNCTIONS   ", SqlCommand.SHOW_FUNCTIONS),
				// show modules
				TestItem.validSql("SHOW MODULES", SqlCommand.SHOW_MODULES).cannotParseComment(),
				TestItem.validSql("  SHOW    MODULES   ", SqlCommand.SHOW_MODULES).cannotParseComment(),
				// Test create function.
				TestItem.invalidSql("CREATE FUNCTION ",
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				TestItem.invalidSql("CREATE FUNCTIONS ",
						SqlExecutionException.class,
						"Encountered \"FUNCTIONS\""),
				TestItem.invalidSql("CREATE    FUNCTIONS ",
						SqlExecutionException.class,
						"Encountered \"FUNCTIONS\""),
				TestItem.validSql("CREATE FUNCTION catalog1.db1.func1 as 'class_name'",
						SqlCommand.CREATE_FUNCTION,
						"CREATE FUNCTION catalog1.db1.func1 as 'class_name'"),
				TestItem.validSql("CREATE TEMPORARY FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA",
						SqlCommand.CREATE_FUNCTION,
						"CREATE TEMPORARY FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA"),
				TestItem.validSql("CREATE TEMPORARY SYSTEM FUNCTION func1 as 'class_name' LANGUAGE JAVA",
						SqlCommand.CREATE_FUNCTION,
						"CREATE TEMPORARY SYSTEM FUNCTION func1 as 'class_name' LANGUAGE JAVA"),
				// drop function xx
				TestItem.invalidSql("DROP FUNCTION ",
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				TestItem.invalidSql("DROP FUNCTIONS ",
						SqlExecutionException.class,
						"Encountered \"FUNCTIONS\""),
				TestItem.invalidSql("DROP    FUNCTIONS ",
						SqlExecutionException.class,
						"Encountered \"FUNCTIONS\""),
				TestItem.validSql("DROP FUNCTION catalog1.db1.func1",
						SqlCommand.DROP_FUNCTION,
						"DROP FUNCTION catalog1.db1.func1"),
				TestItem.validSql("DROP TEMPORARY FUNCTION catalog1.db1.func1",
						SqlCommand.DROP_FUNCTION,
						"DROP TEMPORARY FUNCTION catalog1.db1.func1"),
				TestItem.validSql("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1",
						SqlCommand.DROP_FUNCTION,
						"DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1"),
				// alter function xx
				TestItem.invalidSql("ALTER FUNCTION ",
						SqlExecutionException.class,
						"Encountered \"<EOF>\""),
				TestItem.invalidSql("ALTER FUNCTIONS ",
						SqlExecutionException.class,
						"Encountered \"FUNCTIONS\""),
				TestItem.invalidSql("ALTER    FUNCTIONS ",
						SqlExecutionException.class,
						"Encountered \"FUNCTIONS\""),
				TestItem.validSql("ALTER FUNCTION catalog1.db1.func1 as 'a.b.c.func2'",
						SqlCommand.ALTER_FUNCTION,
						"ALTER FUNCTION catalog1.db1.func1 as 'a.b.c.func2'"),
				TestItem.validSql("ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
						SqlCommand.ALTER_FUNCTION,
						"ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'"),
				TestItem.validSql("ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
						SqlCommand.ALTER_FUNCTION,
						"ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'"),
				TestItem.invalidSql(
						"ALTER TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
						SqlExecutionException.class,
						"Alter temporary system function is not supported")
		);
		for (TestItem item : testItems) {
			tableEnv.getConfig().setSqlDialect(item.sqlDialect);
			runTestItem(item);
		}
	}

	@Test
	public void testHiveCommands() throws Exception {
		List<TestItem> testItems = Collections.singletonList(
			// show partitions
			TestItem.validSql(SqlDialect.HIVE, "SHOW PARTITIONS t1", SqlCommand.SHOW_PARTITIONS, "SHOW PARTITIONS t1")
		);
		for (TestItem item : testItems) {
			tableEnv.getConfig().setSqlDialect(item.sqlDialect);
			runTestItem(item);
		}
	}

	private void runTestItem(TestItem item) {
		Tuple2<Boolean, SqlCommandCall> checkFlagAndActualCall = parseSqlAndCheckException(item);
		if (!checkFlagAndActualCall.f0) {
			return;
		}
		SqlCommandCall actualCall = checkFlagAndActualCall.f1;
		assertNotNull(item.expectedCmd);
		assertEquals("test statement: " + item.sql,
				new SqlCommandCall(item.expectedCmd, item.expectedOperands), actualCall);

		String stmtWithComment = "-- comments \n " + item.sql;
		try {
			actualCall = SqlCommandParser.parse(parser, stmtWithComment);
		} catch (SqlExecutionException e) {
			if (!item.cannotParseComment) {
				fail("test statement: " + item.sql);
			}
			return;
		}
		assertEquals(item.expectedCmd, actualCall.command);
	}

	private Tuple2<Boolean, SqlCommandCall> parseSqlAndCheckException(TestItem item) {
		SqlCommandCall call = null;
		Throwable actualException = null;
		try {
			call = SqlCommandParser.parse(parser, item.sql);
		} catch (Throwable e) {
			actualException = e;
		}

		if (item.expectedException == null && actualException == null) {
			return Tuple2.of(true, call);
		} else if (item.expectedException == null) {
			actualException.printStackTrace();
			fail("Failed to run sql: " + item.sql);
		} else if (actualException == null) {
			fail("the excepted exception: '" + item.expectedException + "' does not occur.\n" +
					"test statement: " + item.sql);
		} else {
			assertTrue(actualException.getClass().isAssignableFrom(item.expectedException));
			boolean hasExpectedExceptionMsg = false;
			while (actualException != null) {
				if (actualException.getMessage().contains(item.expectedExceptionMsg)) {
					hasExpectedExceptionMsg = true;
					break;
				}
				actualException = actualException.getCause();
			}
			if (!hasExpectedExceptionMsg) {
				fail("the excepted exception message: '" + item.expectedExceptionMsg + "' does not occur.\n" +
						"test statement: " + item.sql);
			}
		}
		return Tuple2.of(false, null);
	}

	private static class TestItem {
		private final String sql;
		private boolean cannotParseComment = true;
		private @Nullable
		SqlCommand expectedCmd = null;
		private String[] expectedOperands = new String[0];
		private Class<? extends Throwable> expectedException = null;
		private String expectedExceptionMsg = null;
		private SqlDialect sqlDialect = SqlDialect.DEFAULT;

		private TestItem(String sql) {
			this.sql = sql;
		}

		public static TestItem invalidSql(
				String sql,
				Class<? extends Throwable> expectedException,
				String exceptedMsg) {
			TestItem testItem = new TestItem(sql);
			testItem.expectedException = expectedException;
			testItem.expectedExceptionMsg = exceptedMsg;
			return testItem;
		}

		public static TestItem validSql(
				String sql, SqlCommand expectedCmd, String... expectedOperands) {
			TestItem testItem = new TestItem(sql);
			testItem.expectedCmd = expectedCmd;
			testItem.expectedOperands = expectedOperands;
			testItem.cannotParseComment = false; // default is false
			return testItem;
		}

		public static TestItem validSql(
				SqlDialect sqlDialect, String sql, SqlCommand expectedCmd, String... expectedOperands) {
			TestItem testItem = new TestItem(sql);
			testItem.expectedCmd = expectedCmd;
			testItem.expectedOperands = expectedOperands;
			testItem.cannotParseComment = false; // default is false
			testItem.sqlDialect = sqlDialect;
			return testItem;
		}

		public TestItem cannotParseComment() {
			cannotParseComment = true;
			return this;
		}

		@Override
		public String toString() {
			return this.sql;
		}
	}
}
