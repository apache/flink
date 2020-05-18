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

import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommand;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.cli.utils.SqlParserHelper;
import org.apache.flink.table.delegation.Parser;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SqlCommandParser}.
 */
public class SqlCommandParserTest {

	private Parser parser;

	@Before
	public void setup() {
		SqlParserHelper helper = new SqlParserHelper();
		helper.registerTables();
		parser = helper.getSqlParser();
	}

	@Test
	public void testCommands() {
		List<TestItem> testItems = Arrays.asList(
				TestItem.validSql("QUIT;", SqlCommand.QUIT).cannotParseComment(),
				TestItem.validSql("eXiT;", SqlCommand.QUIT).cannotParseComment(),
				TestItem.validSql("CLEAR;", SqlCommand.CLEAR).cannotParseComment(),
				TestItem.validSql("SHOW TABLES;", SqlCommand.SHOW_TABLES),
				TestItem.validSql("  SHOW   TABLES   ;", SqlCommand.SHOW_TABLES),
				TestItem.validSql("SHOW FUNCTIONS;", SqlCommand.SHOW_FUNCTIONS),
				TestItem.validSql("  SHOW    FUNCTIONS   ", SqlCommand.SHOW_FUNCTIONS),
				TestItem.validSql("DESC MyTable", SqlCommand.DESC, "MyTable").cannotParseComment(),
				TestItem.validSql("DESC         MyTable     ", SqlCommand.DESC, "MyTable").cannotParseComment(),
				TestItem.invalidSql("DESC "), // no table name
				TestItem.validSql("DESCRIBE MyTable",
						SqlCommand.DESCRIBE,
						"`default_catalog`.`default_database`.`MyTable`"),
				TestItem.validSql("DESCRIBE         MyTable     ",
						SqlCommand.DESCRIBE,
						"`default_catalog`.`default_database`.`MyTable`"),
				TestItem.invalidSql("DESCRIBE "), // no table name
				TestItem.validSql("EXPLAIN SELECT a FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR SELECT a FROM MyTable").cannotParseComment(),
				TestItem.validSql("EXPLAIN PLAN FOR SELECT a FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR SELECT a FROM MyTable"),
				TestItem.validSql("EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable"),
				TestItem.invalidSql("EXPLAIN "), // no query
				TestItem.validSql("EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable",
						SqlCommand.EXPLAIN,
						"EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable"),
				TestItem.validSql("SELECT a FROM MyTable", SqlCommand.SELECT, "SELECT a FROM MyTable"),
				TestItem.validSql("WITH t as (select a from MyTable) select a from t",
						SqlCommand.SELECT,
						"WITH t as (select a from MyTable) select a from t"),
				TestItem.validSql("INSERT INTO other SELECT 1+1",
						SqlCommand.INSERT_INTO,
						"INSERT INTO other SELECT 1+1"),
				TestItem.validSql("INSERT OVERWRITE other SELECT 1+1",
						SqlCommand.INSERT_OVERWRITE,
						"INSERT OVERWRITE other SELECT 1+1"),
				TestItem.validSql("CREATE VIEW x AS SELECT 1+1",
						SqlCommand.CREATE_VIEW,
						"`default_catalog`.`default_database`.`x`", "SELECT 1 + 1"),
				TestItem.validSql("CREATE   VIEW    x   AS     SELECT 1+1 FROM MyTable",
						SqlCommand.CREATE_VIEW,
						"`default_catalog`.`default_database`.`x`",
						"SELECT 1 + 1\nFROM `default_catalog`.`default_database`.`MyTable` AS `MyTable`"),
				TestItem.invalidSql("CREATE VIEW x SELECT 1+1 "), // missing AS
				TestItem.validSql("DROP VIEW TestView1",
						SqlCommand.DROP_VIEW,
						"`default_catalog`.`default_database`.`TestView1`"),
				TestItem.invalidSql("DROP VIEW "), // missing name
				TestItem.validSql("SET", SqlCommand.SET).cannotParseComment(),
				TestItem.validSql("SET x=y", SqlCommand.SET, "x", "y").cannotParseComment(),
				TestItem.validSql("SET      x  = y", SqlCommand.SET, "x", " y").cannotParseComment(),
				TestItem.validSql("reset;", SqlCommand.RESET).cannotParseComment(),
				TestItem.validSql("source /my/file;", SqlCommand.SOURCE, "/my/file").cannotParseComment(),
				TestItem.validSql("create CATALOG c1 with('type'='generic_in_memory')",
						SqlCommand.CREATE_CATALOG,
						"create CATALOG c1 with('type'='generic_in_memory')"),
				TestItem.validSql("create CATALOG c1 WITH ('type'='simple-catalog', 'default-database'='db1')",
						SqlCommand.CREATE_CATALOG,
						"create CATALOG c1 WITH ('type'='simple-catalog', 'default-database'='db1')"),
				TestItem.validSql("drop CATALOG c1",
						SqlCommand.DROP_CATALOG,
						"drop CATALOG c1"),
				TestItem.validSql("USE CATALOG catalog1;", SqlCommand.USE_CATALOG, "`catalog1`"),
				TestItem.validSql("use `default`;", SqlCommand.USE, "`default_catalog`.`default`"),
				TestItem.invalidSql("use catalog "), // no catalog name
				TestItem.validSql("create database db1;", SqlCommand.CREATE_DATABASE, "create database db1"),
				TestItem.validSql("drop database db1;", SqlCommand.DROP_DATABASE, "drop database db1"),
				TestItem.validSql("alter database default_database set ('k1' = 'a')",
						SqlCommand.ALTER_DATABASE,
						"alter database default_database set ('k1' = 'a')"),
				TestItem.validSql("alter table default_catalog.default_database.MyTable rename to tb2",
						SqlCommand.ALTER_TABLE, "alter table default_catalog.default_database.MyTable rename to tb2"),
				TestItem.validSql("alter table MyTable set ('k1'='v1', 'k2'='v2')",
						SqlCommand.ALTER_TABLE,
						"alter table MyTable set ('k1'='v1', 'k2'='v2')"),
				TestItem.invalidSql("CREATE tables"),
				TestItem.invalidSql("CREATE    tables"),
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
				TestItem.invalidSql("DROP table"),
				TestItem.invalidSql("DROP   tables"),
				TestItem.validSql("DROP TABLE t1", SqlCommand.DROP_TABLE, "DROP TABLE t1"),
				TestItem.validSql("DROP TABLE IF EXISTS t1", SqlCommand.DROP_TABLE, "DROP TABLE IF EXISTS t1"),
				TestItem.validSql("DROP TABLE IF EXISTS catalog1.db1.t1", SqlCommand.DROP_TABLE,
						"DROP TABLE IF EXISTS catalog1.db1.t1"),
				TestItem.validSql("DROP TABLE IF EXISTS db1.t1", SqlCommand.DROP_TABLE, "DROP TABLE IF EXISTS db1.t1"),
				TestItem.validSql("SHOW MODULES", SqlCommand.SHOW_MODULES).cannotParseComment(),
				TestItem.validSql("  SHOW    MODULES   ", SqlCommand.SHOW_MODULES).cannotParseComment(),
				// Test create function.
				TestItem.invalidSql("CREATE FUNCTION "),
				TestItem.invalidSql("CREATE FUNCTIONS "),
				TestItem.invalidSql("CREATE    FUNCTIONS "),
				TestItem.validSql("CREATE FUNCTION catalog1.db1.func1 as 'class_name'",
						SqlCommand.CREATE_FUNCTION,
						"CREATE FUNCTION catalog1.db1.func1 as 'class_name'"),
				TestItem.validSql("CREATE TEMPORARY FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA",
						SqlCommand.CREATE_FUNCTION,
						"CREATE TEMPORARY FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA"),
				TestItem.validSql("CREATE TEMPORARY SYSTEM FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA",
						SqlCommand.CREATE_FUNCTION,
						"CREATE TEMPORARY SYSTEM FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA"),
				// Test drop function.
				TestItem.invalidSql("DROP FUNCTION "),
				TestItem.invalidSql("DROP FUNCTIONS "),
				TestItem.invalidSql("DROP    FUNCTIONS "),
				TestItem.validSql("DROP FUNCTION catalog1.db1.func1",
						SqlCommand.DROP_FUNCTION,
						"DROP FUNCTION catalog1.db1.func1"),
				TestItem.validSql("DROP TEMPORARY FUNCTION catalog1.db1.func1",
						SqlCommand.DROP_FUNCTION,
						"DROP TEMPORARY FUNCTION catalog1.db1.func1"),
				TestItem.validSql("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1",
						SqlCommand.DROP_FUNCTION,
						"DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1"),
				// Test alter function.
				TestItem.invalidSql("ALTER FUNCTION "),
				TestItem.invalidSql("ALTER FUNCTIONS "),
				TestItem.invalidSql("ALTER    FUNCTIONS "),
				TestItem.validSql("ALTER FUNCTION catalog1.db1.func1 as 'a.b.c.func2'",
						SqlCommand.DROP_FUNCTION,
						"ALTER FUNCTION catalog1.db1.func1 as 'a.b.c.func2'"),
				TestItem.validSql("ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
						SqlCommand.DROP_FUNCTION,
						"ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'"),
				TestItem.validSql("ALTER TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
						SqlCommand.DROP_FUNCTION,
						"ALTER TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'")
		);
		for (TestItem item : testItems) {
			if (item.isValidSqlCmd) {
				Optional<SqlCommandCall> actualCall = SqlCommandParser.parse(parser, item.sql);
				if (!actualCall.isPresent()) {
					fail("test statement: " + item.sql);
				}
				assertNotNull(item.expectedCmd);
				assertEquals("test statement: " + item.sql,
						new SqlCommandCall(item.expectedCmd, item.expectedOperands), actualCall.get());

				String stmtWithComment = "-- comments \n " + item.sql;
				actualCall = SqlCommandParser.parse(parser, stmtWithComment);
				if (item.cannotParseComment) {
					assertFalse(actualCall.isPresent());
				} else {
					if (!actualCall.isPresent()) {
						fail("test statement: " + item.sql);
					}
					assertEquals(item.expectedCmd, actualCall.get().command);
				}
			} else {
				final Optional<SqlCommandCall> actualCall = SqlCommandParser.parse(parser, item.sql);
				if (actualCall.isPresent()) {
					fail("test statement: " + item.sql);
				}
			}
		}
	}

	private static class TestItem {
		private final String sql;
		private final boolean isValidSqlCmd;
		private boolean cannotParseComment = true;
		private @Nullable
		SqlCommand expectedCmd = null;
		private String[] expectedOperands = new String[0];

		private TestItem(String sql, boolean isValidSqlCmd) {
			this.sql = sql;
			this.isValidSqlCmd = isValidSqlCmd;
		}

		public static TestItem invalidSql(String sql) {
			return new TestItem(sql, false);
		}

		public static TestItem validSql(
				String sql, SqlCommand expectedCmd, String... expectedOperands) {
			TestItem testItem = new TestItem(sql, true);
			testItem.expectedCmd = expectedCmd;
			testItem.expectedOperands = expectedOperands;
			testItem.cannotParseComment = false; // default is false
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
