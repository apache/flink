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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommand;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.cli.utils.ParserUtils;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SqlCommandParser}.
 */
public class SqlCommandParserTest {

	@Test
	public void testCommands() {
		testValidSqlCommand("QUIT;", new SqlCommandCall(SqlCommand.QUIT));
		testInvalidSqlCommand("-- comments \nQUIT;");
		testValidSqlCommand("eXiT", new SqlCommandCall(SqlCommand.QUIT));
		testInvalidSqlCommand("-- comments \nEXIT;");
		testValidSqlCommand("CLEAR", new SqlCommandCall(SqlCommand.CLEAR));
		testInvalidSqlCommand("-- comments \nCLEAR;");
		testValidSqlCommand("SHOW TABLES", new SqlCommandCall(SqlCommand.SHOW_TABLES));
		testValidSqlCommand("  SHOW   TABLES   ", new SqlCommandCall(SqlCommand.SHOW_TABLES));
		testValidSqlCommand(" -- comments \n SHOW   TABLES   ", new SqlCommandCall(SqlCommand.SHOW_TABLES));
		testValidSqlCommand("SHOW FUNCTIONS", new SqlCommandCall(SqlCommand.SHOW_FUNCTIONS));
		testValidSqlCommand("  SHOW    FUNCTIONS   ", new SqlCommandCall(SqlCommand.SHOW_FUNCTIONS));
		testValidSqlCommand(" -- comments \n SHOW    FUNCTIONS   ", new SqlCommandCall(SqlCommand.SHOW_FUNCTIONS));
		testValidSqlCommand("DESC MyTable", new SqlCommandCall(SqlCommand.DESC, new String[]{"MyTable"}));
		testValidSqlCommand("DESC         MyTable     ", new SqlCommandCall(SqlCommand.DESC, new String[]{"MyTable"}));
		testInvalidSqlCommand("-- comments \n DESC MyTable");
		testInvalidSqlCommand("DESC  "); // no table name
		testValidSqlCommand("DESCRIBE MyTable", new SqlCommandCall(SqlCommand.DESCRIBE, new String[]{"`default_catalog`.`default_database`.`MyTable`"}));
		testValidSqlCommand("DESCRIBE         MyTable     ", new SqlCommandCall(SqlCommand.DESCRIBE, new String[]{"`default_catalog`.`default_database`.`MyTable`"}));
		testValidSqlCommand("-- comments \nDESCRIBE  MyTable  ", new SqlCommandCall(SqlCommand.DESCRIBE, new String[]{"`default_catalog`.`default_database`.`MyTable`"}));
		testInvalidSqlCommand("DESCRIBE  "); // no table name
		testValidSqlCommand("EXPLAIN SELECT a FROM MyTable", new SqlCommandCall(SqlCommand.EXPLAIN, new String[]{"EXPLAIN PLAN FOR SELECT a FROM MyTable"}));
		testInvalidSqlCommand("-- comments \n EXPLAIN SELECT a FROM MyTable");
		testValidSqlCommand(
				"EXPLAIN PLAN FOR SELECT a FROM MyTable",
				new SqlCommandCall(SqlCommand.EXPLAIN, new String[]{"EXPLAIN PLAN FOR SELECT a FROM MyTable"}));
		testInvalidSql("EXPLAIN PLAN FOR SELECT xxx FROM MyTable");
		testValidSqlCommand(
				"-- comments \n EXPLAIN PLAN FOR SELECT a FROM MyTable",
				new SqlCommandCall(SqlCommand.EXPLAIN, new String[]{"-- comments \n EXPLAIN PLAN FOR SELECT a FROM MyTable"}));
		testValidSqlCommand(
				"EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable",
				new SqlCommandCall(SqlCommand.EXPLAIN, new String[]{"EXPLAIN PLAN FOR INSERT INTO MySink(c) SELECT c FROM MyTable"}));
		testInvalidSqlCommand("EXPLAIN  "); // no query
		testValidSqlCommand("SELECT a FROM MyTable", new SqlCommandCall(SqlCommand.SELECT, new String[]{"SELECT a FROM MyTable"}));
		testValidSqlCommand("   SELECT  a FROM MyTable    ", new SqlCommandCall(SqlCommand.SELECT, new String[]{"SELECT  a FROM MyTable"}));
		testValidSqlCommand(
				"-- comments \n SELECT  a FROM MyTable    ",
				new SqlCommandCall(SqlCommand.SELECT, new String[]{"-- comments \n SELECT  a FROM MyTable"}));
		testValidSqlCommand(
			"WITH t as (select a from MyTable) select a from t",
			new SqlCommandCall(SqlCommand.SELECT, new String[]{"WITH t as (select a from MyTable) select a from t"}));
		testValidSqlCommand(
			"   WITH t as (select a from MyTable) select a from t    ",
			new SqlCommandCall(SqlCommand.SELECT, new String[]{"WITH t as (select a from MyTable) select a from t"}));
		testValidSqlCommand(
			"INSERT INTO other SELECT 1+1",
			new SqlCommandCall(SqlCommand.INSERT_INTO, new String[]{"INSERT INTO other SELECT 1+1"}));
		testValidSqlCommand(
			"INSERT OVERWRITE other SELECT 1+1",
			new SqlCommandCall(SqlCommand.INSERT_OVERWRITE, new String[]{"INSERT OVERWRITE other SELECT 1+1"}));
		testValidSqlCommand(
				"-- comments\nINSERT OVERWRITE other SELECT 1+1",
				new SqlCommandCall(SqlCommand.INSERT_OVERWRITE, new String[]{"-- comments\nINSERT OVERWRITE other SELECT 1+1"}));
		testValidSqlCommand(
			"CREATE VIEW x AS SELECT 1+1",
			new SqlCommandCall(SqlCommand.CREATE_VIEW, new String[]{"`default_catalog`.`default_database`.`x`", "SELECT 1 + 1"}));
		testValidSqlCommand(
			"CREATE   VIEW    MyView   AS     SELECT 1+1 FROM MyTable",
			new SqlCommandCall(SqlCommand.CREATE_VIEW, new String[]{"`default_catalog`.`default_database`.`MyView`",
					"SELECT 1 + 1\nFROM `default_catalog`.`default_database`.`MyTable` AS `MyTable`"}));
		testValidSqlCommand(
				"-- comments\nCREATE VIEW x AS SELECT 1+1",
				new SqlCommandCall(SqlCommand.CREATE_VIEW, new String[]{"`default_catalog`.`default_database`.`x`", "SELECT 1 + 1"}));
		testInvalidSqlCommand("CREATE VIEW x SELECT 1+1"); // missing AS
		testValidSqlCommand("DROP VIEW TestView1", new SqlCommandCall(SqlCommand.DROP_VIEW, new String[]{"`default_catalog`.`default_database`.`TestView1`"}));
		testValidSqlCommand("DROP VIEW  TestView1", new SqlCommandCall(SqlCommand.DROP_VIEW, new String[]{"`default_catalog`.`default_database`.`TestView1`"}));
		testValidSqlCommand("-- comments\nDROP VIEW  TestView1", new SqlCommandCall(SqlCommand.DROP_VIEW, new String[]{"`default_catalog`.`default_database`.`TestView1`"}));
		testInvalidSqlCommand("DROP VIEW");
		testValidSqlCommand("SET", new SqlCommandCall(SqlCommand.SET));
		testValidSqlCommand("SET x=y", new SqlCommandCall(SqlCommand.SET, new String[] {"x", "y"}));
		testValidSqlCommand("SET    x  = y", new SqlCommandCall(SqlCommand.SET, new String[] {"x", " y"}));
		testInvalidSqlCommand("-- comments\nSET x=y");
		testValidSqlCommand("reset;", new SqlCommandCall(SqlCommand.RESET));
		testInvalidSqlCommand("-- comments\nreset");
		testValidSqlCommand("source /my/file", new SqlCommandCall(SqlCommand.SOURCE, new String[] {"/my/file"}));
		testInvalidSqlCommand("-- comments\nsource /my/file");
		testInvalidSqlCommand("source"); // missing path
		testValidSqlCommand("create CATALOG c1 with('type'='generic_in_memory')",
				new SqlCommandCall(SqlCommand.CREATE_CATALOG, new String[]{"create CATALOG c1 with('type'='generic_in_memory')"}));
		testValidSqlCommand("create CATALOG c1 WITH ('type'='simple-catalog', 'default-database'='db1')",
				new SqlCommandCall(SqlCommand.CREATE_CATALOG, new String[]{"create CATALOG c1 WITH ('type'='simple-catalog', 'default-database'='db1')"}));
		testValidSqlCommand("-- comments\ncreate CATALOG c1 with('type'='generic_in_memory')",
				new SqlCommandCall(SqlCommand.CREATE_CATALOG, new String[]{"-- comments\ncreate CATALOG c1 with('type'='generic_in_memory')"}));
		testValidSqlCommand("drop CATALOG c1",
			new SqlCommandCall(SqlCommand.DROP_CATALOG, new String[]{"drop CATALOG c1"}));
		testValidSqlCommand("USE CATALOG default", new SqlCommandCall(SqlCommand.USE_CATALOG, new String[]{"default"}));
		testValidSqlCommand("-- comments\nUSE CATALOG catalog1", new SqlCommandCall(SqlCommand.USE_CATALOG, new String[]{"`catalog1`"}));
		testValidSqlCommand("use default", new SqlCommandCall(SqlCommand.USE, new String[] {"default"}));
		testValidSqlCommand("-- comments\nuse `default`", new SqlCommandCall(SqlCommand.USE, new String[] {"`default_catalog`.`default`"}));
		testInvalidSqlCommand("use catalog");
		testValidSqlCommand("create database db1",
				new SqlCommandCall(SqlCommand.CREATE_DATABASE, new String[] {"create database db1"}));
		testValidSqlCommand("-- comments\ncreate database db1",
				new SqlCommandCall(SqlCommand.CREATE_DATABASE, new String[] {"-- comments\ncreate database db1"}));
		testValidSqlCommand("drop database db1",
				new SqlCommandCall(SqlCommand.DROP_DATABASE, new String[] {"drop database db1"}));
		testValidSqlCommand("-- comments\ndrop database db1",
				new SqlCommandCall(SqlCommand.DROP_DATABASE, new String[] {"-- comments\ndrop database db1"}));
		testValidSqlCommand("alter database default_database set ('k1' = 'a')",
				new SqlCommandCall(SqlCommand.ALTER_DATABASE, new String[] {"alter database default_database set ('k1' = 'a')"}));
		testValidSqlCommand("-- comments\nalter database default_database set ('k1' = 'a')",
				new SqlCommandCall(SqlCommand.ALTER_DATABASE, new String[] {"-- comments\nalter database default_database set ('k1' = 'a')"}));
		testValidSqlCommand("alter table default_catalog.default_database.MyTable rename to tb2",
				new SqlCommandCall(SqlCommand.ALTER_TABLE, new String[]{"alter table default_catalog.default_database.MyTable rename to tb2"}));
		testValidSqlCommand("alter table MyTable set ('k1'='v1', 'k2'='v2')",
				new SqlCommandCall(SqlCommand.ALTER_TABLE,
						new String[]{"alter table MyTable set ('k1'='v1', 'k2'='v2')"}));
		testValidSqlCommand("-- comments\nalter table default_catalog.default_database.MyTable rename to tb2",
				new SqlCommandCall(SqlCommand.ALTER_TABLE, new String[]{"-- comments\nalter table default_catalog.default_database.MyTable rename to tb2"}));
		// Test create table.
		testInvalidSqlCommand("CREATE tables");
		testInvalidSqlCommand("CREATE   tables");
		testValidSqlCommand("create Table hello", new SqlCommandCall(SqlCommand.CREATE_TABLE, new String[]{"create Table hello"}));
		testValidSqlCommand("-- comments\ncreate Table hello", new SqlCommandCall(SqlCommand.CREATE_TABLE, new String[]{"-- comments\ncreate Table hello"}));
		testValidSqlCommand("create Table hello(a int)", new SqlCommandCall(SqlCommand.CREATE_TABLE, new String[]{"create Table hello(a int)"}));
		testValidSqlCommand("  CREATE TABLE hello(a int)", new SqlCommandCall(SqlCommand.CREATE_TABLE, new String[]{"CREATE TABLE hello(a int)"}));
		testValidSqlCommand("CREATE TABLE T(\n"
						+ "  a int,\n"
						+ "  b varchar(20),\n"
						+ "  c as to_timestamp(b),\n"
						+ "  watermark for c as c - INTERVAL '5' second\n"
						+ ") WITH (\n"
						+ "  'k1' = 'v1',\n"
						+ "  'k2' = 'v2')\n",
				new SqlCommandCall(SqlCommand.CREATE_TABLE, new String[] {"CREATE TABLE T(\n"
						+ "  a int,\n"
						+ "  b varchar(20),\n"
						+ "  c as to_timestamp(b),\n"
						+ "  watermark for c as c - INTERVAL '5' second\n"
						+ ") WITH (\n"
						+ "  'k1' = 'v1',\n"
						+ "  'k2' = 'v2')"}));
		// Test drop table.
		testInvalidSqlCommand("DROP table");
		testInvalidSqlCommand("DROP tables");
		testInvalidSqlCommand("DROP   tables");
		testValidSqlCommand("DROP TABLE t1", new SqlCommandCall(SqlCommand.DROP_TABLE, new String[]{"DROP TABLE t1"}));
		testValidSqlCommand("DROP TABLE IF EXISTS t1", new SqlCommandCall(SqlCommand.DROP_TABLE, new String[]{"DROP TABLE IF EXISTS t1"}));
		testValidSqlCommand("DROP TABLE IF EXISTS catalog1.db1.t1", new SqlCommandCall(SqlCommand.DROP_TABLE, new String[]{"DROP TABLE IF EXISTS catalog1.db1.t1"}));
		testValidSqlCommand("DROP TABLE IF EXISTS db1.t1", new SqlCommandCall(SqlCommand.DROP_TABLE, new String[]{"DROP TABLE IF EXISTS db1.t1"}));
		testValidSqlCommand("-- comments\nDROP TABLE IF EXISTS db1.t1", new SqlCommandCall(SqlCommand.DROP_TABLE,
				new String[]{"-- comments\nDROP TABLE IF EXISTS db1.t1"}));
		testValidSqlCommand("SHOW MODULES", new SqlCommandCall(SqlCommand.SHOW_MODULES));
		testValidSqlCommand("  SHOW    MODULES   ", new SqlCommandCall(SqlCommand.SHOW_MODULES));
		testInvalidSqlCommand("-- comments\nSHOW MODULES");
		// Test create function.
		testInvalidSqlCommand("CREATE FUNCTION");
		testInvalidSqlCommand("CREATE FUNCTIONS");
		testInvalidSqlCommand("CREATE    FUNCTIONS");
		testValidSqlCommand("CREATE FUNCTION catalog1.db1.func1 as 'class_name'",
				new SqlCommandCall(SqlCommand.CREATE_FUNCTION, new String[] {"CREATE FUNCTION catalog1.db1.func1 as 'class_name'"}));
		testValidSqlCommand("CREATE TEMPORARY FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA",
				new SqlCommandCall(SqlCommand.CREATE_FUNCTION, new String[] {"CREATE TEMPORARY FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA"}));
		testValidSqlCommand("CREATE TEMPORARY SYSTEM FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA",
				new SqlCommandCall(SqlCommand.CREATE_FUNCTION, new String[] {"CREATE TEMPORARY SYSTEM FUNCTION catalog1.db1.func1 as 'class_name' LANGUAGE JAVA"}));
		// Test drop function.
		testInvalidSqlCommand("DROP FUNCTION");
		testInvalidSqlCommand("DROP FUNCTIONS");
		testInvalidSqlCommand("DROP    FUNCTIONS");
		testValidSqlCommand("DROP FUNCTION catalog1.db1.func1",
				new SqlCommandCall(SqlCommand.DROP_FUNCTION, new String[] {"DROP FUNCTION catalog1.db1.func1"}));
		testValidSqlCommand("DROP TEMPORARY FUNCTION catalog1.db1.func1",
				new SqlCommandCall(SqlCommand.DROP_FUNCTION, new String[] {"DROP TEMPORARY FUNCTION catalog1.db1.func1"}));
		testValidSqlCommand("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1",
				new SqlCommandCall(SqlCommand.DROP_FUNCTION, new String[] {"DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1"}));
		// Test alter function.
		testInvalidSqlCommand("ALTER FUNCTION");
		testInvalidSqlCommand("ALTER FUNCTIONS");
		testInvalidSqlCommand("ALTER    FUNCTIONS");
		testValidSqlCommand("ALTER FUNCTION catalog1.db1.func1 as 'a.b.c.func2'",
				new SqlCommandCall(SqlCommand.ALTER_FUNCTION, new String[] {"ALTER FUNCTION catalog1.db1.func1 as 'a.b.c.func2'"}));
		testValidSqlCommand("ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
				new SqlCommandCall(SqlCommand.ALTER_FUNCTION, new String[] {"ALTER TEMPORARY FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'"}));
		testValidSqlCommand("ALTER TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'",
				new SqlCommandCall(SqlCommand.ALTER_FUNCTION, new String[] {"ALTER TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.db1.func1 as 'a.b.c.func2'"}));

	}

	private void testInvalidSqlCommand(String stmt) {
		final Optional<SqlCommandCall> actualCall = SqlCommandParser.parse(ParserUtils::parse, stmt);
		if (actualCall.isPresent()) {
			fail();
		}
	}

	private void testValidSqlCommand(String stmt, SqlCommandCall expectedCall) {
		final Optional<SqlCommandCall> actualCall = SqlCommandParser.parse(ParserUtils::parse, stmt);
		if (!actualCall.isPresent()) {
			fail();
		}
		assertEquals(expectedCall, actualCall.get());
	}

	private void testInvalidSql(String stmt) {
		try {
			SqlCommandParser.parse(ParserUtils::parse, stmt);
			fail();
		} catch (SqlExecutionException e) {
			assertTrue(e.getCause() instanceof ValidationException);
		}
	}
}
