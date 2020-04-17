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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.impl.FlinkHiveSqlParserImpl;

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
				.ok("CREATE DATABASE `DB1` WITH (\n" +
						"  'is_generic' = 'false'\n" +
						")");
		sql("create database db1 comment 'comment db1' location '/path/to/db1'")
				.ok("CREATE DATABASE `DB1`\n" +
						"COMMENT 'comment db1' WITH (\n" +
						"  'is_generic' = 'false',\n" +
						"  'database.location_uri' = '/path/to/db1'\n" +
						")");
		sql("create database db1 with dbproperties ('k1'='v1','k2'='v2')")
				.ok("CREATE DATABASE `DB1` WITH (\n" +
						"  'k1' = 'v1',\n" +
						"  'k2' = 'v2',\n" +
						"  'is_generic' = 'false'\n" +
						")");
	}

	@Test
	public void testAlterDatabase() {
		sql("alter database db1 set dbproperties('k1'='v1')")
				.ok("ALTER DATABASE `DB1` SET (\n" +
						"  'k1' = 'v1',\n" +
						"  'alter.database.op' = 'CHANGE_PROPS'\n" +
						")");
		sql("alter database db1 set location '/new/path'")
				.ok("ALTER DATABASE `DB1` SET (\n" +
						"  'alter.database.op' = 'CHANGE_LOCATION',\n" +
						"  'database.location_uri' = '/new/path'\n" +
						")");
		sql("alter database db1 set owner user user1")
				.ok("ALTER DATABASE `DB1` SET (\n" +
						"  'alter.database.op' = 'CHANGE_OWNER',\n" +
						"  'database.owner.type' = 'user',\n" +
						"  'database.owner.name' = 'USER1'\n" +
						")");
		sql("alter database db1 set owner role role1")
				.ok("ALTER DATABASE `DB1` SET (\n" +
						"  'alter.database.op' = 'CHANGE_OWNER',\n" +
						"  'database.owner.type' = 'role',\n" +
						"  'database.owner.name' = 'ROLE1'\n" +
						")");
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
}
