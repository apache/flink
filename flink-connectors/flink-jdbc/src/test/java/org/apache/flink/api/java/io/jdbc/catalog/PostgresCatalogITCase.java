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

package org.apache.flink.api.java.io.jdbc.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link PostgresCatalog}.
 */
public class PostgresCatalogITCase {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@ClassRule
	public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

	protected static final String TEST_USERNAME = "postgres";
	protected static final String TEST_PWD = "postgres";
	protected static final String TEST_DB = "test";
	protected static final String TEST_SCHEMA = "test_schema";
	protected static final String TABLE1 = "t1";
	protected static final String TABLE2 = "t2";
	protected static final String TABLE3 = "t3";

	protected static String baseUrl;
	protected static Catalog catalog;

	public static Catalog createCatalog(String name, String defaultDb, String username, String pwd, String jdbcUrl) {
		return new PostgresCatalog("mypg", PostgresCatalog.DEFAULT_DATABASE, username, pwd, jdbcUrl);
	}

	@BeforeClass
	public static void setup() throws SQLException {
		// jdbc:postgresql://localhost:50807/postgres?user=postgres
		String embeddedJdbcUrl = pg.getEmbeddedPostgres().getJdbcUrl(TEST_USERNAME, TEST_PWD);
		// jdbc:postgresql://localhost:50807/
		baseUrl = embeddedJdbcUrl.substring(0, embeddedJdbcUrl.lastIndexOf("/") + 1);

		catalog = createCatalog("mypg", PostgresCatalog.DEFAULT_DATABASE, TEST_USERNAME, TEST_PWD, baseUrl);

		// create test database and schema
		createDatabase(TEST_DB);
		createSchema(TEST_DB, TEST_SCHEMA);

		// create test tables
		// table: postgres.public.user1
		createTable(PostgresTablePath.fromFlinkTableName(TABLE1), getSimpleTable().pgSchemaSql);

		// table: testdb.public.user2
		// table: testdb.testschema.user3
		// table: testdb.public.datatypes
		createTable(TEST_DB, PostgresTablePath.fromFlinkTableName(TABLE2), getSimpleTable().pgSchemaSql);
		createTable(TEST_DB, new PostgresTablePath(TEST_SCHEMA, TABLE3), getSimpleTable().pgSchemaSql);
		createTable(TEST_DB, PostgresTablePath.fromFlinkTableName("datatypes"), getDataTypesTable().pgSchemaSql);
	}

	// ------ databases ------

	@Test
	public void testGetDb_DatabaseNotExistException() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database nonexistent does not exist in Catalog");
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testListDatabases() {
		List<String> actual = catalog.listDatabases();

		assertEquals(
			Arrays.asList("postgres", "test"),
			actual
		);
	}

	@Test
	public void testDbExists() throws Exception {
		assertFalse(catalog.databaseExists("nonexistent"));

		assertTrue(catalog.databaseExists(PostgresCatalog.DEFAULT_DATABASE));
	}

	// ------ tables ------

	@Test
	public void testListTables() throws DatabaseNotExistException {
		List<String> actual = catalog.listTables(PostgresCatalog.DEFAULT_DATABASE);

		assertEquals(Arrays.asList("public.t1"), actual);

		actual = catalog.listTables(TEST_DB);

		assertEquals(Arrays.asList("public.datatypes", "public.t2", "test_schema.t3"), actual);
	}

	@Test
	public void testListTables_DatabaseNotExistException() throws DatabaseNotExistException {
		exception.expect(DatabaseNotExistException.class);
		catalog.listTables("postgres/nonexistschema");
	}

	@Test
	public void testTableExists() {
		assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, "nonexist")));

		assertTrue(catalog.tableExists(new ObjectPath(PostgresCatalog.DEFAULT_DATABASE, TABLE1)));
		assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, TABLE2)));
		assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, "test_schema.t3")));
	}

	@Test
	public void testGetTables_TableNotExistException() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath(TEST_DB, PostgresTablePath.toFlinkTableName(TEST_SCHEMA, "anytable")));
	}

	@Test
	public void testGetTables_TableNotExistException_NoSchema() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath(TEST_DB, PostgresTablePath.toFlinkTableName("nonexistschema", "anytable")));
	}

	@Test
	public void testGetTables_TableNotExistException_NoDb() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath("nonexistdb", PostgresTablePath.toFlinkTableName(TEST_SCHEMA, "anytable")));
	}

	@Test
	public void testGetTable() throws org.apache.flink.table.catalog.exceptions.TableNotExistException {
		// test postgres.public.user1
		TableSchema schema = getSimpleTable().schema;

		CatalogBaseTable table = catalog.getTable(new ObjectPath("postgres", TABLE1));

		assertEquals(schema, table.getSchema());

		table = catalog.getTable(new ObjectPath("postgres", "public.t1"));

		assertEquals(schema, table.getSchema());

		// test testdb.public.user2
		table = catalog.getTable(new ObjectPath(TEST_DB, TABLE2));

		assertEquals(schema, table.getSchema());

		table = catalog.getTable(new ObjectPath(TEST_DB, "public.t2"));

		assertEquals(schema, table.getSchema());

		// test testdb.testschema.user2
		table = catalog.getTable(new ObjectPath(TEST_DB, TEST_SCHEMA + ".t3"));

		assertEquals(schema, table.getSchema());

	}

	@Test
	public void testDataTypes() throws TableNotExistException {
		CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, "datatypes"));

		assertEquals(getDataTypesTable().schema, table.getSchema());
	}

	private static class TestTable {
		TableSchema schema;
		String pgSchemaSql;

		public TestTable(TableSchema schema, String pgSchemaSql) {
			this.schema = schema;
			this.pgSchemaSql = pgSchemaSql;
		}
	}

	private static TestTable getSimpleTable() {
		return new TestTable(
			TableSchema.builder()
				.field("name", DataTypes.INT())
				.build(),
			"name integer"
		);
	}

	private static TestTable getDataTypesTable() {
		return new TestTable(
			TableSchema.builder()
				.field("int", DataTypes.INT())
				.field("int_arr", DataTypes.ARRAY(DataTypes.INT()))
				.field("bytea", DataTypes.BYTES())
				.field("bytea_arr", DataTypes.ARRAY(DataTypes.BYTES()))
				.field("short", DataTypes.SMALLINT())
				.field("short_arr", DataTypes.ARRAY(DataTypes.SMALLINT()))
				.field("long", DataTypes.BIGINT())
				.field("long_arr", DataTypes.ARRAY(DataTypes.BIGINT()))
				.field("real", DataTypes.FLOAT())
				.field("real_arr", DataTypes.ARRAY(DataTypes.FLOAT()))
				.field("double_precision", DataTypes.DOUBLE())
				.field("double_precision_arr", DataTypes.ARRAY(DataTypes.DOUBLE()))
				.field("numeric", DataTypes.DECIMAL(10, 5))
				.field("numeric_arr", DataTypes.ARRAY(DataTypes.DECIMAL(10, 5)))
				.field("boolean", DataTypes.BOOLEAN())
				.field("boolean_arr", DataTypes.ARRAY(DataTypes.BOOLEAN()))
				.field("text", DataTypes.STRING())
				.field("text_arr", DataTypes.ARRAY(DataTypes.STRING()))
				.field("char", DataTypes.CHAR(1))
				.field("char_arr", DataTypes.ARRAY(DataTypes.CHAR(1)))
				.field("character", DataTypes.CHAR(3))
				.field("character_arr", DataTypes.ARRAY(DataTypes.CHAR(3)))
				.field("character_varying", DataTypes.VARCHAR(20))
				.field("character_varying_arr", DataTypes.ARRAY(DataTypes.VARCHAR(20)))
				.field("timestamp", DataTypes.TIMESTAMP(5))
				.field("timestamp_arr", DataTypes.ARRAY(DataTypes.TIMESTAMP(5)))
				.field("timestamptz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(4))
				.field("timestamptz_arr", DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(4)))
				.field("date", DataTypes.DATE())
				.field("date_arr", DataTypes.ARRAY(DataTypes.DATE()))
				.field("time", DataTypes.TIME(3))
				.field("time_arr", DataTypes.ARRAY(DataTypes.TIME(3)))
				.build(),
			"int integer, " +
				"int_arr integer[], " +
				"bytea bytea, " +
				"bytea_arr bytea[], " +
				"short smallint, " +
				"short_arr smallint[], " +
				"long bigint, " +
				"long_arr bigint[], " +
				"real real, " +
				"real_arr real[], " +
				"double_precision double precision, " +
				"double_precision_arr double precision[], " +
				"numeric numeric(10, 5), " +
				"numeric_arr numeric(10, 5)[], " +
				"boolean boolean, " +
				"boolean_arr boolean[], " +
				"text text, " +
				"text_arr text[], " +
				"char char, " +
				"char_arr char[], " +
				"character character(3), " +
				"character_arr character(3)[], " +
				"character_varying character varying(20), " +
				"character_varying_arr character varying(20)[], " +
				"timestamp timestamp(5), " +
				"timestamp_arr timestamp(5)[], " +
				"timestamptz timestamptz(4), " +
				"timestamptz_arr timestamptz(4)[], " +
				"date date, " +
				"date_arr date[], " +
				"time time(3), " +
				"time_arr time(3)[]"
		);
	}

	private static void createTable(PostgresTablePath tablePath, String tableSchemaSql) throws SQLException {
		executeSQL(PostgresCatalog.DEFAULT_DATABASE, String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
	}

	private static void createTable(String db, PostgresTablePath tablePath, String tableSchemaSql) throws SQLException {
		executeSQL(db, String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
	}

	private static void createSchema(String db, String schema) throws SQLException {
		executeSQL(db, String.format("CREATE SCHEMA %s", schema));
	}

	private static void createDatabase(String database) throws SQLException {
		executeSQL(String.format("CREATE DATABASE %s;", database));
	}

	private static void executeSQL(String sql) throws SQLException {
		executeSQL("", sql);
	}

	private static void executeSQL(String db, String sql) throws SQLException {
		try (Connection conn = DriverManager.getConnection(baseUrl + db, TEST_USERNAME, TEST_PWD);
				Statement statement = conn.createStatement()) {
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		}
	}

}
