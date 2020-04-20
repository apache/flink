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

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.flink.api.java.io.jdbc.catalog.PostgresCatalog.DEFAULT_DATABASE;

/**
 * Test base for {@link PostgresCatalog}.
 */
public class PostgresCatalogTestBase {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@ClassRule
	public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

	protected static final String TEST_CATALOG_NAME = "mypg";
	protected static final String TEST_USERNAME = "postgres";
	protected static final String TEST_PWD = "postgres";
	protected static final String TEST_DB = "test";
	protected static final String TEST_SCHEMA = "test_schema";
	protected static final String TABLE1 = "t1";
	protected static final String TABLE2 = "t2";
	protected static final String TABLE3 = "t3";
	protected static final String TABLE4 = "t4";
	protected static final String TABLE5 = "t5";
	protected static final String TABLE_PRIMITIVE_TYPE = "dt";

	protected static String baseUrl;
	protected static PostgresCatalog catalog;

	@BeforeClass
	public static void init() throws SQLException {
		// jdbc:postgresql://localhost:50807/postgres?user=postgres
		String embeddedJdbcUrl = pg.getEmbeddedPostgres().getJdbcUrl(TEST_USERNAME, TEST_PWD);
		// jdbc:postgresql://localhost:50807/
		baseUrl = embeddedJdbcUrl.substring(0, embeddedJdbcUrl.lastIndexOf("/") + 1);

		catalog = new PostgresCatalog(TEST_CATALOG_NAME, PostgresCatalog.DEFAULT_DATABASE, TEST_USERNAME, TEST_PWD, baseUrl);

		// create test database and schema
		createDatabase(TEST_DB);
		createSchema(TEST_DB, TEST_SCHEMA);

		// create test tables
		// table: postgres.public.t1
		// table: postgres.public.t4
		// table: postgres.public.t5
		createTable(PostgresTablePath.fromFlinkTableName(TABLE1), getSimpleTable().pgSchemaSql);
		createTable(PostgresTablePath.fromFlinkTableName(TABLE4), getSimpleTable().pgSchemaSql);
		createTable(PostgresTablePath.fromFlinkTableName(TABLE5), getSimpleTable().pgSchemaSql);

		// table: test.public.t2
		// table: test.test_schema.t3
		// table: postgres.public.dt
		createTable(TEST_DB, PostgresTablePath.fromFlinkTableName(TABLE2), getSimpleTable().pgSchemaSql);
		createTable(TEST_DB, new PostgresTablePath(TEST_SCHEMA, TABLE3), getSimpleTable().pgSchemaSql);
		createTable(PostgresTablePath.fromFlinkTableName(TABLE_PRIMITIVE_TYPE), getPrimitiveTable().pgSchemaSql);

		executeSQL(DEFAULT_DATABASE, String.format("insert into public.%s values (%s);", TABLE1, getSimpleTable().values));
		executeSQL(DEFAULT_DATABASE, String.format("insert into %s values (%s);", TABLE_PRIMITIVE_TYPE, getPrimitiveTable().values));
	}

	public static void createTable(PostgresTablePath tablePath, String tableSchemaSql) throws SQLException {
		executeSQL(PostgresCatalog.DEFAULT_DATABASE, String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
	}

	public static void createTable(String db, PostgresTablePath tablePath, String tableSchemaSql) throws SQLException {
		executeSQL(db, String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
	}

	public static void createSchema(String db, String schema) throws SQLException {
		executeSQL(db, String.format("CREATE SCHEMA %s", schema));
	}

	public static void createDatabase(String database) throws SQLException {
		executeSQL(String.format("CREATE DATABASE %s;", database));
	}

	public static void executeSQL(String sql) throws SQLException {
		executeSQL("", sql);
	}

	public static void executeSQL(String db, String sql) throws SQLException {
		try (Connection conn = DriverManager.getConnection(baseUrl + db, TEST_USERNAME, TEST_PWD);
			Statement statement = conn.createStatement()) {
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw e;
		}
	}

	/**
	 * Object holding schema and corresponding sql.
	 */
	public static class TestTable {
		TableSchema schema;
		String pgSchemaSql;
		String values;

		public TestTable(TableSchema schema, String pgSchemaSql, String values) {
			this.schema = schema;
			this.pgSchemaSql = pgSchemaSql;
			this.values = values;
		}
	}

	public static TestTable getSimpleTable() {
		return new TestTable(
			TableSchema.builder()
				.field("id", DataTypes.INT())
				.build(),
			"id integer",
			"1"
		);
	}

	// TODO: add back timestamptz and time types.
	//  Flink currently doens't support converting time's precision, with the following error
	//  TableException: Unsupported conversion from data type 'TIME(6)' (conversion class: java.sql.Time)
	//  to type information. Only data types that originated from type information fully support a reverse conversion.
	public static TestTable getPrimitiveTable() {
		return new TestTable(
			TableSchema.builder()
				.field("int", DataTypes.INT())
				.field("bytea", DataTypes.BYTES())
				.field("short", DataTypes.SMALLINT())
				.field("long", DataTypes.BIGINT())
				.field("real", DataTypes.FLOAT())
				.field("double_precision", DataTypes.DOUBLE())
				.field("numeric", DataTypes.DECIMAL(10, 5))
				.field("boolean", DataTypes.BOOLEAN())
				.field("text", DataTypes.STRING())
				.field("char", DataTypes.CHAR(1))
				.field("character", DataTypes.CHAR(3))
				.field("character_varying", DataTypes.VARCHAR(20))
				.field("timestamp", DataTypes.TIMESTAMP(5))
//				.field("timestamptz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(4))
				.field("date", DataTypes.DATE())
				.field("time", DataTypes.TIME(0))
				.build(),
			"int integer, " +
				"bytea bytea, " +
				"short smallint, " +
				"long bigint, " +
				"real real, " +
				"double_precision double precision, " +
				"numeric numeric(10, 5), " +
				"boolean boolean, " +
				"text text, " +
				"char char, " +
				"character character(3), " +
				"character_varying character varying(20), " +
				"timestamp timestamp(5), " +
//				"timestamptz timestamptz(4), " +
				"date date," +
				"time time(0)",
			"1," +
				"'2'," +
				"3," +
				"4," +
				"5.5," +
				"6.6," +
				"7.7," +
				"true," +
				"'a'," +
				"'b'," +
				"'c'," +
				"'d'," +
				"'2016-06-22 19:10:25'," +
//				"'2006-06-22 19:10:25'," +
				"'2015-01-01'," +
				"'00:51:02.746572'"
		);
	}

}
