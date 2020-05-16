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

package org.apache.flink.connectors.hive;

import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.flink.table.api.EnvironmentSettings.DEFAULT_BUILTIN_CATALOG;
import static org.apache.flink.table.api.EnvironmentSettings.DEFAULT_BUILTIN_DATABASE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test Hive syntax when Hive dialect is used.
 */
public class HiveDialectTest {

	private TableEnvironment tableEnv;
	private HiveCatalog hiveCatalog;
	private String warehouse;

	@Before
	public void setup() {
		hiveCatalog = HiveTestUtils.createHiveCatalog();
		hiveCatalog.open();
		warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
		tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
	}

	@After
	public void tearDown() {
		if (hiveCatalog != null) {
			hiveCatalog.close();
		}
		if (warehouse != null) {
			FileUtils.deleteDirectoryQuietly(new File(warehouse));
		}
	}

	@Test
	public void testCreateDatabase() throws Exception {
		tableEnv.executeSql("create database db1 comment 'db1 comment'");
		Database db = hiveCatalog.getHiveDatabase("db1");
		assertEquals("db1 comment", db.getDescription());
		assertFalse(Boolean.parseBoolean(db.getParameters().get(CatalogConfig.IS_GENERIC)));

		String db2Location = warehouse + "/db2_location";
		tableEnv.executeSql(String.format("create database db2 location '%s' with dbproperties('k1'='v1')", db2Location));
		db = hiveCatalog.getHiveDatabase("db2");
		assertEquals(db2Location, new URI(db.getLocationUri()).getPath());
		assertEquals("v1", db.getParameters().get("k1"));
	}

	@Test
	public void testAlterDatabase() throws Exception {
		// alter properties
		tableEnv.executeSql("create database db1 with dbproperties('k1'='v1')");
		tableEnv.executeSql("alter database db1 set dbproperties ('k1'='v11','k2'='v2')");
		Database db = hiveCatalog.getHiveDatabase("db1");
		// there's an extra is_generic property
		assertEquals(3, db.getParametersSize());
		assertEquals("v11", db.getParameters().get("k1"));
		assertEquals("v2", db.getParameters().get("k2"));

		// alter owner
		tableEnv.executeSql("alter database db1 set owner user user1");
		db = hiveCatalog.getHiveDatabase("db1");
		assertEquals("user1", db.getOwnerName());
		assertEquals(PrincipalType.USER, db.getOwnerType());

		tableEnv.executeSql("alter database db1 set owner role role1");
		db = hiveCatalog.getHiveDatabase("db1");
		assertEquals("role1", db.getOwnerName());
		assertEquals(PrincipalType.ROLE, db.getOwnerType());

		// alter location
		if (hiveCatalog.getHiveVersion().compareTo("2.4.0") >= 0) {
			String newLocation = warehouse + "/db1_new_location";
			tableEnv.executeSql(String.format("alter database db1 set location '%s'", newLocation));
			db = hiveCatalog.getHiveDatabase("db1");
			assertEquals(newLocation, locationPath(db.getLocationUri()));
		}
	}

	@Test
	public void testCreateTable() throws Exception {
		String location = warehouse + "/external_location";
		tableEnv.executeSql(String.format(
				"create external table tbl1 (d decimal(10,0),ts timestamp) partitioned by (p string) location '%s' tblproperties('k1'='v1')", location));
		Table hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl1"));
		assertEquals(TableType.EXTERNAL_TABLE.toString(), hiveTable.getTableType());
		assertEquals(1, hiveTable.getPartitionKeysSize());
		assertEquals(location, locationPath(hiveTable.getSd().getLocation()));
		assertEquals("v1", hiveTable.getParameters().get("k1"));
		assertFalse(hiveTable.getParameters().containsKey(SqlCreateHiveTable.TABLE_LOCATION_URI));

		tableEnv.executeSql("create table tbl2 (s struct<ts:timestamp,bin:binary>) stored as orc");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl2"));
		assertEquals(TableType.MANAGED_TABLE.toString(), hiveTable.getTableType());
		assertEquals(OrcSerde.class.getName(), hiveTable.getSd().getSerdeInfo().getSerializationLib());
		assertEquals(OrcInputFormat.class.getName(), hiveTable.getSd().getInputFormat());
		assertEquals(OrcOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());

		tableEnv.executeSql("create table tbl3 (m map<timestamp,binary>) partitioned by (p1 bigint,p2 tinyint) " +
				"row format serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe'");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl3"));
		assertEquals(2, hiveTable.getPartitionKeysSize());
		assertEquals(LazyBinarySerDe.class.getName(), hiveTable.getSd().getSerdeInfo().getSerializationLib());

		tableEnv.executeSql("create table tbl4 (x int,y smallint) row format delimited fields terminated by '|' lines terminated by '\n'");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl4"));
		assertEquals("|", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.FIELD_DELIM));
		assertEquals("\n", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.LINE_DELIM));

		tableEnv.executeSql("create table tbl5 (m map<bigint,string>) row format delimited collection items terminated by ';' " +
				"map keys terminated by ':'");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl5"));
		assertEquals(";", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.COLLECTION_DELIM));
		assertEquals(":", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.MAPKEY_DELIM));
	}

	@Test
	public void testCreateTableWithConstraints() throws Exception {
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		tableEnv.executeSql("create table tbl (x int,y int not null disable novalidate rely,z int not null disable novalidate norely," +
				"constraint pk_name primary key (x) rely)");
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(new ObjectPath("default", "tbl"));
		TableSchema tableSchema = catalogTable.getSchema();
		assertTrue("PK not present", tableSchema.getPrimaryKey().isPresent());
		assertEquals("pk_name", tableSchema.getPrimaryKey().get().getName());
		assertFalse("PK cannot be null", tableSchema.getFieldDataTypes()[0].getLogicalType().isNullable());
		assertFalse("RELY NOT NULL should be reflected in schema",
				tableSchema.getFieldDataTypes()[1].getLogicalType().isNullable());
		assertTrue("NORELY NOT NULL shouldn't be reflected in schema",
				tableSchema.getFieldDataTypes()[2].getLogicalType().isNullable());
	}

	@Test
	public void testInsert() throws Exception {
		// src table
		tableEnv.executeSql("create table src (x int,y string)");
		waitForJobFinish(tableEnv.executeSql("insert into src values (1,'a'),(2,'b'),(3,'c')"));

		// non-partitioned dest table
		tableEnv.executeSql("create table dest (x int)");
		waitForJobFinish(tableEnv.executeSql("insert into dest select x from src"));
		List<Row> results = queryResult(tableEnv.sqlQuery("select * from dest"));
		assertEquals("[1, 2, 3]", results.toString());
		waitForJobFinish(tableEnv.executeSql("insert overwrite dest values (3),(4),(5)"));
		results = queryResult(tableEnv.sqlQuery("select * from dest"));
		assertEquals("[3, 4, 5]", results.toString());

		// partitioned dest table
		tableEnv.executeSql("create table dest2 (x int) partitioned by (p1 int,p2 string)");
		waitForJobFinish(tableEnv.executeSql("insert into dest2 partition (p1=0,p2='static') select x from src"));
		results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
		assertEquals("[1,0,static, 2,0,static, 3,0,static]", results.toString());
		waitForJobFinish(tableEnv.executeSql("insert into dest2 partition (p1=1,p2) select x,y from src"));
		results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
		assertEquals("[1,0,static, 1,1,a, 2,0,static, 2,1,b, 3,0,static, 3,1,c]", results.toString());
		waitForJobFinish(tableEnv.executeSql("insert overwrite dest2 partition (p1,p2) select 1,x,y from src"));
		results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
		assertEquals("[1,0,static, 1,1,a, 1,2,b, 1,3,c, 2,0,static, 2,1,b, 3,0,static, 3,1,c]", results.toString());
	}

	private static List<Row> queryResult(org.apache.flink.table.api.Table table) {
		return Lists.newArrayList(table.execute().collect());
	}

	private static void waitForJobFinish(TableResult tableResult) throws Exception {
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
	}

	@Test
	public void testFunction() throws Exception {
		// create function
		tableEnv.executeSql(String.format("create function my_abs as '%s'", GenericUDFAbs.class.getName()));
		List<Row> functions = Lists.newArrayList(tableEnv.executeSql("show functions").collect());
		assertTrue(functions.toString().contains("my_abs"));
		// call the function
		tableEnv.executeSql("create table src(x int)");
		waitForJobFinish(tableEnv.executeSql("insert into src values (1),(-1)"));
		assertEquals("[1, 1]", queryResult(tableEnv.sqlQuery("select my_abs(x) from src")).toString());
		// drop the function
		tableEnv.executeSql("drop function my_abs");
		assertFalse(hiveCatalog.functionExists(new ObjectPath("default", "my_abs")));
		tableEnv.executeSql("drop function if exists foo");
	}

	@Test
	public void testCatalog() {
		List<Row> catalogs = Lists.newArrayList(tableEnv.executeSql("show catalogs").collect());
		assertEquals(2, catalogs.size());
		tableEnv.executeSql("use catalog " + DEFAULT_BUILTIN_CATALOG);
		List<Row> databases = Lists.newArrayList(tableEnv.executeSql("show databases").collect());
		assertEquals(1, databases.size());
		assertEquals(DEFAULT_BUILTIN_DATABASE, databases.get(0).toString());
	}

	private static String locationPath(String locationURI) throws URISyntaxException {
		return new URI(locationURI).getPath();
	}
}
