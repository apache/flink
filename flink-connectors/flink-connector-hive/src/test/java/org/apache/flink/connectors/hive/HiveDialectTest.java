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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

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
		tableEnv.sqlUpdate(String.format(
				"create external table tbl1 (d decimal(10,0),ts timestamp) partitioned by (p string) location '%s' tblproperties('k1'='v1')", location));
		Table hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl1"));
		assertEquals(TableType.EXTERNAL_TABLE.toString(), hiveTable.getTableType());
		assertEquals(1, hiveTable.getPartitionKeysSize());
		assertEquals(location, locationPath(hiveTable.getSd().getLocation()));
		assertEquals("v1", hiveTable.getParameters().get("k1"));
		assertFalse(hiveTable.getParameters().containsKey(SqlCreateHiveTable.TABLE_LOCATION_URI));

		tableEnv.sqlUpdate("create table tbl2 (s struct<ts:timestamp,bin:binary>) stored as orc");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl2"));
		assertEquals(TableType.MANAGED_TABLE.toString(), hiveTable.getTableType());
		assertEquals(OrcSerde.class.getName(), hiveTable.getSd().getSerdeInfo().getSerializationLib());
		assertEquals(OrcInputFormat.class.getName(), hiveTable.getSd().getInputFormat());
		assertEquals(OrcOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());

		tableEnv.sqlUpdate("create table tbl3 (m map<timestamp,binary>) partitioned by (p1 bigint,p2 tinyint) " +
				"row format serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe'");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl3"));
		assertEquals(2, hiveTable.getPartitionKeysSize());
		assertEquals(LazyBinarySerDe.class.getName(), hiveTable.getSd().getSerdeInfo().getSerializationLib());

		tableEnv.sqlUpdate("create table tbl4 (x int,y smallint) row format delimited fields terminated by '|' lines terminated by '\n'");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl4"));
		assertEquals("|", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.FIELD_DELIM));
		assertEquals("\n", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.LINE_DELIM));

		tableEnv.sqlUpdate("create table tbl5 (m map<bigint,string>) row format delimited collection items terminated by ';' " +
				"map keys terminated by ':'");
		hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl5"));
		assertEquals(";", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.COLLECTION_DELIM));
		assertEquals(":", hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.MAPKEY_DELIM));
	}

	@Test
	public void testCreateTableWithConstraints() throws Exception {
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		tableEnv.sqlUpdate("create table tbl (x int,y int not null disable novalidate rely,z int not null disable novalidate norely," +
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

	private static String locationPath(String locationURI) throws URISyntaxException {
		return new URI(locationURI).getPath();
	}
}
