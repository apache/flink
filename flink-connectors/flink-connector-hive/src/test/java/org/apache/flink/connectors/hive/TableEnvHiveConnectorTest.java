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

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.types.Row;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test hive connector with table API.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class TableEnvHiveConnectorTest {

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	private static HiveCatalog hiveCatalog;
	private static HiveMetastoreClientWrapper hmsClient;

	@BeforeClass
	public static void setup() {
		HiveConf hiveConf = hiveShell.getHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
		hmsClient = HiveMetastoreClientFactory.create(hiveConf, HiveShimLoader.getHiveVersion());
	}

	@Test
	public void testDefaultPartitionName() throws Exception {
		hiveShell.execute("create database db1");
		hiveShell.execute("create table db1.src (x int, y int)");
		hiveShell.execute("create table db1.part (x int) partitioned by (y int)");
		HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src").addRow(new Object[]{1, 1}).addRow(new Object[]{2, null}).commit();

		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();

		// test generating partitions with default name
		tableEnv.sqlUpdate("insert into db1.part select * from db1.src");
		tableEnv.execute("mytest");
		HiveConf hiveConf = hiveShell.getHiveConf();
		String defaultPartName = hiveConf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
		Table hiveTable = hmsClient.getTable("db1", "part");
		Path defaultPartPath = new Path(hiveTable.getSd().getLocation(), "y=" + defaultPartName);
		FileSystem fs = defaultPartPath.getFileSystem(hiveConf);
		assertTrue(fs.exists(defaultPartPath));

		TableImpl flinkTable = (TableImpl) tableEnv.sqlQuery("select y, x from db1.part order by x");
		List<Row> rows = TableUtils.collectToList(flinkTable);
		assertEquals(Arrays.toString(new String[]{"1,1", "null,2"}), rows.toString());

		hiveShell.execute("drop database db1 cascade");
	}

	@Test
	public void testGetNonExistingFunction() throws Exception {
		hiveShell.execute("create database db1");
		hiveShell.execute("create table db1.src (d double, s string)");
		hiveShell.execute("create table db1.dest (x bigint)");

		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();

		// just make sure the query runs through, no need to verify result
		tableEnv.sqlUpdate("insert into db1.dest select count(d) from db1.src");
		tableEnv.execute("test");

		hiveShell.execute("drop database db1 cascade");
	}

	@Test
	public void testDifferentFormats() throws Exception {
		String[] formats = new String[]{"orc", "parquet", "sequencefile", "csv"};
		for (String format : formats) {
			if (format.equals("orc") && HiveShimLoader.getHiveVersion().startsWith("2.0")) {
				// Ignore orc test for Hive version 2.0.x for now due to FLINK-13998
				continue;
			}
			readWriteFormat(format);
		}
	}

	private void readWriteFormat(String format) throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();

		hiveShell.execute("create database db1");

		// create source and dest tables
		String suffix;
		if (format.equals("csv")) {
			suffix = "row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'";
		} else {
			suffix = "stored as " + format;
		}
		hiveShell.execute("create table db1.src (i int,s string,ts timestamp) " + suffix);
		hiveShell.execute("create table db1.dest (i int,s string,ts timestamp) " + suffix);

		// prepare source data with Hive
		// TABLE keyword in INSERT INTO is mandatory prior to 1.1.0
		hiveShell.execute("insert into table db1.src values (1,'a','2018-08-20 00:00:00.1'),(2,'b','2019-08-26 00:00:00.1')");

		// populate dest table with source table
		tableEnv.sqlUpdate("insert into db1.dest select * from db1.src");
		tableEnv.execute("test_" + format);

		// verify data on hive side
		verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta\t2018-08-20 00:00:00.1", "2\tb\t2019-08-26 00:00:00.1"));

		hiveShell.execute("drop database db1 cascade");
	}

	@Test
	public void testDecimal() throws Exception {
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.src1 (x decimal(10,2))");
			hiveShell.execute("create table db1.src2 (x decimal(10,2))");
			hiveShell.execute("create table db1.dest (x decimal(10,2))");
			// populate src1 from Hive
			// TABLE keyword in INSERT INTO is mandatory prior to 1.1.0
			hiveShell.execute("insert into table db1.src1 values (1.0),(2.12),(5.123),(5.456),(123456789.12)");

			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			// populate src2 with same data from Flink
			tableEnv.sqlUpdate("insert into db1.src2 values (cast(1.0 as decimal(10,2))), (cast(2.12 as decimal(10,2))), " +
					"(cast(5.123 as decimal(10,2))), (cast(5.456 as decimal(10,2))), (cast(123456789.12 as decimal(10,2)))");
			tableEnv.execute("test1");
			// verify src1 and src2 contain same data
			verifyHiveQueryResult("select * from db1.src2", hiveShell.executeQuery("select * from db1.src1"));

			// populate dest with src1 from Flink -- to test reading decimal type from Hive
			tableEnv.sqlUpdate("insert into db1.dest select * from db1.src1");
			tableEnv.execute("test2");
			verifyHiveQueryResult("select * from db1.dest", hiveShell.executeQuery("select * from db1.src1"));
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testInsertOverwrite() throws Exception {
		hiveShell.execute("create database db1");
		try {
			// non-partitioned
			hiveShell.execute("create table db1.dest (x int, y string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "dest").addRow(new Object[]{1, "a"}).addRow(new Object[]{2, "b"}).commit();
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta", "2\tb"));
			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			tableEnv.sqlUpdate("insert overwrite db1.dest values (3,'c')");
			tableEnv.execute("test insert overwrite");
			verifyHiveQueryResult("select * from db1.dest", Collections.singletonList("3\tc"));

			// static partition
			hiveShell.execute("create table db1.part(x int) partitioned by (y int)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part").addRow(new Object[]{1}).commit("y=1");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part").addRow(new Object[]{2}).commit("y=2");
			tableEnv = getTableEnvWithHiveCatalog();
			tableEnv.sqlUpdate("insert overwrite db1.part partition (y=1) select 100");
			tableEnv.execute("insert overwrite static partition");
			verifyHiveQueryResult("select * from db1.part", Arrays.asList("100\t1", "2\t2"));

			// dynamic partition
			tableEnv = getTableEnvWithHiveCatalog();
			tableEnv.sqlUpdate("insert overwrite db1.part values (200,2),(3,3)");
			tableEnv.execute("insert overwrite dynamic partition");
			// only overwrite dynamically matched partitions, other existing partitions remain intact
			verifyHiveQueryResult("select * from db1.part", Arrays.asList("100\t1", "200\t2", "3\t3"));
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testStaticPartition() throws Exception {
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.src (x int)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src").addRow(new Object[]{1}).addRow(new Object[]{2}).commit();
			hiveShell.execute("create table db1.dest (x int) partitioned by (p1 string, p2 double)");
			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			tableEnv.sqlUpdate("insert into db1.dest partition (p1='1''1', p2=1.1) select x from db1.src");
			tableEnv.execute("static partitioning");
			assertEquals(1, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\t1'1\t1.1", "2\t1'1\t1.1"));
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testDynamicPartition() throws Exception {
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.src (x int, y string, z double)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{1, "a", 1.1})
					.addRow(new Object[]{2, "a", 2.2})
					.addRow(new Object[]{3, "b", 3.3})
					.commit();
			hiveShell.execute("create table db1.dest (x int) partitioned by (p1 string, p2 double)");
			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			tableEnv.sqlUpdate("insert into db1.dest select * from db1.src");
			tableEnv.execute("dynamic partitioning");
			assertEquals(3, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta\t1.1", "2\ta\t2.2", "3\tb\t3.3"));
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testPartialDynamicPartition() throws Exception {
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.src (x int, y string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src").addRow(new Object[]{1, "a"}).addRow(new Object[]{2, "b"}).commit();
			hiveShell.execute("create table db1.dest (x int) partitioned by (p1 double, p2 string)");
			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			tableEnv.sqlUpdate("insert into db1.dest partition (p1=1.1) select x,y from db1.src");
			tableEnv.execute("partial dynamic partitioning");
			assertEquals(2, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\t1.1\ta", "2\t1.1\tb"));
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testUDTF() throws Exception {
		// W/o https://issues.apache.org/jira/browse/HIVE-11878 Hive registers the App classloader as the classloader
		// for the UDTF and closes the App classloader when we tear down the session. This causes problems for JUnit code
		// and shutdown hooks that have to run after the test finishes, because App classloader can no longer load new
		// classes. And will crash the forked JVM, thus failing the test phase.
		// Therefore disable such tests for older Hive versions.
		String hiveVersion = HiveShimLoader.getHiveVersion();
		Assume.assumeTrue(hiveVersion.compareTo("2.0.0") >= 0 || hiveVersion.compareTo("1.3.0") >= 0);
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.simple (i int,a array<int>)");
			hiveShell.execute("create table db1.nested (a array<map<int, string>>)");
			hiveShell.execute("create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
			hiveShell.insertInto("db1", "simple").addRow(3, Arrays.asList(1, 2, 3)).commit();
			Map<Integer, String> map1 = new HashMap<>();
			map1.put(1, "a");
			map1.put(2, "b");
			Map<Integer, String> map2 = new HashMap<>();
			map2.put(3, "c");
			hiveShell.insertInto("db1", "nested").addRow(Arrays.asList(map1, map2)).commit();

			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			List<Row> results = HiveTestUtils.collectTable(tableEnv,
					tableEnv.sqlQuery("select x from db1.simple, lateral table(hiveudtf(a)) as T(x)"));
			assertEquals("[1, 2, 3]", results.toString());
			results = HiveTestUtils.collectTable(tableEnv,
					tableEnv.sqlQuery("select x from db1.nested, lateral table(hiveudtf(a)) as T(x)"));
			assertEquals("[{1=a, 2=b}, {3=c}]", results.toString());

			hiveShell.execute("create table db1.ts (a array<timestamp>)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "ts").addRow(new Object[]{
					new Object[]{Timestamp.valueOf("2015-04-28 15:23:00"), Timestamp.valueOf("2016-06-03 17:05:52")}})
					.commit();
			results = HiveTestUtils.collectTable(tableEnv,
					tableEnv.sqlQuery("select x from db1.ts, lateral table(hiveudtf(a)) as T(x)"));
			assertEquals("[2015-04-28T15:23, 2016-06-03T17:05:52]", results.toString());
		} finally {
			hiveShell.execute("drop database db1 cascade");
			hiveShell.execute("drop function hiveudtf");
		}
	}

	@Test
	public void testNotNullConstraints() throws Exception {
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.tbl (x int,y bigint not null enable rely,z string not null enable norely)");
			CatalogBaseTable catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl"));
			TableSchema tableSchema = catalogTable.getSchema();
			assertTrue("By default columns should be nullable",
					tableSchema.getFieldDataTypes()[0].getLogicalType().isNullable());
			assertFalse("NOT NULL columns should be reflected in table schema",
					tableSchema.getFieldDataTypes()[1].getLogicalType().isNullable());
			assertTrue("NOT NULL NORELY columns should be considered nullable",
					tableSchema.getFieldDataTypes()[2].getLogicalType().isNullable());
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testPKConstraint() throws Exception {
		// While PK constraints are supported since Hive 2.1.0, the constraints cannot be RELY in 2.x versions.
		// So let's only test for 3.x.
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		hiveShell.execute("create database db1");
		try {
			// test rely PK constraints
			hiveShell.execute("create table db1.tbl1 (x tinyint,y smallint,z int, primary key (x,z) disable novalidate rely)");
			CatalogBaseTable catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl1"));
			TableSchema tableSchema = catalogTable.getSchema();
			assertTrue(tableSchema.getPrimaryKey().isPresent());
			UniqueConstraint pk = tableSchema.getPrimaryKey().get();
			assertEquals(2, pk.getColumns().size());
			assertTrue(pk.getColumns().containsAll(Arrays.asList("x", "z")));

			// test norely PK constraints
			hiveShell.execute("create table db1.tbl2 (x tinyint,y smallint, primary key (x) disable norely)");
			catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl2"));
			tableSchema = catalogTable.getSchema();
			assertFalse(tableSchema.getPrimaryKey().isPresent());

			// test table w/o PK
			hiveShell.execute("create table db1.tbl3 (x tinyint)");
			catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl3"));
			tableSchema = catalogTable.getSchema();
			assertFalse(tableSchema.getPrimaryKey().isPresent());
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test
	public void testTimestamp() throws Exception {
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.src (ts timestamp)");
			hiveShell.execute("create table db1.dest (ts timestamp)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{Timestamp.valueOf("2019-11-11 00:00:00")})
					.addRow(new Object[]{Timestamp.valueOf("2019-12-03 15:43:32.123456789")})
					.commit();
			TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
			// test read timestamp from hive
			List<Row> results = HiveTestUtils.collectTable(tableEnv, tableEnv.sqlQuery("select * from db1.src"));
			assertEquals(2, results.size());
			assertEquals(LocalDateTime.of(2019, 11, 11, 0, 0), results.get(0).getField(0));
			assertEquals(LocalDateTime.of(2019, 12, 3, 15, 43, 32, 123456789), results.get(1).getField(0));
			// test write timestamp to hive
			tableEnv.sqlUpdate("insert into db1.dest select max(ts) from db1.src");
			tableEnv.execute("write timestamp to hive");
			verifyHiveQueryResult("select * from db1.dest", Collections.singletonList("2019-12-03 15:43:32.123456789"));
		} finally {
			hiveShell.execute("drop database db1 cascade");
		}
	}

	private TableEnvironment getTableEnvWithHiveCatalog() {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnv();
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		return tableEnv;
	}

	private void verifyHiveQueryResult(String query, List<String> expected) {
		List<String> results = hiveShell.executeQuery(query);
		assertEquals(expected.size(), results.size());
		assertEquals(new HashSet<>(expected), new HashSet<>(results));
	}
}
