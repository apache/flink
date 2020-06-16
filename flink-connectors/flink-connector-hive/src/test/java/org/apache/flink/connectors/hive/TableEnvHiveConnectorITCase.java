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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.ArrayUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

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

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test hive connector with table API.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class TableEnvHiveConnectorITCase {

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
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		tableEnv.executeSql("create table db1.src (x int, y int)");
		tableEnv.executeSql("create table db1.part (x int) partitioned by (y int)");
		HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src").addRow(new Object[]{1, 1}).addRow(new Object[]{2, null}).commit();

		// test generating partitions with default name
		TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.part select * from db1.src");
		HiveConf hiveConf = hiveShell.getHiveConf();
		String defaultPartName = hiveConf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
		Table hiveTable = hmsClient.getTable("db1", "part");
		Path defaultPartPath = new Path(hiveTable.getSd().getLocation(), "y=" + defaultPartName);
		FileSystem fs = defaultPartPath.getFileSystem(hiveConf);
		assertTrue(fs.exists(defaultPartPath));

		TableImpl flinkTable = (TableImpl) tableEnv.sqlQuery("select y, x from db1.part order by x");
		List<Row> rows = Lists.newArrayList(flinkTable.execute().collect());
		assertEquals(Arrays.toString(new String[]{"1,1", "null,2"}), rows.toString());

		tableEnv.executeSql("drop database db1 cascade");
	}

	@Test
	public void testGetNonExistingFunction() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		tableEnv.executeSql("create table db1.src (d double, s string)");
		tableEnv.executeSql("create table db1.dest (x bigint)");

		// just make sure the query runs through, no need to verify result
		TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select count(d) from db1.src");

		tableEnv.executeSql("drop database db1 cascade");
	}

	@Test
	public void testDifferentFormats() throws Exception {
		String[] formats = new String[]{"orc", "parquet", "sequencefile", "csv", "avro"};
		for (String format : formats) {
			if (format.equals("orc") && HiveShimLoader.getHiveVersion().startsWith("2.0")) {
				// Ignore orc test for Hive version 2.0.x for now due to FLINK-13998
				continue;
			} else if (format.equals("avro") && !HiveVersionTestUtil.HIVE_110_OR_LATER) {
				// timestamp is not supported for avro tables before 1.1.0
				continue;
			}
			readWriteFormat(format);
		}
	}

	private void readWriteFormat(String format) throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();

		tableEnv.executeSql("create database db1");

		// create source and dest tables
		String suffix;
		if (format.equals("csv")) {
			suffix = "row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'";
		} else {
			suffix = "stored as " + format;
		}
		String tableSchema;
		// use 2018-08-20 00:00:00.1 to avoid multi-version print difference.
		List<Object> row1 = new ArrayList<>(Arrays.asList(1, "a", "2018-08-20 00:00:00.1"));
		List<Object> row2 = new ArrayList<>(Arrays.asList(2, "b", "2019-08-26 00:00:00.1"));
		// some data types are not supported for parquet tables in early versions -- https://issues.apache.org/jira/browse/HIVE-6384
		if (HiveVersionTestUtil.HIVE_120_OR_LATER || !format.equals("parquet")) {
			tableSchema = "(i int,s string,ts timestamp,dt date)";
			row1.add("2018-08-20");
			row2.add("2019-08-26");
		} else {
			tableSchema = "(i int,s string,ts timestamp)";
		}

		tableEnv.executeSql(String.format(
				"create table db1.src %s partitioned by (p1 string, p2 timestamp) %s", tableSchema, suffix));
		tableEnv.executeSql(String.format(
				"create table db1.dest %s partitioned by (p1 string, p2 timestamp) %s", tableSchema, suffix));

		// prepare source data with Hive
		// TABLE keyword in INSERT INTO is mandatory prior to 1.1.0
		hiveShell.execute(String.format(
				"insert into table db1.src partition(p1='first',p2='2018-08-20 00:00:00.1') values (%s)",
				toRowValue(row1)));
		hiveShell.execute(String.format(
				"insert into table db1.src partition(p1='second',p2='2018-08-26 00:00:00.1') values (%s)",
				toRowValue(row2)));

		List<String> expected = Arrays.asList(
				String.join("\t", ArrayUtils.concat(
						row1.stream().map(Object::toString).toArray(String[]::new),
						new String[]{"first", "2018-08-20 00:00:00.1"})),
				String.join("\t", ArrayUtils.concat(
						row2.stream().map(Object::toString).toArray(String[]::new),
						new String[]{"second", "2018-08-26 00:00:00.1"})));

		verifyFlinkQueryResult(tableEnv.sqlQuery("select * from db1.src"), expected);

		// populate dest table with source table
		TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select * from db1.src");

		// verify data on hive side
		verifyHiveQueryResult("select * from db1.dest", expected);

		tableEnv.executeSql("drop database db1 cascade");
	}

	private String toRowValue(List<Object> row) {
		return row.stream().map(o -> {
			String res = o.toString();
			if (o instanceof String) {
				res = "'" + res + "'";
			}
			return res;
		}).collect(Collectors.joining(","));
	}

	@Test
	public void testDecimal() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src1 (x decimal(10,2))");
			tableEnv.executeSql("create table db1.src2 (x decimal(10,2))");
			tableEnv.executeSql("create table db1.dest (x decimal(10,2))");
			// populate src1 from Hive
			// TABLE keyword in INSERT INTO is mandatory prior to 1.1.0
			hiveShell.execute("insert into table db1.src1 values (1.0),(2.12),(5.123),(5.456),(123456789.12)");

			// populate src2 with same data from Flink
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.src2 values (cast(1.0 as decimal(10,2))), (cast(2.12 as decimal(10,2))), " +
					"(cast(5.123 as decimal(10,2))), (cast(5.456 as decimal(10,2))), (cast(123456789.12 as decimal(10,2)))");
			// verify src1 and src2 contain same data
			verifyHiveQueryResult("select * from db1.src2", hiveShell.executeQuery("select * from db1.src1"));

			// populate dest with src1 from Flink -- to test reading decimal type from Hive
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select * from db1.src1");
			verifyHiveQueryResult("select * from db1.dest", hiveShell.executeQuery("select * from db1.src1"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testInsertOverwrite() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			// non-partitioned
			tableEnv.executeSql("create table db1.dest (x int, y string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "dest").addRow(new Object[]{1, "a"}).addRow(new Object[]{2, "b"}).commit();
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta", "2\tb"));

			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert overwrite db1.dest values (3, 'c')");
			verifyHiveQueryResult("select * from db1.dest", Collections.singletonList("3\tc"));

			// static partition
			tableEnv.executeSql("create table db1.part(x int) partitioned by (y int)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part").addRow(new Object[]{1}).commit("y=1");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part").addRow(new Object[]{2}).commit("y=2");
			tableEnv = getTableEnvWithHiveCatalog();
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert overwrite db1.part partition (y=1) select 100");
			verifyHiveQueryResult("select * from db1.part", Arrays.asList("100\t1", "2\t2"));

			// dynamic partition
			tableEnv = getTableEnvWithHiveCatalog();
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert overwrite db1.part values (200,2),(3,3)");
			// only overwrite dynamically matched partitions, other existing partitions remain intact
			verifyHiveQueryResult("select * from db1.part", Arrays.asList("100\t1", "200\t2", "3\t3"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testStaticPartition() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (x int)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src").addRow(new Object[]{1}).addRow(new Object[]{2}).commit();
			tableEnv.executeSql("create table db1.dest (x int) partitioned by (p1 string, p2 double)");
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest partition (p1='1''1', p2=1.1) select x from db1.src");
			assertEquals(1, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\t1'1\t1.1", "2\t1'1\t1.1"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testDynamicPartition() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (x int, y string, z double)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{1, "a", 1.1})
					.addRow(new Object[]{2, "a", 2.2})
					.addRow(new Object[]{3, "b", 3.3})
					.commit();
			tableEnv.executeSql("create table db1.dest (x int) partitioned by (p1 string, p2 double)");
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select * from db1.src");
			assertEquals(3, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta\t1.1", "2\ta\t2.2", "3\tb\t3.3"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testPartialDynamicPartition() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (x int, y string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src").addRow(new Object[]{1, "a"}).addRow(new Object[]{2, "b"}).commit();
			tableEnv.executeSql("create table db1.dest (x int) partitioned by (p1 double, p2 string)");
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest partition (p1=1.1) select x,y from db1.src");
			assertEquals(2, hiveCatalog.listPartitions(new ObjectPath("db1", "dest")).size());
			verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\t1.1\ta", "2\t1.1\tb"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testDateTimestampPartitionColumns() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.part(x int) partitioned by (dt date,ts timestamp)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{1})
					.addRow(new Object[]{2})
					.commit("dt='2019-12-23',ts='2019-12-23 00:00:00'");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{3})
					.commit("dt='2019-12-25',ts='2019-12-25 16:23:43.012'");
			List<Row> results = Lists.newArrayList(tableEnv.sqlQuery("select * from db1.part order by x").execute().collect());
			assertEquals("[1,2019-12-23,2019-12-23T00:00, 2,2019-12-23,2019-12-23T00:00, 3,2019-12-25,2019-12-25T16:23:43.012]", results.toString());

			results = Lists.newArrayList(tableEnv.sqlQuery("select x from db1.part where dt=cast('2019-12-25' as date)").execute().collect());
			assertEquals("[3]", results.toString());

			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.part select 4,cast('2019-12-31' as date),cast('2019-12-31 12:00:00.0' as timestamp)");
			results = Lists.newArrayList(tableEnv.sqlQuery("select max(dt) from db1.part").execute().collect());
			assertEquals("[2019-12-31]", results.toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
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
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.simple (i int,a array<int>)");
			tableEnv.executeSql("create table db1.nested (a array<map<int, string>>)");
			tableEnv.executeSql("create function hiveudtf as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode'");
			hiveShell.insertInto("db1", "simple").addRow(3, Arrays.asList(1, 2, 3)).commit();
			Map<Integer, String> map1 = new HashMap<>();
			map1.put(1, "a");
			map1.put(2, "b");
			Map<Integer, String> map2 = new HashMap<>();
			map2.put(3, "c");
			hiveShell.insertInto("db1", "nested").addRow(Arrays.asList(map1, map2)).commit();

			List<Row> results = Lists.newArrayList(
					tableEnv.sqlQuery("select x from db1.simple, lateral table(hiveudtf(a)) as T(x)").execute().collect());
			assertEquals("[1, 2, 3]", results.toString());
			results = Lists.newArrayList(
					tableEnv.sqlQuery("select x from db1.nested, lateral table(hiveudtf(a)) as T(x)").execute().collect());
			assertEquals("[{1=a, 2=b}, {3=c}]", results.toString());

			tableEnv.executeSql("create table db1.ts (a array<timestamp>)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "ts").addRow(new Object[]{
					new Object[]{Timestamp.valueOf("2015-04-28 15:23:00"), Timestamp.valueOf("2016-06-03 17:05:52")}})
					.commit();
			results = Lists.newArrayList(
					tableEnv.sqlQuery("select x from db1.ts, lateral table(hiveudtf(a)) as T(x)").execute().collect());
			assertEquals("[2015-04-28T15:23, 2016-06-03T17:05:52]", results.toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
			tableEnv.executeSql("drop function hiveudtf");
		}
	}

	@Test
	public void testNotNullConstraints() throws Exception {
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.tbl (x int,y bigint not null enable rely,z string not null enable norely)");
			CatalogBaseTable catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl"));
			TableSchema tableSchema = catalogTable.getSchema();
			assertTrue("By default columns should be nullable",
					tableSchema.getFieldDataTypes()[0].getLogicalType().isNullable());
			assertFalse("NOT NULL columns should be reflected in table schema",
					tableSchema.getFieldDataTypes()[1].getLogicalType().isNullable());
			assertTrue("NOT NULL NORELY columns should be considered nullable",
					tableSchema.getFieldDataTypes()[2].getLogicalType().isNullable());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testPKConstraint() throws Exception {
		// While PK constraints are supported since Hive 2.1.0, the constraints cannot be RELY in 2.x versions.
		// So let's only test for 3.x.
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			// test rely PK constraints
			tableEnv.executeSql("create table db1.tbl1 (x tinyint,y smallint,z int, primary key (x,z) disable novalidate rely)");
			CatalogBaseTable catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl1"));
			TableSchema tableSchema = catalogTable.getSchema();
			assertTrue(tableSchema.getPrimaryKey().isPresent());
			UniqueConstraint pk = tableSchema.getPrimaryKey().get();
			assertEquals(2, pk.getColumns().size());
			assertTrue(pk.getColumns().containsAll(Arrays.asList("x", "z")));

			// test norely PK constraints
			tableEnv.executeSql("create table db1.tbl2 (x tinyint,y smallint, primary key (x) disable norely)");
			catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl2"));
			tableSchema = catalogTable.getSchema();
			assertFalse(tableSchema.getPrimaryKey().isPresent());

			// test table w/o PK
			tableEnv.executeSql("create table db1.tbl3 (x tinyint)");
			catalogTable = hiveCatalog.getTable(new ObjectPath("db1", "tbl3"));
			tableSchema = catalogTable.getSchema();
			assertFalse(tableSchema.getPrimaryKey().isPresent());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testTimestamp() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (ts timestamp)");
			tableEnv.executeSql("create table db1.dest (ts timestamp)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{Timestamp.valueOf("2019-11-11 00:00:00")})
					.addRow(new Object[]{Timestamp.valueOf("2019-12-03 15:43:32.123456789")})
					.commit();
			// test read timestamp from hive
			List<Row> results = Lists.newArrayList(tableEnv.sqlQuery("select * from db1.src").execute().collect());
			assertEquals(2, results.size());
			assertEquals(LocalDateTime.of(2019, 11, 11, 0, 0), results.get(0).getField(0));
			assertEquals(LocalDateTime.of(2019, 12, 3, 15, 43, 32, 123456789), results.get(1).getField(0));
			// test write timestamp to hive
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select max(ts) from db1.src");
			verifyHiveQueryResult("select * from db1.dest", Collections.singletonList("2019-12-03 15:43:32.123456789"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testDate() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (dt date)");
			tableEnv.executeSql("create table db1.dest (dt date)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{Date.valueOf("2019-12-09")})
					.addRow(new Object[]{Date.valueOf("2019-12-12")})
					.commit();
			// test read date from hive
			List<Row> results = Lists.newArrayList(tableEnv.sqlQuery("select * from db1.src").execute().collect());
			assertEquals(2, results.size());
			assertEquals(LocalDate.of(2019, 12, 9), results.get(0).getField(0));
			assertEquals(LocalDate.of(2019, 12, 12), results.get(1).getField(0));
			// test write date to hive
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select max(dt) from db1.src");
			verifyHiveQueryResult("select * from db1.dest", Collections.singletonList("2019-12-12"));
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testViews() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (key int,val string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{1, "a"})
					.addRow(new Object[]{1, "aa"})
					.addRow(new Object[]{1, "aaa"})
					.addRow(new Object[]{2, "b"})
					.addRow(new Object[]{3, "c"})
					.addRow(new Object[]{3, "ccc"})
					.commit();
			tableEnv.executeSql("create table db1.keys (key int,name string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "keys")
					.addRow(new Object[]{1, "key1"})
					.addRow(new Object[]{2, "key2"})
					.addRow(new Object[]{3, "key3"})
					.addRow(new Object[]{4, "key4"})
					.commit();
			hiveShell.execute("create view db1.v1 as select key as k,val as v from db1.src limit 2");
			hiveShell.execute("create view db1.v2 as select key,count(*) from db1.src group by key having count(*)>1 order by key");
			hiveShell.execute("create view db1.v3 as select k.key,k.name,count(*) from db1.src s join db1.keys k on s.key=k.key group by k.key,k.name order by k.key");
			List<Row> results = Lists.newArrayList(tableEnv.sqlQuery("select count(v) from db1.v1").execute().collect());
			assertEquals("[2]", results.toString());
			results = Lists.newArrayList(tableEnv.sqlQuery("select * from db1.v2").execute().collect());
			assertEquals("[1,3, 3,2]", results.toString());
			results = Lists.newArrayList(tableEnv.sqlQuery("select * from db1.v3").execute().collect());
			assertEquals("[1,key1,3, 2,key2,1, 3,key3,2]", results.toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testWhitespacePartValue() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.dest (x int) partitioned by (p string)");
			StatementSet stmtSet = tableEnv.createStatementSet();
			stmtSet.addInsertSql("insert into db1.dest select 1,'  '");
			stmtSet.addInsertSql("insert into db1.dest select 2,'a \t'");
			TableResult tableResult = stmtSet.execute();
			// wait job finished
			tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
			assertEquals("[p=  , p=a %09]", hiveShell.executeQuery("show partitions db1.dest").toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	private void testCompressTextTable(boolean batch) throws Exception {
		TableEnvironment tableEnv = batch ?
				getTableEnvWithHiveCatalog() :
				getStreamTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (x string,y string)");
			hiveShell.execute("create table db1.dest like db1.src");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{"a", "b"})
					.addRow(new Object[]{"c", "d"})
					.commit();
			hiveCatalog.getHiveConf().setBoolVar(HiveConf.ConfVars.COMPRESSRESULT, true);
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into db1.dest select * from db1.src");
			List<String> expected = Arrays.asList("a\tb", "c\td");
			verifyHiveQueryResult("select * from db1.dest", expected);
			verifyFlinkQueryResult(tableEnv.sqlQuery("select * from db1.dest"), expected);
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testBatchCompressTextTable() throws Exception {
		testCompressTextTable(true);
	}

	@Test
	public void testStreamCompressTextTable() throws Exception {
		testCompressTextTable(false);
	}

	@Test
	public void testRegexSerDe() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (x int,y string) " +
					"row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' " +
					"with serdeproperties ('input.regex'='([\\\\d]+)\\u0001([\\\\S]+)')");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{1, "a"})
					.addRow(new Object[]{2, "ab"})
					.commit();
			assertEquals("[1,a, 2,ab]", Lists.newArrayList(tableEnv.sqlQuery("select * from db1.src order by x").execute().collect()).toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testUpdatePartitionSD() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.dest (x int) partitioned by (p string) stored as rcfile");
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert overwrite db1.dest partition (p='1') select 1");
			tableEnv.executeSql("alter table db1.dest set fileformat sequencefile");
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert overwrite db1.dest partition (p='1') select 1");
			assertEquals("[1,1]", Lists.newArrayList(tableEnv.sqlQuery("select * from db1.dest").execute().collect()).toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testParquetNameMapping() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.t1 (x int,y int) stored as parquet");
			TableEnvUtil.execInsertSqlAndWaitResult(tableEnv, "insert into table db1.t1 values (1,10),(2,20)");
			Table hiveTable = hiveCatalog.getHiveTable(new ObjectPath("db1", "t1"));
			String location = hiveTable.getSd().getLocation();
			tableEnv.executeSql(String.format("create table db1.t2 (y int,x int) stored as parquet location '%s'", location));
			tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, true);
			assertEquals("[1, 2]", Lists.newArrayList(tableEnv.sqlQuery("select x from db1.t1").execute().collect()).toString());
			assertEquals("[1, 2]", Lists.newArrayList(tableEnv.sqlQuery("select x from db1.t2").execute().collect()).toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testOrcSchemaEvol() throws Exception {
		// not supported until 2.1.0 -- https://issues.apache.org/jira/browse/HIVE-11981,
		// https://issues.apache.org/jira/browse/HIVE-13178
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_210_OR_LATER);
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.src (x smallint,y int) stored as orc");
			hiveShell.execute("insert into table db1.src values (1,100),(2,200)");

			tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, true);

			tableEnv.executeSql("alter table db1.src change x x int");
			assertEquals("[1,100, 2,200]", Lists.newArrayList(tableEnv.sqlQuery("select * from db1.src").execute().collect()).toString());

			tableEnv.executeSql("alter table db1.src change y y string");
			assertEquals("[1,100, 2,200]", Lists.newArrayList(tableEnv.sqlQuery("select * from db1.src").execute().collect()).toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testNonExistingPartitionFolder() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.part (x int) partitioned by (p int)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part").addRow(new Object[]{1}).commit("p=1");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part").addRow(new Object[]{2}).commit("p=2");
			tableEnv.executeSql("alter table db1.part add partition (p=3)");
			// remove one partition
			Path toRemove = new Path(hiveCatalog.getHiveTable(new ObjectPath("db1", "part")).getSd().getLocation(), "p=2");
			FileSystem fs = toRemove.getFileSystem(hiveShell.getHiveConf());
			fs.delete(toRemove, true);

			List<Row> results = Lists.newArrayList(tableEnv.sqlQuery("select * from db1.part").execute().collect());
			assertEquals("[1,1]", results.toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testInsertPartitionWithStarSource() throws Exception {
		TableEnvironment tableEnv = getTableEnvWithHiveCatalog();
		tableEnv.executeSql("create table src (x int,y string)");
		HiveTestUtils.createTextTableInserter(
				hiveShell,
				"default",
				"src")
				.addRow(new Object[]{1, "a"})
				.commit();
		tableEnv.executeSql("create table dest (x int) partitioned by (p1 int,p2 string)");
		TableEnvUtil.execInsertSqlAndWaitResult(tableEnv,
				"insert into dest partition (p1=1) select * from src");
		List<Row> results = Lists.newArrayList(tableEnv.sqlQuery("select * from dest").execute().collect());
		assertEquals("[1,1,a]", results.toString());
		tableEnv.executeSql("drop table if exists src");
		tableEnv.executeSql("drop table if exists dest");
	}

	private TableEnvironment getTableEnvWithHiveCatalog() {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		return tableEnv;
	}

	private TableEnvironment getStreamTableEnvWithHiveCatalog() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env, SqlDialect.HIVE);
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		return tableEnv;
	}

	private void verifyHiveQueryResult(String query, List<String> expected) {
		List<String> results = hiveShell.executeQuery(query);
		assertEquals(expected.size(), results.size());
		assertEquals(new HashSet<>(expected), new HashSet<>(results));
	}

	private void verifyFlinkQueryResult(org.apache.flink.table.api.Table table, List<String> expected) throws Exception {
		List<Row> rows = Lists.newArrayList(table.execute().collect());
		List<String> results = rows.stream().map(row ->
				IntStream.range(0, row.getArity())
						.mapToObj(row::getField)
						.map(o -> o instanceof LocalDateTime ?
								Timestamp.valueOf((LocalDateTime) o) : o)
						.map(Object::toString)
						.collect(Collectors.joining("\t"))).collect(Collectors.toList());
		assertEquals(expected.size(), results.size());
		assertEquals(new HashSet<>(expected), new HashSet<>(results));
	}
}
