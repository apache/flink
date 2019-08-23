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

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
		hiveShell.insertInto("db1", "src").addRow(1, 1).addRow(2, null).commit();

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

		// TODO: test reading from flink when https://issues.apache.org/jira/browse/FLINK-13279 is fixed
		assertEquals(Arrays.asList("1\t1", "2\tNULL"), hiveShell.executeQuery("select * from db1.part"));

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
		hiveShell.execute("create table db1.src (i int,s string) " + suffix);
		hiveShell.execute("create table db1.dest (i int,s string) " + suffix);

		// prepare source data with Hive
		hiveShell.execute("insert into db1.src values (1,'a'),(2,'b')");

		// populate dest table with source table
		tableEnv.sqlUpdate("insert into db1.dest select * from db1.src");
		tableEnv.execute("test_" + format);

		// verify data on hive side
		verifyHiveQueryResult("select * from db1.dest", Arrays.asList("1\ta", "2\tb"));

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
			hiveShell.execute("insert into db1.src1 values (1.0),(2.12),(5.123),(5.456),(123456789.12)");

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
