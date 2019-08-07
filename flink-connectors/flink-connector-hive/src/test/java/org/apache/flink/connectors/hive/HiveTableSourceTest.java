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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.planner.runtime.utils.TableUtil;
import org.apache.flink.types.Row;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import scala.collection.JavaConverters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link HiveTableSource}.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveTableSourceTest {

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	private static HiveCatalog hiveCatalog;
	private static HiveConf hiveConf;

	@BeforeClass
	public static void createCatalog() throws IOException {
		hiveConf = hiveShell.getHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
	}

	@AfterClass
	public static void closeCatalog() {
		if (null != hiveCatalog) {
			hiveCatalog.close();
		}
	}

	@Before
	public void setupSourceDatabaseAndData() {
		hiveShell.execute("CREATE DATABASE IF NOT EXISTS source_db");
	}

	@Test
	public void testReadNonPartitionedTable() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test";
		hiveShell.execute("CREATE TABLE source_db.test ( a INT, b INT, c STRING, d BIGINT, e DOUBLE)");
		hiveShell.insertInto(dbName, tblName)
				.withAllColumns()
				.addRow(1, 1, "a", 1000L, 1.11)
				.addRow(2, 2, "b", 2000L, 2.22)
				.addRow(3, 3, "c", 3000L, 3.33)
				.addRow(4, 4, "d", 4000L, 4.44)
				.commit();

		TableEnvironment tEnv = HiveTestUtils.createTableEnv();
		ObjectPath tablePath = new ObjectPath(dbName, tblName);
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tablePath);
		HiveTableSource hiveTableSource = new HiveTableSource(new JobConf(hiveConf), tablePath, catalogTable);
		Table src = tEnv.fromTableSource(hiveTableSource);
		List<Row> rows = JavaConverters.seqAsJavaListConverter(TableUtil.collect((TableImpl) src)).asJava();

		Assert.assertEquals(4, rows.size());
		Assert.assertEquals(1, rows.get(0).getField(0));
		Assert.assertEquals(2, rows.get(1).getField(0));
		Assert.assertEquals(3, rows.get(2).getField(0));
		Assert.assertEquals(4, rows.get(3).getField(0));
	}

	/**
	 * Test to read from partition table.
	 * @throws Exception
	 */
	@Test
	public void testReadPartitionTable() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test_table_pt";
		hiveShell.execute("CREATE TABLE source_db.test_table_pt " +
						"(year STRING, value INT) partitioned by (pt int);");
		hiveShell.insertInto("source_db", "test_table_pt")
				.withColumns("year", "value", "pt")
				.addRow("2014", 3, 0)
				.addRow("2014", 4, 0)
				.addRow("2015", 2, 1)
				.addRow("2015", 5, 1)
				.commit();
		TableEnvironment tEnv = HiveTestUtils.createTableEnv();
		ObjectPath tablePath = new ObjectPath(dbName, tblName);
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tablePath);
		HiveTableSource hiveTableSource = new HiveTableSource(new JobConf(hiveConf), tablePath, catalogTable);
		Table src = tEnv.fromTableSource(hiveTableSource);
		List<Row> rows = JavaConverters.seqAsJavaListConverter(TableUtil.collect((TableImpl) src)).asJava();
		assertEquals(4, rows.size());
		Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
		assertArrayEquals(new String[]{"2014,3,0", "2014,4,0", "2015,2,1", "2015,5,1"}, rowStrings);
	}

	@Test
	public void testPartitionPrunning() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test_table_pt_1";
		hiveShell.execute("CREATE TABLE source_db.test_table_pt_1 " +
						"(year STRING, value INT) partitioned by (pt int);");
		hiveShell.insertInto("source_db", "test_table_pt_1")
				.withColumns("year", "value", "pt")
				.addRow("2014", 3, 0)
				.addRow("2014", 4, 0)
				.addRow("2015", 2, 1)
				.addRow("2015", 5, 1)
				.commit();
		TableEnvironment tEnv = HiveTestUtils.createTableEnv();
		ObjectPath tablePath = new ObjectPath(dbName, tblName);
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tablePath);
		tEnv.registerTableSource("src", new HiveTableSource(new JobConf(hiveConf), tablePath, catalogTable));
		Table table = tEnv.sqlQuery("select * from src where pt = 0");
		String[] explain = tEnv.explain(table).split("==.*==\n");
		assertEquals(4, explain.length);
		String abstractSyntaxTree = explain[1];
		String optimizedLogicalPlan = explain[2];
		String physicalExecutionPlan = explain[3];
		assertTrue(abstractSyntaxTree.contains("HiveTableSource(year, value, pt) TablePath: source_db.test_table_pt_1, PartitionPruned: false, PartitionNums: 2]"));
		assertTrue(optimizedLogicalPlan.contains("HiveTableSource(year, value, pt) TablePath: source_db.test_table_pt_1, PartitionPruned: true, PartitionNums: 1]"));
		assertTrue(physicalExecutionPlan.contains("HiveTableSource(year, value, pt) TablePath: source_db.test_table_pt_1, PartitionPruned: true, PartitionNums: 1]"));
		List<Row> rows = JavaConverters.seqAsJavaListConverter(TableUtil.collect((TableImpl) table)).asJava();
		assertEquals(2, rows.size());
		Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
		assertArrayEquals(new String[]{"2014,3,0", "2014,4,0"}, rowStrings);
	}

}
