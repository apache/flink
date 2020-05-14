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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.table.api.EnvironmentSettings;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract IT case class for HBase.
 */
@RunWith(Parameterized.class)
public abstract class HBaseTestBase extends HBaseTestingClusterAutoStarter {

	protected static final String TEST_TABLE_1 = "testTable1";
	protected static final String TEST_TABLE_2 = "testTable2";
	protected static final String TEST_TABLE_3 = "testTable3";

	protected static final String ROWKEY = "rk";
	protected static final String FAMILY1 = "family1";
	protected static final String F1COL1 = "col1";

	protected static final String FAMILY2 = "family2";
	protected static final String F2COL1 = "col1";
	protected static final String F2COL2 = "col2";

	protected static final String FAMILY3 = "family3";
	protected static final String F3COL1 = "col1";
	protected static final String F3COL2 = "col2";
	protected static final String F3COL3 = "col3";

	protected static final String FAMILY4 = "family4";

	private static final byte[][] FAMILIES = new byte[][]{
		Bytes.toBytes(FAMILY1),
		Bytes.toBytes(FAMILY2),
		Bytes.toBytes(FAMILY3)
	};

	private static final byte[][] SPLIT_KEYS = new byte[][]{Bytes.toBytes(4)};

	@Parameterized.Parameter
	public PlannerType planner;
	protected EnvironmentSettings streamSettings;
	protected EnvironmentSettings batchSettings;

	@Parameterized.Parameters(name = "planner = {0}")
	public static PlannerType[] getPlanner() {
		return new PlannerType[]{PlannerType.BLINK_PLANNER, PlannerType.OLD_PLANNER};
	}

	@BeforeClass
	public static void activateHBaseCluster() throws IOException {
		registerHBaseMiniClusterInClasspath();
		prepareTables();
	}

	@Before
	public void before() {
		EnvironmentSettings.Builder streamBuilder = EnvironmentSettings.newInstance().inStreamingMode();
		EnvironmentSettings.Builder batchBuilder = EnvironmentSettings.newInstance().inBatchMode();
		if (PlannerType.BLINK_PLANNER.equals(planner)) {
			this.streamSettings = streamBuilder.useBlinkPlanner().build();
			this.batchSettings = batchBuilder.useBlinkPlanner().build();
		} else if (PlannerType.OLD_PLANNER.equals(planner)) {
			this.streamSettings = streamBuilder.useOldPlanner().build();
			this.batchSettings = batchBuilder.useOldPlanner().build();
		} else {
			throw new IllegalArgumentException("Unsupported planner name " + planner);
		}
	}

	private static void prepareTables() throws IOException {
		createHBaseTable1();
		createHBaseTable2();
		createHBaseTable3();
	}

	private static void createHBaseTable1() throws IOException {
		// create a table
		TableName tableName = TableName.valueOf(TEST_TABLE_1);
		createTable(tableName, FAMILIES, SPLIT_KEYS);

		// get the HTable instance
		HTable table = openTable(tableName);
		List<Put> puts = new ArrayList<>();
		// add some data
		puts.add(putRow(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1"));
		puts.add(putRow(2, 20, "Hello-2", 200L, 2.02, true, "Welt-2"));
		puts.add(putRow(3, 30, "Hello-3", 300L, 3.03, false, "Welt-3"));
		puts.add(putRow(4, 40, null, 400L, 4.04, true, "Welt-4"));
		puts.add(putRow(5, 50, "Hello-5", 500L, 5.05, false, "Welt-5"));
		puts.add(putRow(6, 60, "Hello-6", 600L, 6.06, true, "Welt-6"));
		puts.add(putRow(7, 70, "Hello-7", 700L, 7.07, false, "Welt-7"));
		puts.add(putRow(8, 80, null, 800L, 8.08, true, "Welt-8"));

		// append rows to table
		table.put(puts);
		table.close();
	}

	private static void createHBaseTable2() {
		// create a table
		TableName tableName = TableName.valueOf(TEST_TABLE_2);
		createTable(tableName, FAMILIES, SPLIT_KEYS);
	}

	private static void createHBaseTable3() {
		// create a table
		byte[][] families = new byte[][]{
			Bytes.toBytes(FAMILY1),
			Bytes.toBytes(FAMILY2),
			Bytes.toBytes(FAMILY3),
			Bytes.toBytes(FAMILY4),
		};
		TableName tableName = TableName.valueOf(TEST_TABLE_3);
		createTable(tableName, families, SPLIT_KEYS);
	}

	private static Put putRow(int rowKey, int f1c1, String f2c1, long f2c2, double f3c1, boolean f3c2, String f3c3) {
		Put put = new Put(Bytes.toBytes(rowKey));
		// family 1
		put.addColumn(Bytes.toBytes(FAMILY1), Bytes.toBytes(F1COL1), Bytes.toBytes(f1c1));
		// family 2
		if (f2c1 != null) {
			put.addColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(F2COL1), Bytes.toBytes(f2c1));
		}
		put.addColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(F2COL2), Bytes.toBytes(f2c2));
		// family 3
		put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL1), Bytes.toBytes(f3c1));
		put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL2), Bytes.toBytes(f3c2));
		put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL3), Bytes.toBytes(f3c3));

		return put;
	}
}
