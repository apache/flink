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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link HiveTableSink}.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveTableSinkTest {

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
		if (hiveCatalog != null) {
			hiveCatalog.close();
		}
	}

	@Test
	public void testInsertIntoNonPartitionTable() throws Exception {
		String dbName = "default";
		String tblName = "dest";
		RowTypeInfo rowTypeInfo = createDestTable(dbName, tblName, 0);
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnv);
		List<Row> toWrite = generateRecords(5);
		tableEnv.registerDataSet("src", execEnv.fromCollection(toWrite, rowTypeInfo));

		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlQuery("select * from src").insertInto("hive", "default", "dest");
		execEnv.execute();

		verifyWrittenData(toWrite, hiveShell.executeQuery("select * from " + tblName));

		hiveCatalog.dropTable(tablePath, false);
	}

	@Test
	public void testInsertIntoDynamicPartition() throws Exception {
		String dbName = "default";
		String tblName = "dest";
		RowTypeInfo rowTypeInfo = createDestTable(dbName, tblName, 1);
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnv);
		List<Row> toWrite = generateRecords(5);
		tableEnv.registerDataSet("src", execEnv.fromCollection(toWrite, rowTypeInfo));

		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlQuery("select * from src").insertInto("hive", "default", "dest");
		execEnv.execute();

		List<CatalogPartitionSpec> partitionSpecs = hiveCatalog.listPartitions(tablePath);
		assertEquals(toWrite.size(), partitionSpecs.size());

		verifyWrittenData(toWrite, hiveShell.executeQuery("select * from " + tblName));

		hiveCatalog.dropTable(tablePath, false);
	}

	@Test
	public void testWriteComplexType() throws Exception {
		String dbName = "default";
		String tblName = "dest";
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		TableSchema.Builder builder = new TableSchema.Builder();
		builder.fields(new String[]{"a", "m", "s"}, new DataType[]{
				DataTypes.ARRAY(DataTypes.INT()),
				DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
				DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.INT()), DataTypes.FIELD("f2", DataTypes.STRING()))});

		RowTypeInfo rowTypeInfo = createDestTable(dbName, tblName, builder.build(), 0);
		List<Row> toWrite = new ArrayList<>();
		Row row = new Row(rowTypeInfo.getArity());
		Object[] array = new Object[]{1, 2, 3};
		Map<Integer, String> map = new HashMap<Integer, String>() {{
			put(1, "a");
			put(2, "b");
		}};
		Row struct = new Row(2);
		struct.setField(0, 3);
		struct.setField(1, "c");

		row.setField(0, array);
		row.setField(1, map);
		row.setField(2, struct);
		toWrite.add(row);

		ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnv);
		tableEnv.registerDataSet("complexSrc", execEnv.fromCollection(toWrite, rowTypeInfo));

		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlQuery("select * from complexSrc").insertInto("hive", "default", "dest");
		execEnv.execute();

		List<String> result = hiveShell.executeQuery("select * from " + tblName);
		assertEquals(1, result.size());
		assertEquals("[1,2,3]\t{1:\"a\",2:\"b\"}\t{\"f1\":3,\"f2\":\"c\"}", result.get(0));
		hiveCatalog.dropTable(tablePath, false);

		// nested complex types
		builder = new TableSchema.Builder();
		// array of rows
		builder.fields(new String[]{"a"}, new DataType[]{DataTypes.ARRAY(
				DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.INT()), DataTypes.FIELD("f2", DataTypes.STRING())))});
		rowTypeInfo = createDestTable(dbName, tblName, builder.build(), 0);
		row = new Row(rowTypeInfo.getArity());
		array = new Object[3];
		row.setField(0, array);
		for (int i = 0; i < array.length; i++) {
			struct = new Row(2);
			struct.setField(0, 1 + i);
			struct.setField(1, String.valueOf((char) ('a' + i)));
			array[i] = struct;
		}
		toWrite.clear();
		toWrite.add(row);

		tableEnv.registerDataSet("nestedSrc", execEnv.fromCollection(toWrite, rowTypeInfo));
		tableEnv.sqlQuery("select * from nestedSrc").insertInto("hive", "default", "dest");
		execEnv.execute();

		result = hiveShell.executeQuery("select * from " + tblName);
		assertEquals(1, result.size());
		assertEquals("[{\"f1\":1,\"f2\":\"a\"},{\"f1\":2,\"f2\":\"b\"},{\"f1\":3,\"f2\":\"c\"}]", result.get(0));
		hiveCatalog.dropTable(tablePath, false);
	}

	@Test
	public void testInsertIntoStaticPartition() throws Exception {
		String dbName = "default";
		String tblName = "dest";
		RowTypeInfo rowTypeInfo = createDestTable(dbName, tblName, 1);
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnv);
		List<Row> toWrite = generateRecords(1);
		tableEnv.registerDataSet("src", execEnv.fromCollection(toWrite, rowTypeInfo));

		Map<String, String> partSpec = new HashMap<>();
		partSpec.put("s", "a");

		CatalogTable table = (CatalogTable) hiveCatalog.getTable(tablePath);
		HiveTableSink hiveTableSink = new HiveTableSink(new JobConf(hiveConf), tablePath, table);
		hiveTableSink.setStaticPartition(partSpec);
		tableEnv.registerTableSink("destSink", hiveTableSink);
		tableEnv.sqlQuery("select * from src").insertInto("destSink");
		execEnv.execute();

		// make sure new partition is created
		assertEquals(toWrite.size(), hiveCatalog.listPartitions(tablePath).size());

		verifyWrittenData(toWrite, hiveShell.executeQuery("select * from " + tblName));

		hiveCatalog.dropTable(tablePath, false);
	}

	@Test
	public void testInsertOverwrite() throws Exception {
		String dbName = "default";
		String tblName = "dest";
		RowTypeInfo rowTypeInfo = createDestTable(dbName, tblName, 0);
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnv);

		// write some data and verify
		List<Row> toWrite = generateRecords(5);
		tableEnv.registerDataSet("src", execEnv.fromCollection(toWrite, rowTypeInfo));

		CatalogTable table = (CatalogTable) hiveCatalog.getTable(tablePath);
		tableEnv.registerTableSink("destSink", new HiveTableSink(new JobConf(hiveConf), tablePath, table));
		tableEnv.sqlQuery("select * from src").insertInto("destSink");
		execEnv.execute();

		verifyWrittenData(toWrite, hiveShell.executeQuery("select * from " + tblName));

		// write some data to overwrite existing data and verify
		toWrite = generateRecords(3);
		tableEnv.registerDataSet("src1", execEnv.fromCollection(toWrite, rowTypeInfo));

		HiveTableSink sink = new HiveTableSink(new JobConf(hiveConf), tablePath, table);
		sink.setOverwrite(true);
		tableEnv.registerTableSink("destSink1", sink);
		tableEnv.sqlQuery("select * from src1").insertInto("destSink1");
		execEnv.execute();

		verifyWrittenData(toWrite, hiveShell.executeQuery("select * from " + tblName));

		hiveCatalog.dropTable(tablePath, false);
	}

	private RowTypeInfo createDestTable(String dbName, String tblName, TableSchema tableSchema, int numPartCols) throws Exception {
		CatalogTable catalogTable = createCatalogTable(tableSchema, numPartCols);
		hiveCatalog.createTable(new ObjectPath(dbName, tblName), catalogTable, false);
		return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
	}

	private RowTypeInfo createDestTable(String dbName, String tblName, int numPartCols) throws Exception {
		TableSchema.Builder builder = new TableSchema.Builder();
		builder.fields(new String[]{"i", "l", "d", "s"},
				new DataType[]{
						DataTypes.INT(),
						DataTypes.BIGINT(),
						DataTypes.DOUBLE(),
						DataTypes.STRING()});
		return createDestTable(dbName, tblName, builder.build(), numPartCols);
	}

	private CatalogTable createCatalogTable(TableSchema tableSchema, int numPartCols) {
		if (numPartCols == 0) {
			return new CatalogTableImpl(tableSchema, new HashMap<>(), "");
		}
		String[] partCols = new String[numPartCols];
		System.arraycopy(tableSchema.getFieldNames(), tableSchema.getFieldNames().length - numPartCols, partCols, 0, numPartCols);
		return new CatalogTableImpl(tableSchema, Arrays.asList(partCols), new HashMap<>(), "");
	}

	private void verifyWrittenData(List<Row> expected, List<String> results) throws Exception {
		assertEquals(expected.size(), results.size());
		for (int i = 0; i < results.size(); i++) {
			assertEquals(expected.get(i).toString().replaceAll(",", "\t"), results.get(i));
		}
	}

	private List<Row> generateRecords(int numRecords) {
		int arity = 4;
		List<Row> res = new ArrayList<>(numRecords);
		for (int i = 0; i < numRecords; i++) {
			Row row = new Row(arity);
			row.setField(0, i);
			row.setField(1, (long) i);
			row.setField(2, Double.valueOf(String.valueOf(String.format("%d.%d", i, i))));
			row.setField(3, String.valueOf((char) ('a' + i)));
			res.add(row);
		}
		return res;
	}
}
