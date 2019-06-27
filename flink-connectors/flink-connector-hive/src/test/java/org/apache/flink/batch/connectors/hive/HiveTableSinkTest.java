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
import org.apache.flink.table.catalog.hive.HiveCatalogPartition;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link HiveTableSink}.
 */
public class HiveTableSinkTest {

	private static HiveCatalog hiveCatalog;
	private static HiveConf hiveConf;

	@BeforeClass
	public static void createCatalog() throws IOException {
		hiveConf = HiveTestUtils.createHiveConf();
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

		Table hiveTable = hiveCatalog.getHiveTable(tablePath);
		CatalogTable table = (CatalogTable) hiveCatalog.getTable(tablePath);
		tableEnv.registerTableSink("destSink", new HiveTableSink(new JobConf(hiveConf), tablePath, table));
		tableEnv.sqlQuery("select * from src").insertInto("destSink");
		execEnv.execute();

		verifyWrittenData(new Path(hiveTable.getSd().getLocation(), "0"), toWrite, 0);
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

		Table hiveTable = hiveCatalog.getHiveTable(tablePath);
		CatalogTable table = (CatalogTable) hiveCatalog.getTable(tablePath);
		tableEnv.registerTableSink("destSink", new HiveTableSink(new JobConf(hiveConf), tablePath, table));
		tableEnv.sqlQuery("select * from src").insertInto("destSink");
		execEnv.execute();

		List<CatalogPartitionSpec> partitionSpecs = hiveCatalog.listPartitions(tablePath);
		assertEquals(toWrite.size(), partitionSpecs.size());
		for (int i = 0; i < toWrite.size(); i++) {
			HiveCatalogPartition partition = (HiveCatalogPartition) hiveCatalog.getPartition(tablePath, partitionSpecs.get(i));
			verifyWrittenData(new Path(partition.getLocation(), "0"), Collections.singletonList(toWrite.get(i)), 1);
		}

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

		Table hiveTable = hiveCatalog.getHiveTable(tablePath);
		CatalogTable catalogTable = (CatalogTable) hiveCatalog.getTable(tablePath);
		tableEnv.registerTableSink("complexSink", new HiveTableSink(new JobConf(hiveConf), tablePath, catalogTable));
		tableEnv.sqlQuery("select * from complexSrc").insertInto("complexSink");
		execEnv.execute();

		verifyWrittenData(new Path(hiveTable.getSd().getLocation(), "0"), Collections.singletonList("1 2 3,1 a 2 b,3 c"));
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
		hiveTable = hiveCatalog.getHiveTable(tablePath);
		catalogTable = (CatalogTable) hiveCatalog.getTable(tablePath);
		tableEnv.registerTableSink("nestedSink", new HiveTableSink(new JobConf(hiveConf), tablePath, catalogTable));
		tableEnv.sqlQuery("select * from nestedSrc").insertInto("nestedSink");
		execEnv.execute();

		verifyWrittenData(new Path(hiveTable.getSd().getLocation(), "0"), Collections.singletonList("1 a 2 b 3 c"));
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

	private void verifyWrittenData(Path outputFile, List<Row> expectedRows, int numPartCols) throws Exception {
		int[] fields = IntStream.range(0, expectedRows.get(0).getArity() - numPartCols).toArray();
		List<String> expected = new ArrayList<>(expectedRows.size());
		for (Row row : expectedRows) {
			expected.add(Row.project(row, fields).toString());
		}
		verifyWrittenData(outputFile, expected);
	}

	private void verifyWrittenData(Path outputFile, List<String> expected) throws Exception {
		FileSystem fs = outputFile.getFileSystem(hiveConf);
		assertTrue(fs.exists(outputFile));
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
			int numWritten = 0;
			String line = reader.readLine();
			while (line != null) {
				assertEquals(expected.get(numWritten++), line.replaceAll("\u0001", ",").replaceAll("\u0002", " ").replaceAll("\u0003", " "));
				line = reader.readLine();
			}
			assertEquals(expected.size(), numWritten);
		}
	}
}
