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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link HiveTableSink}.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveTableSinkITCase {

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
		RowTypeInfo rowTypeInfo = createHiveDestTable(dbName, tblName, 0);
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
		List<Row> toWrite = generateRecords(5);
		Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo));
		tableEnv.registerTable("src", src);

		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlQuery("select * from src").executeInsert("hive.`default`.dest").await();

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

		RowTypeInfo rowTypeInfo = createHiveDestTable(dbName, tblName, builder.build(), 0);
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

		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
		Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo));
		tableEnv.registerTable("complexSrc", src);

		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlQuery("select * from complexSrc").executeInsert("hive.`default`.dest").await();

		List<String> result = hiveShell.executeQuery("select * from " + tblName);
		assertEquals(1, result.size());
		assertEquals("[1,2,3]\t{1:\"a\",2:\"b\"}\t{\"f1\":3,\"f2\":\"c\"}", result.get(0));

		hiveCatalog.dropTable(tablePath, false);
	}

	@Test
	public void testWriteNestedComplexType() throws Exception {
		String dbName = "default";
		String tblName = "dest";
		ObjectPath tablePath = new ObjectPath(dbName, tblName);

		// nested complex types
		TableSchema.Builder builder = new TableSchema.Builder();
		// array of rows
		builder.fields(new String[]{"a"}, new DataType[]{DataTypes.ARRAY(
				DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.INT()), DataTypes.FIELD("f2", DataTypes.STRING())))});
		RowTypeInfo rowTypeInfo = createHiveDestTable(dbName, tblName, builder.build(), 0);
		Row row = new Row(rowTypeInfo.getArity());
		Object[] array = new Object[3];
		row.setField(0, array);
		for (int i = 0; i < array.length; i++) {
			Row struct = new Row(2);
			struct.setField(0, 1 + i);
			struct.setField(1, String.valueOf((char) ('a' + i)));
			array[i] = struct;
		}
		List<Row> toWrite = new ArrayList<>();
		toWrite.add(row);

		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();

		Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo));
		tableEnv.registerTable("nestedSrc", src);
		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.sqlQuery("select * from nestedSrc").executeInsert("hive.`default`.dest").await();

		List<String> result = hiveShell.executeQuery("select * from " + tblName);
		assertEquals(1, result.size());
		assertEquals("[{\"f1\":1,\"f2\":\"a\"},{\"f1\":2,\"f2\":\"b\"},{\"f1\":3,\"f2\":\"c\"}]", result.get(0));
		hiveCatalog.dropTable(tablePath, false);
	}

	@Test
	public void testWriteNullValues() throws Exception {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
		tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tableEnv.useCatalog(hiveCatalog.getName());
		tableEnv.executeSql("create database db1");
		try {
			// 17 data types
			tableEnv.executeSql("create table db1.src" +
					"(t tinyint,s smallint,i int,b bigint,f float,d double,de decimal(10,5),ts timestamp,dt date," +
					"str string,ch char(5),vch varchar(8),bl boolean,bin binary,arr array<int>,mp map<int,string>,strt struct<f1:int,f2:string>)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "src")
					.addRow(new Object[]{null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null})
					.commit();
			hiveShell.execute("create table db1.dest like db1.src");

			tableEnv.executeSql("insert into db1.dest select * from db1.src").await();
			List<String> results = hiveShell.executeQuery("select * from db1.dest");
			assertEquals(1, results.size());
			String[] cols = results.get(0).split("\t");
			assertEquals(17, cols.length);
			assertEquals("NULL", cols[0]);
			assertEquals(1, new HashSet<>(Arrays.asList(cols)).size());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testBatchAppend() throws Exception {
		TableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
		tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tEnv.useCatalog(hiveCatalog.getName());
		tEnv.executeSql("create database db1");
		tEnv.useDatabase("db1");
		try {
			tEnv.executeSql("create table append_table (i int, j int)");
			tEnv.executeSql("insert into append_table select 1, 1").await();
			tEnv.executeSql("insert into append_table select 2, 2").await();
			List<Row> rows = CollectionUtil.iteratorToList(tEnv.executeSql("select * from append_table").collect());
			rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
			Assert.assertEquals(Arrays.asList(Row.of(1, 1), Row.of(2, 2)), rows);
		} finally {
			tEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test(timeout = 120000)
	public void testDefaultSerPartStreamingWrite() throws Exception {
		testStreamingWrite(true, false, "textfile", this::checkSuccessFiles);
	}

	@Test(timeout = 120000)
	public void testPartStreamingWrite() throws Exception {
		testStreamingWrite(true, false, "parquet", this::checkSuccessFiles);
		// disable vector orc writer test for hive 2.x due to dependency conflict
		if (!hiveCatalog.getHiveVersion().startsWith("2.")) {
			testStreamingWrite(true, false, "orc", this::checkSuccessFiles);
		}
	}

	@Test(timeout = 120000)
	public void testNonPartStreamingWrite() throws Exception {
		testStreamingWrite(false, false, "parquet", (p) -> {});
		// disable vector orc writer test for hive 2.x due to dependency conflict
		if (!hiveCatalog.getHiveVersion().startsWith("2.")) {
			testStreamingWrite(false, false, "orc", (p) -> {});
		}
	}

	@Test(timeout = 120000)
	public void testPartStreamingMrWrite() throws Exception {
		testStreamingWrite(true, true, "parquet", this::checkSuccessFiles);
		// doesn't support writer 2.0 orc table
		if (!hiveCatalog.getHiveVersion().startsWith("2.0")) {
			testStreamingWrite(true, true, "orc", this::checkSuccessFiles);
		}
	}

	@Test(timeout = 120000)
	public void testNonPartStreamingMrWrite() throws Exception {
		testStreamingWrite(false, true, "parquet", (p) -> {});
		// doesn't support writer 2.0 orc table
		if (!hiveCatalog.getHiveVersion().startsWith("2.0")) {
			testStreamingWrite(false, true, "orc", (p) -> {});
		}
	}

	@Test(timeout = 120000)
	public void testStreamingAppend() throws Exception {
		testStreamingWrite(false, false, "parquet", (p) -> {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(1);
			StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env);
			tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
			tEnv.useCatalog(hiveCatalog.getName());

			try {
				tEnv.executeSql(
						"insert into db1.sink_table select 6,'a','b','2020-05-03','12'")
						.await();
			} catch (Exception e) {
				Assert.fail("Failed to execute sql: " + e.getMessage());
			}

			assertBatch("db1.sink_table", Arrays.asList(
					"1,a,b,2020-05-03,7",
					"1,a,b,2020-05-03,7",
					"2,p,q,2020-05-03,8",
					"2,p,q,2020-05-03,8",
					"3,x,y,2020-05-03,9",
					"3,x,y,2020-05-03,9",
					"4,x,y,2020-05-03,10",
					"4,x,y,2020-05-03,10",
					"5,x,y,2020-05-03,11",
					"5,x,y,2020-05-03,11",
					"6,a,b,2020-05-03,12"));
		});
	}

	private void checkSuccessFiles(String path) {
		File basePath = new File(path, "d=2020-05-03");
		Assert.assertEquals(5, basePath.list().length);
		Assert.assertTrue(new File(new File(basePath, "e=7"), "_MY_SUCCESS").exists());
		Assert.assertTrue(new File(new File(basePath, "e=8"), "_MY_SUCCESS").exists());
		Assert.assertTrue(new File(new File(basePath, "e=9"), "_MY_SUCCESS").exists());
		Assert.assertTrue(new File(new File(basePath, "e=10"), "_MY_SUCCESS").exists());
		Assert.assertTrue(new File(new File(basePath, "e=11"), "_MY_SUCCESS").exists());
	}

	private void testStreamingWrite(
			boolean part,
			boolean useMr,
			String format,
			Consumer<String> pathConsumer) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(100);

		StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env);
		tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tEnv.useCatalog(hiveCatalog.getName());
		tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		if (useMr) {
			tEnv.getConfig().getConfiguration().set(
					HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, true);
		} else {
			tEnv.getConfig().getConfiguration().set(
					HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, false);
		}

		try {
			tEnv.executeSql("create database db1");
			tEnv.useDatabase("db1");

			// prepare source
			List<Row> data = Arrays.asList(
					Row.of(1, "a", "b", "2020-05-03", "7"),
					Row.of(2, "p", "q", "2020-05-03", "8"),
					Row.of(3, "x", "y", "2020-05-03", "9"),
					Row.of(4, "x", "y", "2020-05-03", "10"),
					Row.of(5, "x", "y", "2020-05-03", "11"));
			DataStream<Row> stream = env.addSource(
					new FiniteTestSource<>(data),
					new RowTypeInfo(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING));
			tEnv.createTemporaryView("my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"));

			// DDL
			tEnv.executeSql("create external table sink_table (a int,b string,c string" +
					(part ? "" : ",d string,e string") +
					") " +
					(part ? "partitioned by (d string,e string) " : "") +
					" stored as " + format +
					" TBLPROPERTIES (" +
					"'" + PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key() + "'='$d $e:00:00'," +
					"'" + SINK_PARTITION_COMMIT_DELAY.key() + "'='1h'," +
					"'" + SINK_PARTITION_COMMIT_POLICY_KIND.key() + "'='metastore,success-file'," +
					"'" + SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key() + "'='_MY_SUCCESS'" +
					")");

			tEnv.sqlQuery("select * from my_table").executeInsert("sink_table").await();

			assertBatch("db1.sink_table", Arrays.asList(
					"1,a,b,2020-05-03,7",
					"1,a,b,2020-05-03,7",
					"2,p,q,2020-05-03,8",
					"2,p,q,2020-05-03,8",
					"3,x,y,2020-05-03,9",
					"3,x,y,2020-05-03,9",
					"4,x,y,2020-05-03,10",
					"4,x,y,2020-05-03,10",
					"5,x,y,2020-05-03,11",
					"5,x,y,2020-05-03,11"));

			// using batch table env to query.
			List<String> results = new ArrayList<>();
			TableEnvironment batchTEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
			batchTEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
			batchTEnv.useCatalog(hiveCatalog.getName());
			batchTEnv.executeSql("select * from db1.sink_table").collect()
					.forEachRemaining(r -> results.add(r.toString()));
			results.sort(String::compareTo);
			Assert.assertEquals(
					Arrays.asList(
							"1,a,b,2020-05-03,7",
							"1,a,b,2020-05-03,7",
							"2,p,q,2020-05-03,8",
							"2,p,q,2020-05-03,8",
							"3,x,y,2020-05-03,9",
							"3,x,y,2020-05-03,9",
							"4,x,y,2020-05-03,10",
							"4,x,y,2020-05-03,10",
							"5,x,y,2020-05-03,11",
							"5,x,y,2020-05-03,11"),
					results);

			pathConsumer.accept(URI.create(hiveCatalog.getHiveTable(
					ObjectPath.fromString("db1.sink_table")).getSd().getLocation()).getPath());
		} finally {
			tEnv.executeSql("drop database db1 cascade");
		}
	}

	private void assertBatch(String table, List<String> expected) {
		// using batch table env to query.
		List<String> results = new ArrayList<>();
		TableEnvironment batchTEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
		batchTEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
		batchTEnv.useCatalog(hiveCatalog.getName());
		batchTEnv.executeSql("select * from " + table).collect()
				.forEachRemaining(r -> results.add(r.toString()));
		results.sort(String::compareTo);
		expected.sort(String::compareTo);
		Assert.assertEquals(expected, results);
	}

	private RowTypeInfo createHiveDestTable(String dbName, String tblName, TableSchema tableSchema, int numPartCols) throws Exception {
		CatalogTable catalogTable = createHiveCatalogTable(tableSchema, numPartCols);
		hiveCatalog.createTable(new ObjectPath(dbName, tblName), catalogTable, false);
		return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
	}

	private RowTypeInfo createHiveDestTable(String dbName, String tblName, int numPartCols) throws Exception {
		TableSchema.Builder builder = new TableSchema.Builder();
		builder.fields(new String[]{"i", "l", "d", "s"},
				new DataType[]{
						DataTypes.INT(),
						DataTypes.BIGINT(),
						DataTypes.DOUBLE(),
						DataTypes.STRING()});
		return createHiveDestTable(dbName, tblName, builder.build(), numPartCols);
	}

	private CatalogTable createHiveCatalogTable(TableSchema tableSchema, int numPartCols) {
		if (numPartCols == 0) {
			return new CatalogTableImpl(
				tableSchema,
				new HashMap<String, String>() {{
					// creating a hive table needs explicit is_generic=false flag
					put(CatalogConfig.IS_GENERIC, String.valueOf(false));
				}},
				"");
		}
		String[] partCols = new String[numPartCols];
		System.arraycopy(tableSchema.getFieldNames(), tableSchema.getFieldNames().length - numPartCols, partCols, 0, numPartCols);
		return new CatalogTableImpl(
			tableSchema,
			Arrays.asList(partCols),
			new HashMap<String, String>() {{
				// creating a hive table needs explicit is_generic=false flag
				put(CatalogConfig.IS_GENERIC, String.valueOf(false));
			}},
			"");
	}

	private void verifyWrittenData(List<Row> expected, List<String> results) throws Exception {
		assertEquals(expected.size(), results.size());
		Set<String> expectedSet = new HashSet<>();
		for (int i = 0; i < results.size(); i++) {
			expectedSet.add(expected.get(i).toString().replaceAll(",", "\t"));
		}
		assertEquals(expectedSet, new HashSet<>(results));
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

	private static class CollectionTableSource extends InputFormatTableSource<Row> {

		private final Collection<Row> data;
		private final RowTypeInfo rowTypeInfo;

		CollectionTableSource(Collection<Row> data, RowTypeInfo rowTypeInfo) {
			this.data = data;
			this.rowTypeInfo = rowTypeInfo;
		}

		@Override
		public DataType getProducedDataType() {
			return TypeConversions.fromLegacyInfoToDataType(rowTypeInfo);
		}

		@Override
		public TypeInformation<Row> getReturnType() {
			return rowTypeInfo;
		}

		@Override
		public InputFormat<Row, ?> getInputFormat() {
			return new CollectionInputFormat<>(data, rowTypeInfo.createSerializer(new ExecutionConfig()));
		}

		@Override
		public TableSchema getTableSchema() {
			return new TableSchema.Builder().fields(rowTypeInfo.getFieldNames(),
					TypeConversions.fromLegacyInfoToDataType(rowTypeInfo.getFieldTypes())).build();
		}
	}
}
