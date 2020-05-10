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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.FlinkStandaloneHiveRunner;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableBuilder;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.functions.hive.util.TestHiveGenericUDF;
import org.apache.flink.table.functions.hive.util.TestHiveSimpleUDF;
import org.apache.flink.table.functions.hive.util.TestHiveUDTF;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestingRetractSink;
import org.apache.flink.table.util.JavaScalaConversionUtil;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * IT case for HiveCatalog.
 * TODO: move to flink-connector-hive-test end-to-end test module once it's setup
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveCatalogUseBlinkITCase extends AbstractTestBase {

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private static HiveCatalog hiveCatalog;

	private String sourceTableName = "csv_source";
	private String sinkTableName = "csv_sink";

	@BeforeClass
	public static void createCatalog() {
		HiveConf hiveConf = hiveShell.getHiveConf();
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
	public void testBlinkUdf() throws Exception {
		TableSchema schema = TableSchema.builder()
				.field("name", DataTypes.STRING())
				.field("age", DataTypes.INT())
				.build();

		FormatDescriptor format = new OldCsv()
				.field("name", Types.STRING())
				.field("age", Types.INT());

		CatalogTable source =
				new CatalogTableBuilder(
						new FileSystem().path(this.getClass().getResource("/csv/test.csv").getPath()),
						schema)
						.withFormat(format)
						.inAppendMode()
						.withComment("Comment.")
						.build();

		hiveCatalog.createTable(
				new ObjectPath(HiveCatalog.DEFAULT_DB, sourceTableName),
				source,
				false
		);

		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "myudf"),
				new CatalogFunctionImpl(TestHiveSimpleUDF.class.getCanonicalName()),
				false);
		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "mygenericudf"),
				new CatalogFunctionImpl(TestHiveGenericUDF.class.getCanonicalName()),
				false);
		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "myudtf"),
				new CatalogFunctionImpl(TestHiveUDTF.class.getCanonicalName()),
				false);
		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "myudaf"),
				new CatalogFunctionImpl(GenericUDAFSum.class.getCanonicalName()),
				false);

		testUdf(true);
		testUdf(false);
	}

	private void testUdf(boolean batch) throws Exception {
		StreamExecutionEnvironment env = null;
		TableEnvironment tEnv;
		EnvironmentSettings.Builder envBuilder = EnvironmentSettings.newInstance().useBlinkPlanner();
		if (batch) {
			envBuilder.inBatchMode();
		} else {
			envBuilder.inStreamingMode();
		}
		if (batch) {
			tEnv = TableEnvironment.create(envBuilder.build());
		} else {
			env = StreamExecutionEnvironment.getExecutionEnvironment();
			tEnv = StreamTableEnvironment.create(env, envBuilder.build());
		}

		BatchTestBase.configForMiniCluster(tEnv.getConfig());

		tEnv.registerCatalog("myhive", hiveCatalog);
		tEnv.useCatalog("myhive");

		String innerSql = format("select mygenericudf(myudf(name), 1) as a, mygenericudf(myudf(age), 1) as b," +
				" s from %s, lateral table(myudtf(name, 1)) as T(s)", sourceTableName);

		String selectSql = format("select a, s, sum(b), myudaf(b) from (%s) group by a, s", innerSql);

		List<String> results;
		if (batch) {
			Path p = Paths.get(tempFolder.newFolder().getAbsolutePath(), "test.csv");

			TableSchema sinkSchema = TableSchema.builder()
					.field("name1", Types.STRING())
					.field("name2", Types.STRING())
					.field("sum1", Types.INT())
					.field("sum2", Types.LONG())
					.build();

			FormatDescriptor sinkFormat = new OldCsv()
					.field("name1", Types.STRING())
					.field("name2", Types.STRING())
					.field("sum1", Types.INT())
					.field("sum2", Types.LONG());
			CatalogTable sink =
					new CatalogTableBuilder(
							new FileSystem().path(p.toAbsolutePath().toString()),
							sinkSchema)
							.withFormat(sinkFormat)
							.inAppendMode()
							.withComment("Comment.")
							.build();

			hiveCatalog.createTable(
					new ObjectPath(HiveCatalog.DEFAULT_DB, sinkTableName),
					sink,
					false
			);

			tEnv.sqlUpdate(format("insert into %s " + selectSql, sinkTableName));
			tEnv.execute("myjob");

			// assert written result
			StringBuilder builder = new StringBuilder();
			try (Stream<Path> paths = Files.walk(Paths.get(p.toAbsolutePath().toString()))) {
				paths.filter(Files::isRegularFile).forEach(path -> {
					try {
						String content = FileUtils.readFileUtf8(path.toFile());
						if (content.isEmpty()) {
							return;
						}
						builder.append(content);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
			}
			results = Arrays.stream(builder.toString().split("\n"))
					.filter(s -> !s.isEmpty())
					.collect(Collectors.toList());
		} else {
			StreamTableEnvironment streamTEnv = (StreamTableEnvironment) tEnv;
			TestingRetractSink sink = new TestingRetractSink();
			streamTEnv.toRetractStream(tEnv.sqlQuery(selectSql), Row.class)
					.map(new JavaToScala())
					.addSink((SinkFunction) sink);
			env.execute("");
			results = JavaScalaConversionUtil.toJava(sink.getRetractResults());
		}

		results = new ArrayList<>(results);
		results.sort(String::compareTo);
		Assert.assertEquals(Arrays.asList("1,1,2,2", "2,2,4,4", "3,3,6,6"), results);
	}

	@Test
	public void testTimestampUDF() throws Exception {
		hiveCatalog.createFunction(new ObjectPath("default", "myyear"),
				new CatalogFunctionImpl(UDFYear.class.getCanonicalName()),
				false);

		hiveShell.execute("create table src(ts timestamp)");
		try {
			HiveTestUtils.createTextTableInserter(hiveShell, "default", "src")
					.addRow(new Object[]{Timestamp.valueOf("2013-07-15 10:00:00")})
					.addRow(new Object[]{Timestamp.valueOf("2019-05-23 17:32:55")})
					.commit();
			TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
			tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
			tableEnv.useCatalog(hiveCatalog.getName());

			List<Row> results = Lists.newArrayList(
					tableEnv.sqlQuery("select myyear(ts) as y from src").execute().collect());
			Assert.assertEquals(2, results.size());
			Assert.assertEquals("[2013, 2019]", results.toString());
		} finally {
			hiveShell.execute("drop table src");
		}
	}

	@Test
	public void testDateUDF() throws Exception {
		hiveCatalog.createFunction(new ObjectPath("default", "mymonth"),
				new CatalogFunctionImpl(UDFMonth.class.getCanonicalName()),
				false);

		hiveShell.execute("create table src(dt date)");
		try {
			HiveTestUtils.createTextTableInserter(hiveShell, "default", "src")
					.addRow(new Object[]{Date.valueOf("2019-01-19")})
					.addRow(new Object[]{Date.valueOf("2019-03-02")})
					.commit();
			TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
			tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
			tableEnv.useCatalog(hiveCatalog.getName());

			List<Row> results = Lists.newArrayList(
					tableEnv.sqlQuery("select mymonth(dt) as m from src order by m").execute().collect());
			Assert.assertEquals(2, results.size());
			Assert.assertEquals("[1, 3]", results.toString());
		} finally {
			hiveShell.execute("drop table src");
		}
	}

	private static class JavaToScala implements MapFunction<Tuple2<Boolean, Row>, scala.Tuple2<Boolean, Row>> {

		@Override
		public scala.Tuple2<Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
			return new scala.Tuple2<>(value.f0, value.f1);
		}
	}
}
