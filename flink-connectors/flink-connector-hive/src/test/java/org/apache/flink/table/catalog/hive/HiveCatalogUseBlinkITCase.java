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

import org.apache.flink.connectors.hive.FlinkStandaloneHiveRunner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
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
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.FileUtils;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.conf.HiveConf;
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
import java.util.Arrays;
import java.util.HashMap;
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
		TableEnvironment tEnv = TableEnvironment.create(
				EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());

		BatchTestBase.configForMiniCluster(tEnv.getConfig());

		tEnv.registerCatalog("myhive", hiveCatalog);
		tEnv.useCatalog("myhive");

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
				new ObjectPath(HiveCatalog.DEFAULT_DB, sourceTableName),
				source,
				false
		);

		hiveCatalog.createTable(
				new ObjectPath(HiveCatalog.DEFAULT_DB, sinkTableName),
				sink,
				false
		);

		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "myudf"),
				new CatalogFunctionImpl(TestHiveSimpleUDF.class.getCanonicalName(), new HashMap<>()),
				false);
		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "mygenericudf"),
				new CatalogFunctionImpl(TestHiveGenericUDF.class.getCanonicalName(), new HashMap<>()),
				false);
		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "myudtf"),
				new CatalogFunctionImpl(TestHiveUDTF.class.getCanonicalName(), new HashMap<>()),
				false);
		hiveCatalog.createFunction(
				new ObjectPath(HiveCatalog.DEFAULT_DB, "myudaf"),
				new CatalogFunctionImpl(GenericUDAFSum.class.getCanonicalName(), new HashMap<>()),
				false);

		String innerSql = format("select mygenericudf(myudf(name), 1) as a, mygenericudf(myudf(age), 1) as b," +
				" s from %s, lateral table(myudtf(name, 1)) as T(s)", sourceTableName);

		tEnv.sqlUpdate(
				format("insert into %s select a, s, sum(b), myudaf(b) from (%s) group by a, s",
						sinkTableName,
						innerSql));
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
		List<String> results = Arrays.stream(builder.toString().split("\n"))
				.filter(s -> !s.isEmpty())
				.collect(Collectors.toList());
		results.sort(String::compareTo);
		Assert.assertEquals(Arrays.asList("1,1,2,2", "2,2,4,4", "3,3,6,6"), results);
	}
}
