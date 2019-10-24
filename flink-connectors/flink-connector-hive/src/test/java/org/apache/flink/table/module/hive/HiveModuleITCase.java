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

package org.apache.flink.table.module.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableBuilder;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * IT case for {@link HiveModule}.
 */
public class HiveModuleITCase extends AbstractTestBase {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private String sourceTableName = "csv_source";
	private String sinkTableName = "csv_sink";
	private final String testFunctionName = "reverse";

	@Test
	public void testHiveBuiltinFunction() throws Exception {
		TableEnvironment tEnv = TableEnvironment.create(
				EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());

		BatchTestBase.configForMiniCluster(tEnv.getConfig());

		TableSchema schema = TableSchema.builder()
				.field("name", DataTypes.STRING())
				.build();

		FormatDescriptor format = new OldCsv()
				.field("name", Types.STRING());

		CatalogTable source =
				new CatalogTableBuilder(
						new FileSystem().path(this.getClass().getResource("/csv/test2.csv").getPath()),
						schema)
						.withFormat(format)
						.inAppendMode()
						.withComment("Comment.")
						.build();

		Path p = Paths.get(tempFolder.newFolder().getAbsolutePath(), "test2.csv");

		TableSchema sinkSchema = TableSchema.builder()
				.field("reverse", Types.STRING())
				.build();

		FormatDescriptor sinkFormat = new OldCsv()
				.field("reverse", Types.STRING());
		CatalogTable sink =
				new CatalogTableBuilder(
						new FileSystem().path(p.toAbsolutePath().toString()),
						sinkSchema)
						.withFormat(sinkFormat)
						.inAppendMode()
						.withComment("Comment.")
						.build();

		Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();

		catalog.createTable(
				new ObjectPath(tEnv.getCurrentDatabase(), sourceTableName),
				source,
				false
		);

		catalog.createTable(
				new ObjectPath(tEnv.getCurrentDatabase(), sinkTableName),
				sink,
				false
		);

		tEnv.loadModule("hive", new HiveModule("2.3.1"));

		String innerSql = format("select %s(name) as a from %s", testFunctionName, sourceTableName);

		tEnv.sqlUpdate(
				format("insert into %s select a from (%s)",
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
		Assert.assertEquals(Arrays.asList("mot", "ydna", "ymmij"), results);
	}
}
