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

package org.apache.flink.table.runtime.batch;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.runtime.utils.CommonTestData;
import org.apache.flink.table.sinks.csv.CsvTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.csv.CsvTableSource;
import org.apache.flink.table.util.FinalizeCsvSink;
import org.apache.flink.table.util.NodeResourceUtil;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link BatchTableSource}.
 */
public class BatchITCase extends AbstractTestBase {
	private File tmpFile = new File("/tmp/123");
	private File finalizedFile = new File("/tmp/finalized");
	private StreamExecutionEnvironment env;
	private BatchTableEnvironment tableEnv;

	@Before
	public void setUp() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tableEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tableEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE,
				NodeResourceUtil.InferMode.ONLY_SOURCE.toString());
		deleteFiles();
	}

	@After
	public void tearDown() {
		deleteFiles();
	}

	private void deleteFiles() {
		if (tmpFile.exists()) {
			if (tmpFile.isDirectory() && tmpFile.listFiles() != null) {
				for (File subFile : tmpFile.listFiles()) {
					subFile.delete();
				}
			}
			tmpFile.delete();
		}
		if (finalizedFile.exists()) {
			finalizedFile.delete();
		}
	}

	@Test
	public void testBatchExecCollect() throws Exception {

		CsvTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		Table table = tableEnv.scan("persons")
				.select("id, first, last, score");

		table.print("jobName");
	}

	@Test
	public void testInSubQueryThreshold() throws Exception {
		CsvTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tableEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM, 32);
		tableEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 32);
		tableEnv.registerTableSource("persons", csvTable);

		// default InSubQueryThreshold value is 20
		String sql = "SELECT id FROM persons WHERE " +
				"id IN (1,2,6,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26) OR " +
				"`first` IN ('Mike','Sam','Liz','a','b','c','d','e','f','i','j','k','l'," +
				"'m','n','o','p','q','r','s','t')";

		Table table = tableEnv.sqlQuery(sql);
		Seq<Row> results = table.collect();
		List<Row> result = new ArrayList<>(JavaConverters.asJavaCollectionConverter(results).asJavaCollection());

		String expected = "1\n2\n3\n5\n6";
		TestBaseUtils.compareOrderedResultAsText(result, expected);
	}

	@Test
	public void testBatchExecSink() throws Exception {
		env.setParallelism(1);
		BatchTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		Table table = tableEnv.scan("persons")
				.select("first, last, score");

		CsvTableSink sink = new CsvTableSink(tmpFile.getPath(), "|");
		table.writeToSink(sink);
		tableEnv.execute();

		BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}
		reader.close();
		String expected = "Mike|Smith|12.3Bob|Taylor|45.6Sam|Miller|7.89Peter|Smith|0.12Liz|Williams|34.5Sally|Miller|6.78Alice|Smith|90.1Kelly|Williams|2.34";
		assertEquals(expected, sb.toString());
	}

	@Test(expected = TableException.class)
	public void testNoTableSink() {
		env.setParallelism(1);
		BatchTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		tableEnv.scan("persons")
				.select("first, last, score");

		tableEnv.execute();
	}

	@Test
	public void testIOVertex() throws IOException {
		env.setParallelism(1);
		BatchTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		Table table = tableEnv.scan("persons")
				.select("first, last, score");

		FinalizeCsvSink sink = new FinalizeCsvSink(
				tmpFile.getPath(), "|", finalizedFile.getAbsolutePath());
		table.writeToSink(sink);
		tableEnv.execute();

		BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}
		reader.close();
		String expected = "Mike|Smith|12.3Bob|Taylor|45.6Sam|Miller|7.89Peter|Smith|0.12Liz|Williams|34.5Sally|Miller|6.78Alice|Smith|90.1Kelly|Williams|2.34";
		assertEquals(expected, sb.toString());

		// Test whether the finalizeGlobal() is called
		assertTrue((finalizedFile).exists());
	}

	@Test
	public void testSinkParallelism() throws IOException {
		tableEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 5);
		BatchTableSource csvTable = CommonTestData.getCsvTableSource();

		tableEnv.registerTableSource("persons", csvTable);

		Table table = tableEnv.scan("persons")
				.select("first, last, score");

		FinalizeCsvSink sink = new FinalizeCsvSink(
				tmpFile.getPath(), "|", finalizedFile.getAbsolutePath());
		table.writeToSink(sink);
		tableEnv.execute();

		assertEquals(5, tmpFile.listFiles().length);
	}
}
