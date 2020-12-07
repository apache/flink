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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.Row;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for {@link ParquetTableSource}.
 */
public class ParquetTableSourceITCase extends MultipleProgramsTestBase {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();
	private static Path testPath;

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();

	public ParquetTableSourceITCase() {
		super(TestExecutionMode.COLLECTION);
	}

	@BeforeClass
	public static void setup() throws Exception {
		testPath = createTestParquetFile(1000);
	}

	@Test
	public void testFullScan() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);
		ParquetTableSource tableSource = createParquetTableSource(testPath);
		((TableEnvironmentInternal) batchTableEnvironment).registerTableSourceInternal("ParquetTable", tableSource);
		String query =
			"SELECT foo " +
			"FROM ParquetTable";

		Table table = batchTableEnvironment.sqlQuery(query);
		DataSet<Row> dataSet = batchTableEnvironment.toDataSet(table, Row.class);
		List<Row> result = dataSet.collect();

		assertEquals(1000, result.size());
	}

	@Test
	public void testScanWithProjectionAndFilter() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);
		ParquetTableSource tableSource = createParquetTableSource(testPath);
		((TableEnvironmentInternal) batchTableEnvironment).registerTableSourceInternal("ParquetTable", tableSource);
		String query =
			"SELECT foo " +
			"FROM ParquetTable WHERE foo >= 1 AND bar.spam >= 30 AND CARDINALITY(arr) >= 1 AND arr[1] <= 50";

		Table table = batchTableEnvironment.sqlQuery(query);
		DataSet<Row> dataSet = batchTableEnvironment.toDataSet(table, Row.class);
		List<Row> result = dataSet.collect();

		assertEquals(21, result.size());
	}

	/**
	 * Create test Parquet table source that reads a test file created by {@link #createTestParquetFile(int)}.
	 */
	private ParquetTableSource createParquetTableSource(Path path) throws IOException {
		MessageType nestedSchema = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);
		ParquetTableSource parquetTableSource = ParquetTableSource.builder()
			.path(path.getPath())
			.forParquetSchema(nestedSchema)
			.build();
		return parquetTableSource;
	}

	/**
	 * Create a test Parquet file with a given number of rows.
	 */
	private static Path createTestParquetFile(int numberOfRows) throws Exception {
		List<IndexedRecord> records = TestUtil.createRecordList(numberOfRows);
		Path path = TestUtil.createTempParquetFile(tempRoot.getRoot(), TestUtil.NESTED_SCHEMA, records, new Configuration());
		return path;
	}
}
