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
import org.apache.flink.formats.parquet.generated.ArrayItem;
import org.apache.flink.formats.parquet.generated.Bar;
import org.apache.flink.formats.parquet.generated.MapItem;
import org.apache.flink.formats.parquet.generated.NestedRecord;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.Row;

import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit Tests for {@link ParquetTableSource}.
 */
public class ParquetTableSourceITCase extends MultipleProgramsTestBase {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();

	public ParquetTableSourceITCase() {
		super(TestExecutionMode.COLLECTION);
	}

	@Test
	public void testFullScan() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);
		ParquetTableSource tableSource = createParquetTableSource(1000);
		batchTableEnvironment.registerTableSource("ParquetTable", tableSource);
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
		ParquetTableSource tableSource = createParquetTableSource(1000);
		batchTableEnvironment.registerTableSource("ParquetTable", tableSource);
		String query =
			"SELECT foo " +
			"FROM ParquetTable WHERE bar.spam >= 30 AND CARDINALITY(arr) >= 1 AND arr[1] <= 50";

		Table table = batchTableEnvironment.sqlQuery(query);
		DataSet<Row> dataSet = batchTableEnvironment.toDataSet(table, Row.class);
		List<Row> result = dataSet.collect();

		assertEquals(21, result.size());
	}

	private ParquetTableSource createParquetTableSource(int number) throws IOException {
		List<IndexedRecord> records = createRecordList(number);
		Path path = TestUtil.createTempParquetFile(tempRoot.getRoot(), TestUtil.NESTED_SCHEMA, records);

		MessageType nestedSchema = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);

		ParquetTableSource parquetTableSource = ParquetTableSource.builder()
			.path(path.getPath())
			.forParquetSchema(nestedSchema)
			.build();
		return parquetTableSource;
	}

	private List<IndexedRecord> createRecordList(long number) {
		List<IndexedRecord> records = new ArrayList<>();
		for (long i = 0; i < number; i++) {
			final Bar bar = Bar.newBuilder()
				.setSpam(i).build();

			final ArrayItem arrayItem = ArrayItem.newBuilder()
				.setType("color")
				.setValue(i).build();

			final MapItem mapItem = MapItem.newBuilder()
				.setType("map")
				.setValue("hashMap").build();

			List<ArrayItem> nestedArray = new ArrayList<>();
			nestedArray.add(arrayItem);

			Map<CharSequence, MapItem> nestedMap = new HashMap<>();
			nestedMap.put("mapItem", mapItem);

			List<Long> longArray = new ArrayList<>();
			longArray.add(i);

			List<CharSequence> stringArray = new ArrayList<>();
			stringArray.add("String");

			final NestedRecord nestedRecord = NestedRecord.newBuilder()
				.setBar(bar)
				.setNestedArray(nestedArray)
				.setStrArray(stringArray)
				.setNestedMap(nestedMap)
				.setArr(longArray).build();

			records.add(nestedRecord);
		}

		return records;
	}
}
