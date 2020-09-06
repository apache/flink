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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.generated.ArrayItem;
import org.apache.flink.formats.parquet.generated.Bar;
import org.apache.flink.formats.parquet.generated.MapItem;
import org.apache.flink.formats.parquet.generated.NestedRecord;
import org.apache.flink.formats.parquet.generated.SimpleRecord;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Utilities for testing schema conversion and test parquet file creation.
 */
public class TestUtil {
	private static final TypeInformation<Row[]> nestedArray = Types.OBJECT_ARRAY(Types.ROW_NAMED(
		new String[] {"type", "value"}, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));

	@SuppressWarnings("unchecked")
	private static final TypeInformation<Map<String, Row>> nestedMap = Types.MAP(BasicTypeInfo.STRING_TYPE_INFO,
		Types.ROW_NAMED(new String[] {"type", "value"},
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();
	public static final Schema NESTED_SCHEMA = getTestSchema("nested.avsc");
	public static final Schema SIMPLE_SCHEMA = getTestSchema("simple.avsc");

	public static final TypeInformation<Row> SIMPLE_ROW_TYPE = Types.ROW_NAMED(new String[] {"foo", "bar", "arr"},
		BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO);

	@SuppressWarnings("unchecked")
	public static final TypeInformation<Row> NESTED_ROW_TYPE = Types.ROW_NAMED(
		new String[] {"foo", "spamMap", "bar", "arr", "strArray", "nestedMap", "nestedArray"},
		BasicTypeInfo.LONG_TYPE_INFO,
		Types.MAP(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
		Types.ROW_NAMED(new String[] {"spam"}, BasicTypeInfo.LONG_TYPE_INFO),
		BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO,
		BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
		nestedMap,
		nestedArray);

	public static Path createTempParquetFile(File folder, Schema schema, List<IndexedRecord> records) throws IOException {
		Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
		ParquetWriter<IndexedRecord> writer = AvroParquetWriter.<IndexedRecord>builder(
			new org.apache.hadoop.fs.Path(path.toUri())).withSchema(schema).withRowGroupSize(10).build();

		for (IndexedRecord record : records) {
			writer.write(record);
		}

		writer.close();
		return path;
	}

	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getSimpleRecordTestData() {
		Long[] longArray = {1L};
		final SimpleRecord simpleRecord = SimpleRecord.newBuilder()
			.setBar("test_simple")
			.setFoo(1L)
			.setArr(Arrays.asList(longArray)).build();

		final Row simpleRow = new Row(3);
		simpleRow.setField(0, 1L);
		simpleRow.setField(1, "test_simple");
		simpleRow.setField(2, longArray);

		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> t = new Tuple3<>();
		t.f0 = SimpleRecord.class;
		t.f1 = simpleRecord;
		t.f2 = simpleRow;

		return t;
	}

	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getNestedRecordTestData() {
		final Bar bar = Bar.newBuilder()
			.setSpam(1L).build();

		final ArrayItem arrayItem = ArrayItem.newBuilder()
			.setType("color")
			.setValue(1L).build();

		final MapItem mapItem = MapItem.newBuilder()
			.setType("map")
			.setValue("hashMap").build();

		List<ArrayItem> nestedArray = new ArrayList<>();
		nestedArray.add(arrayItem);

		Map<CharSequence, MapItem> nestedMap = new HashMap<>();
		nestedMap.put("mapItem", mapItem);

		List<Long> longArray = new ArrayList<>();
		longArray.add(1L);

		List<CharSequence> stringArray = new ArrayList<>();
		stringArray.add("String");

		Long[] primitiveLongArray = {1L};
		String[] primitiveStringArray = {"String"};

		final NestedRecord nestedRecord = NestedRecord.newBuilder()
			.setBar(bar)
			.setNestedArray(nestedArray)
			.setStrArray(stringArray)
			.setNestedMap(nestedMap)
			.setArr(longArray).build();

		final Row barRow = new Row(1);
		barRow.setField(0, 1L);

		final Row arrayItemRow = new Row(2);
		arrayItemRow.setField(0, "color");
		arrayItemRow.setField(1, 1L);

		final Row mapItemRow = new Row(2);
		mapItemRow.setField(0, "map");
		mapItemRow.setField(1, "hashMap");

		Row[] nestedRowArray = {arrayItemRow};
		Map<String, Row> nestedRowMap = new HashMap<>();
		nestedRowMap.put("mapItem", mapItemRow);

		final Row nestedRow = new Row(7);
		nestedRow.setField(2, barRow);
		nestedRow.setField(3, primitiveLongArray);
		nestedRow.setField(4, primitiveStringArray);
		nestedRow.setField(5, nestedRowMap);
		nestedRow.setField(6, nestedRowArray);

		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> t = new Tuple3<>();
		t.f0 = NestedRecord.class;
		t.f1 = nestedRecord;
		t.f2 = nestedRow;

		return t;
	}

	/**
	 * Create a list of NestedRecord with the NESTED_SCHEMA.
	 */
	public static List<IndexedRecord> createRecordList(long numberOfRows) {
		List<IndexedRecord> records = new ArrayList<>(0);
		for (long i = 0; i < numberOfRows; i++) {
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
				.setFoo(1L)
				.setBar(bar)
				.setNestedArray(nestedArray)
				.setStrArray(stringArray)
				.setNestedMap(nestedMap)
				.setArr(longArray).build();

			records.add(nestedRecord);
		}

		return records;
	}

	public static RuntimeContext getMockRuntimeContext() {
		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		return mockContext;
	}

	public static Schema getTestSchema(String schemaName) {
		try {
			InputStream inputStream = TestUtil.class.getClassLoader()
				.getResourceAsStream("avro/" + schemaName);
			return new Schema.Parser().parse(inputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
