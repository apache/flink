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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.generated.ArrayItem;
import org.apache.flink.formats.parquet.generated.Bar;
import org.apache.flink.formats.parquet.generated.MapItem;
import org.apache.flink.formats.parquet.generated.NestedRecord;
import org.apache.flink.formats.parquet.generated.SimpleRecord;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.avro.Schema.Type.NULL;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestUtil {
	@ClassRule
	public static TemporaryFolder temp = new TemporaryFolder();
	public static final Configuration TEST_CONFIGURATION = new Configuration();
	public static final Schema NESTED_SCHEMA = getTestSchema("nested.avsc");
	public static final Schema SIMPLE_SCHEMA = getTestSchema("simple.avsc");

	protected static final Type[] SIMPLE_TYPES = {
		Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).named("foo"),
		Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
			.as(OriginalType.UTF8).named("bar")};

	protected static final Type[] NESTED_TYPES = {
		Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).named("foo"),
		Types.optionalMap()
			.value(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("value"))
			.named("spamMap"),
		Types.optionalGroup().addField(
			Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).named("spam")).named("bar"),
		Types.optionalGroup()
			.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REPEATED)
				.named("array")).as(OriginalType.LIST)
			.named("arr"),
		Types.optionalGroup()
			.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REPEATED)
				.named("array")).as(OriginalType.LIST)
			.named("strArray"),
		Types.optionalMap().value(Types.optionalGroup()
			.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("type"))
			.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("value"))
			.named("value"))
			.named("nestedMap"),
		Types.optionalGroup().addField(Types.repeatedGroup()
			.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("type"))
			.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("value"))
			.named("array")).as(OriginalType.LIST)
			.named("nestedArray")
	};
	private AvroSchemaConverter converter = new AvroSchemaConverter();

	@Test
	public void testSimpleSchemaConversion() {
		MessageType simpleType = converter.convert(SIMPLE_SCHEMA);
		assertEquals(SIMPLE_TYPES.length, simpleType.getFieldCount());

		for (int i = 0; i < simpleType.getFieldCount(); i++) {
			assertEquals(SIMPLE_TYPES[i], simpleType.getType(i));
		}
	}

	/**
	 * This test case is using the parquet native avro schema converter that convert
	 * avro map value typed to required. But komodo write parquet files with map value
	 * type as optional. Thus, the test case is meaningless. But keep it here for later
	 * reference.
	 */
	@Ignore
	public void testNestedSchemaConversion() {
		MessageType nestedType = converter.convert(NESTED_SCHEMA);
		assertEquals(NESTED_TYPES.length, nestedType.getFieldCount());

		for (int i = 0; i < nestedType.getFieldCount(); i++) {
			assertEquals(NESTED_TYPES[i], nestedType.getType(i));
		}
	}

	public static Path createTempParquetFile(
		TemporaryFolder temporaryFolder,
		Schema schema,
		IndexedRecord record,
		int recordNum) throws IOException {
		File root = temporaryFolder.getRoot();
		Path path = new Path(root.getPath(), UUID.randomUUID().toString());
		ParquetWriter writer = AvroParquetWriter.builder(
			new org.apache.hadoop.fs.Path(path.toUri())).withSchema(schema).build();

		for (int i = 0; i < recordNum; i++) {
			writer.write(record);
		}

		writer.close();
		return path;
	}

	private static Schema getTestSchema(String schemaName) {
		try {
			InputStream inputStream = TestUtil.class.getClassLoader()
				.getResourceAsStream("avro/" + schemaName);
			return new Schema.Parser().parse(inputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected Schema unWrapSchema(Schema o) {
		List<Schema> schemas = o.getTypes();
		Preconditions.checkArgument(schemas.size() == 2, "Invalid union type");
		return schemas.get(0).getType() == NULL ? schemas.get(1) : schemas.get(0);
	}

	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getSimpleRecordTestData() {
		final SimpleRecord simpleRecord = SimpleRecord.newBuilder()
			.setBar("test_simple")
			.setFoo(1L).build();

		final Row simpleRow = new Row(2);
		simpleRow.setField(0, 1L);
		simpleRow.setField(1, "test_simple");

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
			.setValue("yellow").build();

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
		arrayItemRow.setField(1, "yellow");

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

	public static void main(String[] args) throws IOException {
		TestUtil testUtil = new TestUtil();
		temp.create();

		GenericData.Record record = new GenericRecordBuilder(SIMPLE_SCHEMA)
			.set("bar", "test")
			.set("foo", 32L).build();

		Path path = testUtil.createTempParquetFile(temp, SIMPLE_SCHEMA, record, 100);
		ParquetReader<Group> reader = ParquetReader.builder(
			new GroupReadSupport(), new org.apache.hadoop.fs.Path(path.toUri())).withConf(new Configuration()).build();

		Group group = reader.read();
		while (group != null) {
			System.out.println(group.toString());
			group = reader.read();
		}
	}
}
