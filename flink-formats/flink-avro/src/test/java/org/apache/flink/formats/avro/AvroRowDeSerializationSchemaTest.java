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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.typeutils.AvroRecordClassConverter;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for the Avro serialization and deserialization schema.
 */
public class AvroRowDeSerializationSchemaTest {

	@Test
	public void testSerializeDeserializeSimpleRow() throws IOException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getSimpleTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeSimpleRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getSimpleTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testDeserializeRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getSimpleTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeDeserializeComplexRow() throws IOException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeComplexRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testDeserializeComplexRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializability() throws IOException, ClassNotFoundException {
		final Tuple3<Class<? extends SpecificRecordBase>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serOrig = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserOrig = new AvroRowDeserializationSchema(testData.f0);

		byte[] serBytes = InstantiationUtil.serializeObject(serOrig);
		byte[] deserBytes = InstantiationUtil.serializeObject(deserOrig);

		AvroRowSerializationSchema serCopy =
			InstantiationUtil.deserializeObject(serBytes, Thread.currentThread().getContextClassLoader());
		AvroRowDeserializationSchema deserCopy =
			InstantiationUtil.deserializeObject(deserBytes, Thread.currentThread().getContextClassLoader());

		final byte[] bytes = serCopy.serialize(testData.f2);
		deserCopy.deserialize(bytes);
		deserCopy.deserialize(bytes);
		final Row actual = deserCopy.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testHasMapFieldsAvroClass(){
		AvroRowDeserializationSchema schema = new AvroRowDeserializationSchema(HasMapFieldsAvroClass.class);
		RowTypeInfo returnType = (RowTypeInfo) schema.getProducedType();

		assertEquals(Types.MAP(Types.STRING, Types.STRING), returnType.getTypeAt(0));
		assertEquals(Types.MAP(Types.STRING, Types.INT), returnType.getTypeAt(1));
		assertEquals(Types.MAP(Types.STRING, Types.LONG), returnType.getTypeAt(2));
		assertEquals(Types.MAP(Types.STRING, Types.FLOAT), returnType.getTypeAt(3));
		assertEquals(Types.MAP(Types.STRING, Types.DOUBLE), returnType.getTypeAt(4));
		assertEquals(Types.MAP(Types.STRING, Types.BOOLEAN), returnType.getTypeAt(5));
		assertEquals(Types.MAP(Types.STRING, AvroRecordClassConverter.convert(Address.class)), returnType.getTypeAt(6));
	}

	@Test
	public void testHasArrayFieldsAvroClass(){
		AvroRowDeserializationSchema schema = new AvroRowDeserializationSchema(HasArrayFieldsAvroClass.class);
		RowTypeInfo returnType = (RowTypeInfo) schema.getProducedType();

		assertEquals(Types.OBJECT_ARRAY(Types.STRING), returnType.getTypeAt(0));
		assertEquals(Types.OBJECT_ARRAY(Types.INT), returnType.getTypeAt(1));
		assertEquals(Types.OBJECT_ARRAY(Types.LONG), returnType.getTypeAt(2));
		assertEquals(Types.OBJECT_ARRAY(Types.FLOAT), returnType.getTypeAt(3));
		assertEquals(Types.OBJECT_ARRAY(Types.DOUBLE), returnType.getTypeAt(4));
		assertEquals(Types.OBJECT_ARRAY(Types.BOOLEAN), returnType.getTypeAt(5));
		assertEquals(Types.OBJECT_ARRAY(AvroRecordClassConverter.convert(Address.class)), returnType.getTypeAt(6));
	}

	/**
	 * Avro record that has map fields.
	 */
	@SuppressWarnings("unused")
	public static class HasMapFieldsAvroClass extends SpecificRecordBase {

		public static final String[] FIELD_NAMES = new String[]{
			"strMapField", "intMapField", "longMapField", "floatMapField",
			"doubleMapField", "boolMapField", "recordMapField"};
		public static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
			Types.MAP(Types.STRING, Types.STRING), Types.MAP(Types.STRING, Types.INT),
			Types.MAP(Types.STRING, Types.LONG), Types.MAP(Types.STRING, Types.FLOAT),
			Types.MAP(Types.STRING, Types.DOUBLE), Types.MAP(Types.STRING, Types.BOOLEAN),
			Types.MAP(Types.STRING, new AvroTypeInfo(Address.class))};

		//CHECKSTYLE.OFF: StaticVariableNameCheck - Avro accesses this field by name via reflection.
		public static Schema SCHEMA$ = AvroTestUtils.createFlatAvroSchema(FIELD_NAMES, FIELD_TYPES);
		//CHECKSTYLE.ON: StaticVariableNameCheck

		public Map<String, String> strMapField;
		public Map<String, Integer> intMapField;
		public Map<String, Long> longMapField;
		public Map<String, Float> floatMapField;
		public Map<String, Double> doubleMapField;
		public Map<String, Boolean> boolMapField;
		public Map<String, Address> recordMapField;

		@Override
		public Schema getSchema() {
			return null;
		}

		@Override
		public Object get(int field) {
			return null;
		}

		@Override
		public void put(int field, Object value) { }
	}

	/**
	 * Avro record that has array fields.
	 */
	@SuppressWarnings("unused")
	public static class HasArrayFieldsAvroClass extends SpecificRecordBase {

		public static final String[] FIELD_NAMES = new String[]{
			"strArrayField", "intArrayField", "longArrayField", "floatArrayField",
			"doubleArrayField", "boolArrayField", "recordArrayField"};
		public static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
			Types.OBJECT_ARRAY(Types.STRING), Types.OBJECT_ARRAY(Types.INT),
			Types.OBJECT_ARRAY(Types.LONG), Types.OBJECT_ARRAY(Types.FLOAT),
			Types.OBJECT_ARRAY(Types.DOUBLE), Types.OBJECT_ARRAY(Types.BOOLEAN),
			Types.OBJECT_ARRAY(new AvroTypeInfo(Address.class))};

		//CHECKSTYLE.OFF: StaticVariableNameCheck - Avro accesses this field by name via reflection.
		public static Schema SCHEMA$ = AvroTestUtils.createFlatAvroSchema(FIELD_NAMES, FIELD_TYPES);
		//CHECKSTYLE.ON: StaticVariableNameCheck

		public List<String> strArrayField;
		public List<Integer> intArrayField;
		public List<Long> longArrayField;
		public List<Float> floatArrayField;
		public List<Double> doubleArrayField;
		public List<Boolean> boolArrayField;
		public List<Address> recordArrayField;

		@Override
		public Schema getSchema() {
			return null;
		}

		@Override
		public Object get(int field) {
			return null;
		}

		@Override
		public void put(int field, Object value) { }
	}
}
