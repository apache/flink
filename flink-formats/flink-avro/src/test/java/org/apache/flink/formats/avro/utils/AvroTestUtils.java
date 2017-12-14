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

package org.apache.flink.formats.avro.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.Colors;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Utilities for creating Avro Schemas.
 */
public final class AvroTestUtils {

	private static final String NAMESPACE = "org.apache.flink.streaming.connectors.kafka";

	/**
	 * Creates a flat Avro Schema for testing.
	 */
	public static Schema createFlatAvroSchema(String[] fieldNames, TypeInformation[] fieldTypes) {
		final SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
			.record("BasicAvroRecord")
			.namespace(NAMESPACE)
			.fields();

		final Schema nullSchema = Schema.create(Schema.Type.NULL);

		for (int i = 0; i < fieldNames.length; i++) {
			Schema schema = ReflectData.get().getSchema(fieldTypes[i].getTypeClass());
			Schema unionSchema = Schema.createUnion(Arrays.asList(nullSchema, schema));
			fieldAssembler.name(fieldNames[i]).type(unionSchema).noDefault();
		}

		return fieldAssembler.endRecord();
	}

	/**
	 * Tests a simple Avro data types without nesting.
	 */
	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getSimpleTestData() {
		final Address addr = Address.newBuilder()
			.setNum(42)
			.setStreet("Main Street 42")
			.setCity("Test City")
			.setState("Test State")
			.setZip("12345")
			.build();

		final Row rowAddr = new Row(5);
		rowAddr.setField(0, 42);
		rowAddr.setField(1, "Main Street 42");
		rowAddr.setField(2, "Test City");
		rowAddr.setField(3, "Test State");
		rowAddr.setField(4, "12345");

		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> t = new Tuple3<>();
		t.f0 = Address.class;
		t.f1 = addr;
		t.f2 = rowAddr;

		return t;
	}

	/**
	 * Tests all Avro data types as well as nested types.
	 */
	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getComplexTestData() {
		final Address addr = Address.newBuilder()
			.setNum(42)
			.setStreet("Main Street 42")
			.setCity("Test City")
			.setState("Test State")
			.setZip("12345")
			.build();

		final Row rowAddr = new Row(5);
		rowAddr.setField(0, 42);
		rowAddr.setField(1, "Main Street 42");
		rowAddr.setField(2, "Test City");
		rowAddr.setField(3, "Test State");
		rowAddr.setField(4, "12345");

		final User user = User.newBuilder()
			.setName("Charlie")
			.setFavoriteNumber(null)
			.setFavoriteColor("blue")
			.setTypeLongTest(1337L)
			.setTypeDoubleTest(1.337d)
			.setTypeNullTest(null)
			.setTypeBoolTest(false)
			.setTypeArrayString(new ArrayList<CharSequence>())
			.setTypeArrayBoolean(new ArrayList<Boolean>())
			.setTypeNullableArray(null)
			.setTypeEnum(Colors.RED)
			.setTypeMap(new HashMap<CharSequence, Long>())
			.setTypeFixed(null)
			.setTypeUnion(null)
			.setTypeNested(addr)
			.build();

		final Row rowUser = new Row(15);
		rowUser.setField(0, "Charlie");
		rowUser.setField(1, null);
		rowUser.setField(2, "blue");
		rowUser.setField(3, 1337L);
		rowUser.setField(4, 1.337d);
		rowUser.setField(5, null);
		rowUser.setField(6, false);
		rowUser.setField(7, new ArrayList<CharSequence>());
		rowUser.setField(8, new ArrayList<Boolean>());
		rowUser.setField(9, null);
		rowUser.setField(10, Colors.RED);
		rowUser.setField(11, new HashMap<CharSequence, Long>());
		rowUser.setField(12, null);
		rowUser.setField(13, null);
		rowUser.setField(14, rowAddr);

		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> t = new Tuple3<>();
		t.f0 = User.class;
		t.f1 = user;
		t.f2 = rowUser;

		return t;
	}
}
