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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AvroSchemaConverter}.
 */
public class AvroSchemaConverterTest {

	@Test
	public void testAvroClassConversion() {
		validateUserSchema(AvroSchemaConverter.convertToTypeInfo(User.class));
	}

	@Test
	public void testAvroSchemaConversion() {
		final String schema = User.getClassSchema().toString(true);
		validateUserSchema(AvroSchemaConverter.convertToTypeInfo(schema));
	}

	@Test
	public void testLogicalTypeConversion() {
		final RowType userType = AvroTestUtils.getUserLogicalType();

		String schemaString =
			"{\"type\":\"record\",\"name\":\"row_0\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]}" +
			",{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":" +
			"[\"string\",\"null\"]},{\"name\":\"type_long_test\",\"type\":[\"long\",\"null\"]},{\"name\":\"type_double_test\"," +
			"\"type\":[\"double\",\"null\"]},{\"name\":\"type_null_test\",\"type\":\"null\"}," +
			"{\"name\":\"type_bool_test\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"type_array_string\",\"type\":[" +
			"{\"type\":\"array\",\"items\":[\"string\",\"null\"]},\"null\"]},{\"name\":\"type_array_boolean\",\"type\":[" +
			"{\"type\":\"array\",\"items\":[\"boolean\",\"null\"]},\"null\"]},{\"name\":\"type_nullable_array\",\"type\":[" +
			"{\"type\":\"array\",\"items\":[\"string\",\"null\"]},\"null\"]},{\"name\":\"type_enum\",\"type\":[\"string\"," +
			"\"null\"]},{\"name\":\"type_map\",\"type\":[{\"type\":\"map\",\"values\":[\"long\",\"null\"]},\"null\"]}," +
			"{\"name\":\"type_fixed\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"type_union\",\"type\":[\"null\"," +
			"\"boolean\",\"long\",\"double\"]},{\"name\":\"type_nested\",\"type\":{\"type\":\"record\",\"name\":\"row_1\"," +
			"\"fields\":[{\"name\":\"num\",\"type\":[\"int\",\"null\"]},{\"name\":\"street\",\"type\":[\"string\",\"null\"]}," +
			"{\"name\":\"city\",\"type\":[\"string\",\"null\"]},{\"name\":\"state\",\"type\":[\"string\",\"null\"]}," +
			"{\"name\":\"zip\",\"type\":[\"string\",\"null\"]}]}},{\"name\":\"type_bytes\",\"type\":[\"bytes\",\"null\"]}," +
			"{\"name\":\"type_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"type_time_millis\"," +
			"\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}},{\"name\":\"type_time_micros\",\"type\":[\"int\"," +
			"\"null\"]},{\"name\":\"type_timestamp_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
			"{\"name\":\"type_timestamp_micros\",\"type\":[\"long\",\"null\"]},{\"name\":\"type_decimal_bytes\",\"type\":" +
			"{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}},{\"name\":\"type_decimal_fixed\"," +
			"\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}\n";
		final Schema expected = new Schema.Parser().parse(schemaString);

		assertEquals(expected, AvroSchemaConverter.convertToSchema(userType));
	}

	private void validateUserSchema(TypeInformation<?> actual) {
		final TypeInformation<Row> address = Types.ROW_NAMED(
			new String[]{
				"num",
				"street",
				"city",
				"state",
				"zip"},
			Types.INT,
			Types.STRING,
			Types.STRING,
			Types.STRING,
			Types.STRING);

		final TypeInformation<Row> user = Types.ROW_NAMED(
			new String[] {
				"name",
				"favorite_number",
				"favorite_color",
				"type_long_test",
				"type_double_test",
				"type_null_test",
				"type_bool_test",
				"type_array_string",
				"type_array_boolean",
				"type_nullable_array",
				"type_enum",
				"type_map",
				"type_fixed",
				"type_union",
				"type_nested",
				"type_bytes",
				"type_date",
				"type_time_millis",
				"type_time_micros",
				"type_timestamp_millis",
				"type_timestamp_micros",
				"type_decimal_bytes",
				"type_decimal_fixed"},
			Types.STRING,
			Types.INT,
			Types.STRING,
			Types.LONG,
			Types.DOUBLE,
			Types.VOID,
			Types.BOOLEAN,
			Types.OBJECT_ARRAY(Types.STRING),
			Types.OBJECT_ARRAY(Types.BOOLEAN),
			Types.OBJECT_ARRAY(Types.STRING),
			Types.STRING,
			Types.MAP(Types.STRING, Types.LONG),
			Types.PRIMITIVE_ARRAY(Types.BYTE),
			Types.GENERIC(Object.class),
			address,
			Types.PRIMITIVE_ARRAY(Types.BYTE),
			Types.SQL_DATE,
			Types.SQL_TIME,
			Types.INT,
			Types.SQL_TIMESTAMP,
			Types.LONG,
			Types.BIG_DEC,
			Types.BIG_DEC);

		assertEquals(user, actual);

		final RowTypeInfo userRowInfo = (RowTypeInfo) user;
		assertTrue(userRowInfo.schemaEquals(actual));
	}
}
