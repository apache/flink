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
import org.apache.flink.types.Row;

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
