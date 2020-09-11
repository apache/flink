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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AvroSchemaConverter}.
 */
public class AvroSchemaConverterTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

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
	public void testConvertAvroSchemaToDataType() {
		final String schema = User.getClassSchema().toString(true);
		validateUserSchema(AvroSchemaConverter.convertToDataType(schema));
	}

	@Test
	public void testInvalidRawTypeAvroSchemaConversion() {
		RowType rowType = (RowType) TableSchema.builder()
			.field("a", DataTypes.STRING())
			.field("b", DataTypes.RAW(Types.GENERIC(AvroSchemaConverterTest.class)))
			.build().toRowDataType().getLogicalType();
		thrown.expect(UnsupportedOperationException.class);
		thrown.expectMessage("Unsupported to derive Schema for type: RAW");
		AvroSchemaConverter.convertToSchema(rowType);
	}

	@Test
	public void testInvalidTimestampTypeAvroSchemaConversion() {
		RowType rowType = (RowType) TableSchema.builder()
			.field("a", DataTypes.STRING())
			.field("b", DataTypes.TIMESTAMP(9))
			.build().toRowDataType().getLogicalType();
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Avro does not support TIMESTAMP type with precision: 9, " +
			"it only supports precision less than 3.");
		AvroSchemaConverter.convertToSchema(rowType);
	}

	@Test
	public void testInvalidTimeTypeAvroSchemaConversion() {
		RowType rowType = (RowType) TableSchema.builder()
			.field("a", DataTypes.STRING())
			.field("b", DataTypes.TIME(6))
			.build().toRowDataType().getLogicalType();
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Avro does not support TIME type with precision: 6, it only supports precision less than 3.");
		AvroSchemaConverter.convertToSchema(rowType);
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
			Types.SQL_TIME,
			Types.SQL_TIMESTAMP,
			Types.SQL_TIMESTAMP,
			Types.BIG_DEC,
			Types.BIG_DEC);

		assertEquals(user, actual);

		final RowTypeInfo userRowInfo = (RowTypeInfo) user;
		assertTrue(userRowInfo.schemaEquals(actual));
	}

	private void validateUserSchema(DataType actual) {
		final DataType address = DataTypes.ROW(
				DataTypes.FIELD("num", DataTypes.INT().notNull()),
				DataTypes.FIELD("street", DataTypes.STRING().notNull()),
				DataTypes.FIELD("city", DataTypes.STRING().notNull()),
				DataTypes.FIELD("state", DataTypes.STRING().notNull()),
				DataTypes.FIELD("zip", DataTypes.STRING().notNull()));

		final DataType user = DataTypes.ROW(
				DataTypes.FIELD("name", DataTypes.STRING().notNull()),
				DataTypes.FIELD("favorite_number", DataTypes.INT()),
				DataTypes.FIELD("favorite_color", DataTypes.STRING()),
				DataTypes.FIELD("type_long_test", DataTypes.BIGINT()),
				DataTypes.FIELD("type_double_test", DataTypes.DOUBLE().notNull()),
				DataTypes.FIELD("type_null_test", DataTypes.NULL()),
				DataTypes.FIELD("type_bool_test", DataTypes.BOOLEAN().notNull()),
				DataTypes.FIELD("type_array_string",
						DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
				DataTypes.FIELD("type_array_boolean",
						DataTypes.ARRAY(DataTypes.BOOLEAN().notNull()).notNull()),
				DataTypes.FIELD("type_nullable_array", DataTypes.ARRAY(DataTypes.STRING().notNull())),
				DataTypes.FIELD("type_enum", DataTypes.STRING().notNull()),
				DataTypes.FIELD("type_map",
						DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT().notNull()).notNull()),
				DataTypes.FIELD("type_fixed", DataTypes.VARBINARY(16)),
				DataTypes.FIELD("type_union", DataTypes.RAW(Types.GENERIC(Object.class)).notNull()),
				DataTypes.FIELD("type_nested", address),
				DataTypes.FIELD("type_bytes", DataTypes.BYTES().notNull()),
				DataTypes.FIELD("type_date", DataTypes.DATE().notNull()),
				DataTypes.FIELD("type_time_millis", DataTypes.TIME().notNull()),
				DataTypes.FIELD("type_time_micros", DataTypes.TIME(6).notNull()),
				DataTypes.FIELD("type_timestamp_millis",
						DataTypes.TIMESTAMP(3).notNull()),
				DataTypes.FIELD("type_timestamp_micros",
						DataTypes.TIMESTAMP(6).notNull()),
				DataTypes.FIELD("type_decimal_bytes", DataTypes.DECIMAL(4, 2).notNull()),
				DataTypes.FIELD("type_decimal_fixed", DataTypes.DECIMAL(4, 2).notNull()))
				.notNull();

		assertEquals(user, actual);
	}
}
