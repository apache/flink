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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
	public void testAddingOptionalField() throws IOException {
		Schema oldSchema = SchemaBuilder.record("record")
			.fields()
			.requiredLong("category_id")
			.optionalString("name")
			.endRecord();

		Schema newSchema = AvroSchemaConverter.convertToSchema(
			DataTypes.ROW(
				DataTypes.FIELD("category_id", DataTypes.BIGINT().notNull()),
				DataTypes.FIELD("name", DataTypes.STRING().nullable()),
				DataTypes.FIELD("description", DataTypes.STRING().nullable())
			).getLogicalType()
		);

		byte[] serializedRecord = AvroTestUtils.writeRecord(
			new GenericRecordBuilder(oldSchema)
				.set("category_id", 1L)
				.set("name", "test")
				.build(),
			oldSchema
		);
		GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(
			oldSchema,
			newSchema);
		GenericRecord newRecord = datumReader.read(
			null,
			DecoderFactory.get().binaryDecoder(serializedRecord, 0, serializedRecord.length, null));
		assertThat(
			newRecord,
			equalTo(new GenericRecordBuilder(newSchema)
				.set("category_id", 1L)
				.set("name", "test")
				.set("description", null)
				.build()));
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

	@Test
	public void testRowTypeAvroSchemaConversion() {
		RowType rowType = (RowType) TableSchema.builder()
			.field("row1", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING())))
			.field("row2", DataTypes.ROW(DataTypes.FIELD("b", DataTypes.STRING())))
			.field("row3", DataTypes.ROW(
				DataTypes.FIELD("row3", DataTypes.ROW(DataTypes.FIELD("c", DataTypes.STRING())))))
			.build().toRowDataType().getLogicalType();
		Schema schema = AvroSchemaConverter.convertToSchema(rowType);
		assertEquals("{\n"
			+ "  \"type\" : \"record\",\n"
			+ "  \"name\" : \"record\",\n"
			+ "  \"fields\" : [ {\n"
			+ "    \"name\" : \"row1\",\n"
			+ "    \"type\" : {\n"
			+ "      \"type\" : \"record\",\n"
			+ "      \"name\" : \"record_row1\",\n"
			+ "      \"fields\" : [ {\n"
			+ "        \"name\" : \"a\",\n"
			+ "        \"type\" : [ \"null\", \"string\" ],\n"
			+ "        \"default\" : null\n"
			+ "      } ]\n"
			+ "    },\n"
			+ "    \"default\" : null\n"
			+ "  }, {\n"
			+ "    \"name\" : \"row2\",\n"
			+ "    \"type\" : {\n"
			+ "      \"type\" : \"record\",\n"
			+ "      \"name\" : \"record_row2\",\n"
			+ "      \"fields\" : [ {\n"
			+ "        \"name\" : \"b\",\n"
			+ "        \"type\" : [ \"null\", \"string\" ],\n"
			+ "        \"default\" : null\n"
			+ "      } ]\n"
			+ "    },\n"
			+ "    \"default\" : null\n"
			+ "  }, {\n"
			+ "    \"name\" : \"row3\",\n"
			+ "    \"type\" : {\n"
			+ "      \"type\" : \"record\",\n"
			+ "      \"name\" : \"record_row3\",\n"
			+ "      \"fields\" : [ {\n"
			+ "        \"name\" : \"row3\",\n"
			+ "        \"type\" : {\n"
			+ "          \"type\" : \"record\",\n"
			+ "          \"name\" : \"record_row3_row3\",\n"
			+ "          \"fields\" : [ {\n"
			+ "            \"name\" : \"c\",\n"
			+ "            \"type\" : [ \"null\", \"string\" ],\n"
			+ "            \"default\" : null\n"
			+ "          } ]\n"
			+ "        },\n"
			+ "        \"default\" : null\n"
			+ "      } ]\n"
			+ "    },\n"
			+ "    \"default\" : null\n"
			+ "  } ]\n"
			+ "}", schema.toString(true));
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
