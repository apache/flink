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
import org.apache.flink.table.types.DataType;
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

/** Tests for {@link AvroSchemaConverter}. */
public class AvroSchemaConverterTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

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
    public void testAddingOptionalField() throws IOException {
        Schema oldSchema =
                SchemaBuilder.record("record")
                        .fields()
                        .requiredLong("category_id")
                        .optionalString("name")
                        .endRecord();

        Schema newSchema =
                AvroSchemaConverter.convertToSchema(
                        TableSchema.builder()
                                .field("category_id", DataTypes.BIGINT().notNull())
                                .field("name", DataTypes.STRING().nullable())
                                .field("description", DataTypes.STRING().nullable())
                                .build()
                                .toRowDataType()
                                .getLogicalType());

        byte[] serializedRecord =
                AvroTestUtils.writeRecord(
                        new GenericRecordBuilder(oldSchema)
                                .set("category_id", 1L)
                                .set("name", "test")
                                .build(),
                        oldSchema);
        GenericDatumReader<GenericRecord> datumReader =
                new GenericDatumReader<>(oldSchema, newSchema);
        GenericRecord newRecord =
                datumReader.read(
                        null,
                        DecoderFactory.get()
                                .binaryDecoder(serializedRecord, 0, serializedRecord.length, null));
        assertThat(
                newRecord,
                equalTo(
                        new GenericRecordBuilder(newSchema)
                                .set("category_id", 1L)
                                .set("name", "test")
                                .set("description", null)
                                .build()));
    }

    @Test
    public void testInvalidRawTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType)
                        TableSchema.builder()
                                .field("a", DataTypes.STRING())
                                .field(
                                        "b",
                                        DataTypes.RAW(Types.GENERIC(AvroSchemaConverterTest.class)))
                                .build()
                                .toRowDataType()
                                .getLogicalType();
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Unsupported to derive Schema for type: RAW");
        AvroSchemaConverter.convertToSchema(rowType);
    }

    @Test
    public void testInvalidTimestampTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType)
                        TableSchema.builder()
                                .field("a", DataTypes.STRING())
                                .field("b", DataTypes.TIMESTAMP(9))
                                .build()
                                .toRowDataType()
                                .getLogicalType();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "Avro does not support TIMESTAMP type with precision: 9, "
                        + "it only supports precision less than 3.");
        AvroSchemaConverter.convertToSchema(rowType);
    }

    @Test
    public void testInvalidTimeTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType)
                        TableSchema.builder()
                                .field("a", DataTypes.STRING())
                                .field("b", DataTypes.TIME(6))
                                .build()
                                .toRowDataType()
                                .getLogicalType();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "Avro does not support TIME type with precision: 6, it only supports precision less than 3.");
        AvroSchemaConverter.convertToSchema(rowType);
    }

    @Test
    public void testRowTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType)
                        TableSchema.builder()
                                .field(
                                        "row1",
                                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING())))
                                .field(
                                        "row2",
                                        DataTypes.ROW(DataTypes.FIELD("b", DataTypes.STRING())))
                                .field(
                                        "row3",
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "row3",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "c", DataTypes.STRING())))))
                                .build()
                                .toRowDataType()
                                .getLogicalType();
        Schema schema = AvroSchemaConverter.convertToSchema(rowType);
        assertEquals(
                "{\n"
                        + "  \"type\" : \"record\",\n"
                        + "  \"name\" : \"record\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"name\" : \"row1\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"record\",\n"
                        + "      \"name\" : \"record_row1\",\n"
                        + "      \"fields\" : [ {\n"
                        + "        \"name\" : \"a\",\n"
                        + "        \"type\" : [ \"null\", \"string\" ],\n"
                        + "        \"default\" : null\n"
                        + "      } ]\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"row2\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"record\",\n"
                        + "      \"name\" : \"record_row2\",\n"
                        + "      \"fields\" : [ {\n"
                        + "        \"name\" : \"b\",\n"
                        + "        \"type\" : [ \"null\", \"string\" ],\n"
                        + "        \"default\" : null\n"
                        + "      } ]\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"row3\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"record\",\n"
                        + "      \"name\" : \"record_row3\",\n"
                        + "      \"fields\" : [ {\n"
                        + "        \"name\" : \"row3\",\n"
                        + "        \"type\" : [ \"null\", {\n"
                        + "          \"type\" : \"record\",\n"
                        + "          \"name\" : \"record_row3_row3\",\n"
                        + "          \"fields\" : [ {\n"
                        + "            \"name\" : \"c\",\n"
                        + "            \"type\" : [ \"null\", \"string\" ],\n"
                        + "            \"default\" : null\n"
                        + "          } ]\n"
                        + "        } ],\n"
                        + "        \"default\" : null\n"
                        + "      } ]\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  } ]\n"
                        + "}",
                schema.toString(true));
    }

    /** Test convert nullable data type to Avro schema then converts back. */
    @Test
    public void testDataTypeToSchemaToDataTypeNullable() {
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("f_null", DataTypes.NULL()),
                        DataTypes.FIELD("f_boolean", DataTypes.BOOLEAN()),
                        // tinyint and smallint all convert to int
                        DataTypes.FIELD("f_int", DataTypes.INT()),
                        DataTypes.FIELD("f_bigint", DataTypes.BIGINT()),
                        DataTypes.FIELD("f_float", DataTypes.FLOAT()),
                        DataTypes.FIELD("f_double", DataTypes.DOUBLE()),
                        // char converts to string
                        DataTypes.FIELD("f_string", DataTypes.STRING()),
                        // binary converts to bytes
                        DataTypes.FIELD("f_varbinary", DataTypes.BYTES()),
                        DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD("f_date", DataTypes.DATE()),
                        DataTypes.FIELD("f_time", DataTypes.TIME(3)),
                        DataTypes.FIELD("f_decimal", DataTypes.DECIMAL(10, 0)),
                        DataTypes.FIELD(
                                "f_row",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f0", DataTypes.INT()),
                                        DataTypes.FIELD("f1", DataTypes.TIMESTAMP(3)))),
                        // multiset converts to map
                        // map key is always not null
                        DataTypes.FIELD(
                                "f_map",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT())),
                        DataTypes.FIELD("f_array", DataTypes.ARRAY(DataTypes.INT())));
        Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        DataType converted = AvroSchemaConverter.convertToDataType(schema.toString());
        assertEquals(dataType, converted);
    }

    /** Test convert non-nullable data type to Avro schema then converts back. */
    @Test
    public void testDataTypeToSchemaToDataTypeNonNullable() {
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD("f_boolean", DataTypes.BOOLEAN().notNull()),
                                // tinyint and smallint all convert to int
                                DataTypes.FIELD("f_int", DataTypes.INT().notNull()),
                                DataTypes.FIELD("f_bigint", DataTypes.BIGINT().notNull()),
                                DataTypes.FIELD("f_float", DataTypes.FLOAT().notNull()),
                                DataTypes.FIELD("f_double", DataTypes.DOUBLE().notNull()),
                                // char converts to string
                                DataTypes.FIELD("f_string", DataTypes.STRING().notNull()),
                                // binary converts to bytes
                                DataTypes.FIELD("f_varbinary", DataTypes.BYTES().notNull()),
                                DataTypes.FIELD("f_timestamp", DataTypes.TIMESTAMP(3).notNull()),
                                DataTypes.FIELD("f_date", DataTypes.DATE().notNull()),
                                DataTypes.FIELD("f_time", DataTypes.TIME(3).notNull()),
                                DataTypes.FIELD("f_decimal", DataTypes.DECIMAL(10, 0).notNull()),
                                DataTypes.FIELD(
                                        "f_row",
                                        DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "f0", DataTypes.INT().notNull()),
                                                        DataTypes.FIELD(
                                                                "f1",
                                                                DataTypes.TIMESTAMP(3).notNull()))
                                                .notNull()),
                                // multiset converts to map
                                // map key is always not null
                                DataTypes.FIELD(
                                        "f_map",
                                        DataTypes.MAP(
                                                        DataTypes.STRING().notNull(),
                                                        DataTypes.INT().notNull())
                                                .notNull()),
                                DataTypes.FIELD(
                                        "f_array",
                                        DataTypes.ARRAY(DataTypes.INT().notNull()).notNull()))
                        .notNull();
        Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        DataType converted = AvroSchemaConverter.convertToDataType(schema.toString());
        assertEquals(dataType, converted);
    }

    /** Test convert nullable Avro schema to data type then converts back. */
    @Test
    public void testSchemaToDataTypeToSchemaNullable() {
        String schemaStr =
                "{\n"
                        + "  \"type\" : \"record\",\n"
                        + "  \"name\" : \"record\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"name\" : \"f_null\",\n"
                        + "    \"type\" : \"null\",\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_boolean\",\n"
                        + "    \"type\" : [ \"null\", \"boolean\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_int\",\n"
                        + "    \"type\" : [ \"null\", \"int\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_bigint\",\n"
                        + "    \"type\" : [ \"null\", \"long\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_float\",\n"
                        + "    \"type\" : [ \"null\", \"float\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_double\",\n"
                        + "    \"type\" : [ \"null\", \"double\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_string\",\n"
                        + "    \"type\" : [ \"null\", \"string\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_varbinary\",\n"
                        + "    \"type\" : [ \"null\", \"bytes\" ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_timestamp\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"long\",\n"
                        + "      \"logicalType\" : \"timestamp-millis\"\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_date\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"int\",\n"
                        + "      \"logicalType\" : \"date\"\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_time\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"int\",\n"
                        + "      \"logicalType\" : \"time-millis\"\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_decimal\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"bytes\",\n"
                        + "      \"logicalType\" : \"decimal\",\n"
                        + "      \"precision\" : 10,\n"
                        + "      \"scale\" : 0\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_row\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"record\",\n"
                        + "      \"name\" : \"record_f_row\",\n"
                        + "      \"fields\" : [ {\n"
                        + "        \"name\" : \"f0\",\n"
                        + "        \"type\" : [ \"null\", \"int\" ],\n"
                        + "        \"default\" : null\n"
                        + "      }, {\n"
                        + "        \"name\" : \"f1\",\n"
                        + "        \"type\" : [ \"null\", {\n"
                        + "          \"type\" : \"long\",\n"
                        + "          \"logicalType\" : \"timestamp-millis\"\n"
                        + "        } ],\n"
                        + "        \"default\" : null\n"
                        + "      } ]\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_map\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"map\",\n"
                        + "      \"values\" : [ \"null\", \"int\" ]\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_array\",\n"
                        + "    \"type\" : [ \"null\", {\n"
                        + "      \"type\" : \"array\",\n"
                        + "      \"items\" : [ \"null\", \"int\" ]\n"
                        + "    } ],\n"
                        + "    \"default\" : null\n"
                        + "  } ]\n"
                        + "}";
        DataType dataType = AvroSchemaConverter.convertToDataType(schemaStr);
        Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        assertEquals(new Schema.Parser().parse(schemaStr), schema);
    }

    /** Test convert non-nullable Avro schema to data type then converts back. */
    @Test
    public void testSchemaToDataTypeToSchemaNonNullable() {
        String schemaStr =
                "{\n"
                        + "  \"type\" : \"record\",\n"
                        + "  \"name\" : \"record\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"name\" : \"f_boolean\",\n"
                        + "    \"type\" : \"boolean\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_int\",\n"
                        + "    \"type\" : \"int\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_bigint\",\n"
                        + "    \"type\" : \"long\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_float\",\n"
                        + "    \"type\" : \"float\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_double\",\n"
                        + "    \"type\" : \"double\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_string\",\n"
                        + "    \"type\" : \"string\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_varbinary\",\n"
                        + "    \"type\" : \"bytes\"\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_timestamp\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"long\",\n"
                        + "      \"logicalType\" : \"timestamp-millis\"\n"
                        + "    }\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_date\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"int\",\n"
                        + "      \"logicalType\" : \"date\"\n"
                        + "    }\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_time\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"int\",\n"
                        + "      \"logicalType\" : \"time-millis\"\n"
                        + "    }\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_decimal\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"bytes\",\n"
                        + "      \"logicalType\" : \"decimal\",\n"
                        + "      \"precision\" : 10,\n"
                        + "      \"scale\" : 0\n"
                        + "    }\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_row\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"record\",\n"
                        + "      \"name\" : \"record_f_row\",\n"
                        + "      \"fields\" : [ {\n"
                        + "        \"name\" : \"f0\",\n"
                        + "        \"type\" : \"int\"\n"
                        + "      }, {\n"
                        + "        \"name\" : \"f1\",\n"
                        + "        \"type\" : {\n"
                        + "          \"type\" : \"long\",\n"
                        + "          \"logicalType\" : \"timestamp-millis\"\n"
                        + "        }\n"
                        + "      } ]\n"
                        + "    }\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_map\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"map\",\n"
                        + "      \"values\" : \"int\"\n"
                        + "    }\n"
                        + "  }, {\n"
                        + "    \"name\" : \"f_array\",\n"
                        + "    \"type\" : {\n"
                        + "      \"type\" : \"array\",\n"
                        + "      \"items\" : \"int\"\n"
                        + "    }\n"
                        + "  } ]\n"
                        + "}";
        DataType dataType = AvroSchemaConverter.convertToDataType(schemaStr);
        Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        assertEquals(new Schema.Parser().parse(schemaStr), schema);
    }

    private void validateUserSchema(TypeInformation<?> actual) {
        final TypeInformation<Row> address =
                Types.ROW_NAMED(
                        new String[] {"num", "street", "city", "state", "zip"},
                        Types.INT,
                        Types.STRING,
                        Types.STRING,
                        Types.STRING,
                        Types.STRING);

        final TypeInformation<Row> user =
                Types.ROW_NAMED(
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
                            "type_decimal_fixed"
                        },
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
        final DataType address =
                DataTypes.ROW(
                        DataTypes.FIELD("num", DataTypes.INT().notNull()),
                        DataTypes.FIELD("street", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("city", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("state", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("zip", DataTypes.STRING().notNull()));

        final DataType user =
                DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING().notNull()),
                                DataTypes.FIELD("favorite_number", DataTypes.INT()),
                                DataTypes.FIELD("favorite_color", DataTypes.STRING()),
                                DataTypes.FIELD("type_long_test", DataTypes.BIGINT()),
                                DataTypes.FIELD("type_double_test", DataTypes.DOUBLE().notNull()),
                                DataTypes.FIELD("type_null_test", DataTypes.NULL()),
                                DataTypes.FIELD("type_bool_test", DataTypes.BOOLEAN().notNull()),
                                DataTypes.FIELD(
                                        "type_array_string",
                                        DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
                                DataTypes.FIELD(
                                        "type_array_boolean",
                                        DataTypes.ARRAY(DataTypes.BOOLEAN().notNull()).notNull()),
                                DataTypes.FIELD(
                                        "type_nullable_array",
                                        DataTypes.ARRAY(DataTypes.STRING().notNull())),
                                DataTypes.FIELD("type_enum", DataTypes.STRING().notNull()),
                                DataTypes.FIELD(
                                        "type_map",
                                        DataTypes.MAP(
                                                        DataTypes.STRING().notNull(),
                                                        DataTypes.BIGINT().notNull())
                                                .notNull()),
                                DataTypes.FIELD("type_fixed", DataTypes.VARBINARY(16)),
                                DataTypes.FIELD(
                                        "type_union",
                                        DataTypes.RAW(Types.GENERIC(Object.class)).notNull()),
                                DataTypes.FIELD("type_nested", address),
                                DataTypes.FIELD("type_bytes", DataTypes.BYTES().notNull()),
                                DataTypes.FIELD("type_date", DataTypes.DATE().notNull()),
                                DataTypes.FIELD("type_time_millis", DataTypes.TIME(3).notNull()),
                                DataTypes.FIELD("type_time_micros", DataTypes.TIME(6).notNull()),
                                DataTypes.FIELD(
                                        "type_timestamp_millis", DataTypes.TIMESTAMP(3).notNull()),
                                DataTypes.FIELD(
                                        "type_timestamp_micros", DataTypes.TIMESTAMP(6).notNull()),
                                DataTypes.FIELD(
                                        "type_decimal_bytes", DataTypes.DECIMAL(4, 2).notNull()),
                                DataTypes.FIELD(
                                        "type_decimal_fixed", DataTypes.DECIMAL(4, 2).notNull()))
                        .notNull();

        assertEquals(user, actual);
    }
}
