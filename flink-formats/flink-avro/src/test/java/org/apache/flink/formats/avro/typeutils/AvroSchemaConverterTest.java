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
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.NULL;
import static org.apache.flink.table.api.DataTypes.RAW;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroSchemaConverter}. */
class AvroSchemaConverterTest {

    @Test
    void testAvroClassConversion() {
        validateUserSchema(AvroSchemaConverter.convertToTypeInfo(User.class));
    }

    @Test
    void testAvroSchemaConversion() {
        final String schema = User.getClassSchema().toString(true);
        validateUserSchema(AvroSchemaConverter.convertToTypeInfo(schema));
    }

    @Test
    void testConvertAvroSchemaToDataType() {
        final String schema = User.getClassSchema().toString(true);
        validateUserSchema(AvroSchemaConverter.convertToDataType(schema));
    }

    @Test
    void testAddingOptionalField() throws IOException {
        Schema oldSchema =
                SchemaBuilder.record("record")
                        .fields()
                        .requiredLong("category_id")
                        .optionalString("name")
                        .endRecord();

        Schema newSchema =
                AvroSchemaConverter.convertToSchema(
                        ROW(
                                        FIELD("category_id", BIGINT().notNull()),
                                        FIELD("name", STRING().nullable()),
                                        FIELD("description", STRING().nullable()))
                                .notNull()
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
        assertThat(newRecord)
                .isEqualTo(
                        new GenericRecordBuilder(newSchema)
                                .set("category_id", 1L)
                                .set("name", "test")
                                .set("description", null)
                                .build());
    }

    @Test
    void testInvalidRawTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType)
                        ROW(
                                        FIELD("a", STRING()),
                                        FIELD("b", RAW(Void.class, VoidSerializer.INSTANCE)))
                                .getLogicalType();

        assertThatThrownBy(() -> AvroSchemaConverter.convertToSchema(rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageStartingWith("Unsupported to derive Schema for type: RAW");
    }

    @Test
    void testInvalidTimestampTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType) ROW(FIELD("a", STRING()), FIELD("b", TIMESTAMP(8))).getLogicalType();

        assertThatThrownBy(() -> AvroSchemaConverter.convertToSchema(rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Avro does not support TIMESTAMP type with precision: 8, "
                                + "it only supports precision less than or equal to 6.");
    }

    @Test
    void testInvalidTimeTypeAvroSchemaConversion() {
        RowType rowType = (RowType) ROW(FIELD("a", STRING()), FIELD("b", TIME(6))).getLogicalType();

        assertThatThrownBy(() -> AvroSchemaConverter.convertToSchema(rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Avro does not support TIME type with precision: 6, it only supports precision less than 3.");
    }

    @Test
    void testRowTypeAvroSchemaConversion() {
        RowType rowType =
                (RowType)
                        ROW(
                                        FIELD("row1", ROW(FIELD("a", STRING()))),
                                        FIELD("row2", ROW(FIELD("b", STRING()))),
                                        FIELD(
                                                "row3",
                                                ROW(FIELD("row3", ROW(FIELD("c", STRING()))))))
                                .notNull()
                                .getLogicalType();
        Schema schema = AvroSchemaConverter.convertToSchema(rowType);
        assertThat(schema.toString(true))
                .isEqualTo(
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
                                + "}");
    }

    /** Test convert nullable data type to Avro schema then converts back. */
    @Test
    void testDataTypeToSchemaToDataTypeNullable() {
        DataType dataType =
                ROW(
                        FIELD("f_null", NULL()),
                        FIELD("f_boolean", BOOLEAN()),
                        // tinyint and smallint all convert to int
                        FIELD("f_int", INT()),
                        FIELD("f_bigint", BIGINT()),
                        FIELD("f_float", FLOAT()),
                        FIELD("f_double", DOUBLE()),
                        // char converts to string
                        FIELD("f_string", STRING()),
                        // binary converts to bytes
                        FIELD("f_varbinary", BYTES()),
                        FIELD("f_timestamp", TIMESTAMP(3)),
                        FIELD("f_date", DATE()),
                        FIELD("f_time", TIME(3)),
                        FIELD("f_decimal", DECIMAL(10, 0)),
                        FIELD("f_row", ROW(FIELD("f0", INT()), FIELD("f1", TIMESTAMP(3)))),
                        // multiset converts to map
                        // map key is always not null
                        FIELD("f_map", MAP(STRING().notNull(), INT())),
                        FIELD("f_array", ARRAY(INT())));
        Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        DataType converted = AvroSchemaConverter.convertToDataType(schema.toString());
        assertThat(converted).isEqualTo(dataType);
    }

    /** Test convert non-nullable data type to Avro schema then converts back. */
    @Test
    void testDataTypeToSchemaToDataTypeNonNullable() {
        DataType dataType =
                ROW(
                                FIELD("f_boolean", BOOLEAN().notNull()),
                                // tinyint and smallint all convert to int
                                FIELD("f_int", INT().notNull()),
                                FIELD("f_bigint", BIGINT().notNull()),
                                FIELD("f_float", FLOAT().notNull()),
                                FIELD("f_double", DOUBLE().notNull()),
                                // char converts to string
                                FIELD("f_string", STRING().notNull()),
                                // binary converts to bytes
                                FIELD("f_varbinary", BYTES().notNull()),
                                FIELD("f_timestamp", TIMESTAMP(3).notNull()),
                                FIELD("f_date", DATE().notNull()),
                                FIELD("f_time", TIME(3).notNull()),
                                FIELD("f_decimal", DECIMAL(10, 0).notNull()),
                                FIELD(
                                        "f_row",
                                        ROW(
                                                        FIELD("f0", INT().notNull()),
                                                        FIELD("f1", TIMESTAMP(3).notNull()))
                                                .notNull()),
                                // multiset converts to map
                                // map key is always not null
                                FIELD("f_map", MAP(STRING().notNull(), INT().notNull()).notNull()),
                                FIELD("f_array", ARRAY(INT().notNull()).notNull()))
                        .notNull();
        Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        DataType converted = AvroSchemaConverter.convertToDataType(schema.toString());
        assertThat(converted).isEqualTo(dataType);
    }

    /** Test convert nullable Avro schema to data type then converts back. */
    @Test
    void testSchemaToDataTypeToSchemaNullable() {
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
        assertThat(schema).isEqualTo(new Schema.Parser().parse(schemaStr));
    }

    /** Test convert non-nullable Avro schema to data type then converts back. */
    @Test
    void testSchemaToDataTypeToSchemaNonNullable() {
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
        assertThat(schema).isEqualTo(new Schema.Parser().parse(schemaStr));
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

        assertThat(actual).isEqualTo(user);

        final RowTypeInfo userRowInfo = (RowTypeInfo) user;
        assertThat(userRowInfo.schemaEquals(actual)).isTrue();
    }

    private void validateUserSchema(DataType actual) {
        final DataType address =
                ROW(
                        FIELD("num", INT().notNull()),
                        FIELD("street", STRING().notNull()),
                        FIELD("city", STRING().notNull()),
                        FIELD("state", STRING().notNull()),
                        FIELD("zip", STRING().notNull()));

        final DataType user =
                ROW(
                                FIELD("name", STRING().notNull()),
                                FIELD("favorite_number", INT()),
                                FIELD("favorite_color", STRING()),
                                FIELD("type_long_test", BIGINT()),
                                FIELD("type_double_test", DOUBLE().notNull()),
                                FIELD("type_null_test", NULL()),
                                FIELD("type_bool_test", BOOLEAN().notNull()),
                                FIELD("type_array_string", ARRAY(STRING().notNull()).notNull()),
                                FIELD("type_array_boolean", ARRAY(BOOLEAN().notNull()).notNull()),
                                FIELD("type_nullable_array", ARRAY(STRING().notNull())),
                                FIELD("type_enum", STRING().notNull()),
                                FIELD(
                                        "type_map",
                                        MAP(STRING().notNull(), BIGINT().notNull()).notNull()),
                                FIELD("type_fixed", VARBINARY(16)),
                                FIELD(
                                        "type_union",
                                        new AtomicDataType(
                                                new TypeInformationRawType<>(
                                                        false, Types.GENERIC(Object.class)),
                                                Object.class)),
                                FIELD("type_nested", address),
                                FIELD("type_bytes", BYTES().notNull()),
                                FIELD("type_date", DATE().notNull()),
                                FIELD("type_time_millis", TIME(3).notNull()),
                                FIELD("type_time_micros", TIME(6).notNull()),
                                FIELD("type_timestamp_millis", TIMESTAMP(3).notNull()),
                                FIELD("type_timestamp_micros", TIMESTAMP(6).notNull()),
                                FIELD("type_decimal_bytes", DECIMAL(4, 2).notNull()),
                                FIELD("type_decimal_fixed", DECIMAL(4, 2).notNull()))
                        .notNull();

        assertThat(actual).isEqualTo(user);
    }
}
