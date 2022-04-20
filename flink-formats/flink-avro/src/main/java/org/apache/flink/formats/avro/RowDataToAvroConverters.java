/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from {@link RowData} to Avro {@link GenericRecord}. */
@Internal
public class RowDataToAvroConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Flink Table & SQL internal data structures to
     * corresponding Avro data structures.
     */
    @FunctionalInterface
    public interface RowDataToAvroConverter extends Serializable {
        Object convert(Schema schema, Object object);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
    // sql-client uber jars.
    // --------------------------------------------------------------------------------

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Flink Table & SQL internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(LogicalType type) {
        final RowDataToAvroConverter converter;
        switch (type.getTypeRoot()) {
            case NULL:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return null;
                            }
                        };
                break;
            case TINYINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Byte) object).intValue();
                            }
                        };
                break;
            case SMALLINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Short) object).intValue();
                            }
                        };
                break;
            case BOOLEAN: // boolean
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
            case DOUBLE: // double
            case TIME_WITHOUT_TIME_ZONE: // int
            case DATE: // int
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return object;
                            }
                        };
                break;
            case CHAR:
            case VARCHAR:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return new Utf8(object.toString());
                            }
                        };
                break;
            case BINARY:
            case VARBINARY:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ByteBuffer.wrap((byte[]) object);
                            }
                        };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = LogicalTypeChecks.getPrecision(type);
                if (precision <= 3) {
                    converter =
                            new RowDataToAvroConverter() {
                                private static final long serialVersionUID = 1L;

                                @Override
                                public Object convert(Schema schema, Object object) {
                                    return ((TimestampData) object).toInstant().toEpochMilli();
                                }
                            };
                } else if (precision <= 6) {
                    converter =
                            new RowDataToAvroConverter() {
                                private static final long serialVersionUID = 1L;

                                @Override
                                public Object convert(Schema schema, Object object) {
                                    Instant instant = ((TimestampData) object).toInstant();
                                    return instant.getEpochSecond() * 1_000_000
                                            + instant.get(ChronoField.MICRO_OF_SECOND);
                                }
                            };
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
                break;
            case DECIMAL:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ByteBuffer.wrap(((DecimalData) object).toUnscaledBytes());
                            }
                        };
                break;
            case ARRAY:
                converter = createArrayConverter((ArrayType) type);
                break;
            case ROW:
                converter = createRowConverter((RowType) type);
                break;
            case MAP:
            case MULTISET:
                converter = createMapConverter(type);
                break;
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        // wrap into nullable converter
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                if (object == null) {
                    return null;
                }

                // get actual schema if it is a nullable schema
                Schema actualSchema;
                if (schema.getType() == Schema.Type.UNION) {
                    List<Schema> types = schema.getTypes();
                    int size = types.size();
                    if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(0);
                    } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(1);
                    } else {
                        throw new IllegalArgumentException(
                                "The Avro schema is not a nullable type: " + schema);
                    }
                } else {
                    actualSchema = schema;
                }
                return converter.convert(actualSchema, object);
            }
        };
    }

    private static RowDataToAvroConverter createRowConverter(RowType rowType) {
        final RowDataToAvroConverter[] fieldConverters =
                rowType.getChildren().stream()
                        .map(RowDataToAvroConverters::createConverter)
                        .toArray(RowDataToAvroConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        final int length = rowType.getFieldCount();

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final RowData row = (RowData) object;
                final List<Schema.Field> fields = schema.getFields();
                final GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < length; ++i) {
                    final Schema.Field schemaField = fields.get(i);
                    try {
                        Object avroObject =
                                fieldConverters[i].convert(
                                        schemaField.schema(), fieldGetters[i].getFieldOrNull(row));
                        record.put(i, avroObject);
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format(
                                        "Fail to serialize at field: %s.", schemaField.name()),
                                t);
                    }
                }
                return record;
            }
        };
    }

    private static RowDataToAvroConverter createArrayConverter(ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToAvroConverter elementConverter = createConverter(arrayType.getElementType());

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema elementSchema = schema.getElementType();
                ArrayData arrayData = (ArrayData) object;
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < arrayData.size(); ++i) {
                    list.add(
                            elementConverter.convert(
                                    elementSchema, elementGetter.getElementOrNull(arrayData, i)));
                }
                return list;
            }
        };
    }

    private static RowDataToAvroConverter createMapConverter(LogicalType type) {
        LogicalType valueType = extractValueTypeToAvroMap(type);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        final RowDataToAvroConverter valueConverter = createConverter(valueType);

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema valueSchema = schema.getValueType();
                final MapData mapData = (MapData) object;
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                final Map<Object, Object> map = new HashMap<>(mapData.size());
                for (int i = 0; i < mapData.size(); ++i) {
                    final String key = keyArray.getString(i).toString();
                    final Object value =
                            valueConverter.convert(
                                    valueSchema, valueGetter.getElementOrNull(valueArray, i));
                    map.put(key, value);
                }
                return map;
            }
        };
    }
}
