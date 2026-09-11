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
import org.apache.flink.util.CollectionUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
        return createConverter(type, true);
    }

    public static RowDataToAvroConverter createConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        return createConverter(type, legacyTimestampMapping, FieldMatching.INDEX);
    }

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Flink Table & SQL internal data structures to corresponding Avro data structures.
     *
     * @param legacyTimestampMapping whether to use the legacy mapping of Flink timestamp types onto
     *     Avro timestamp types.
     * @param fieldMatching how the fields of a {@link RowType} are paired with the fields of the
     *     target Avro record schema. Only {@link FieldMatching#NAME} tolerates the two declaring
     *     their fields in a different order.
     */
    public static RowDataToAvroConverter createConverter(
            LogicalType type, boolean legacyTimestampMapping, FieldMatching fieldMatching) {
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
                                if (schema.getType() == Schema.Type.ENUM) {
                                    return new GenericData.EnumSymbol(schema, object.toString());
                                }
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
                if (legacyTimestampMapping) {
                    converter =
                            new RowDataToAvroConverter() {
                                private static final long serialVersionUID = 1L;

                                @Override
                                public Object convert(Schema schema, Object object) {
                                    return ((TimestampData) object).toInstant().toEpochMilli();
                                }
                            };
                } else {
                    converter =
                            new RowDataToAvroConverter() {
                                private static final long serialVersionUID = 1L;

                                @Override
                                public Object convert(Schema schema, Object object) {
                                    return ((TimestampData) object)
                                            .toLocalDateTime()
                                            .toInstant(ZoneOffset.UTC)
                                            .toEpochMilli();
                                }
                            };
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (legacyTimestampMapping) {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                } else {
                    converter =
                            new RowDataToAvroConverter() {
                                private static final long serialVersionUID = 1L;

                                @Override
                                public Object convert(Schema schema, Object object) {
                                    return ((TimestampData) object).toInstant().toEpochMilli();
                                }
                            };
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
                converter =
                        createArrayConverter(
                                (ArrayType) type, legacyTimestampMapping, fieldMatching);
                break;
            case ROW:
                converter =
                        createRowConverter((RowType) type, legacyTimestampMapping, fieldMatching);
                break;
            case MAP:
            case MULTISET:
                converter = createMapConverter(type, legacyTimestampMapping, fieldMatching);
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
                                "The Avro schema is not a nullable type: " + schema.toString());
                    }
                } else {
                    actualSchema = schema;
                }
                return converter.convert(actualSchema, object);
            }
        };
    }

    private static RowDataToAvroConverter createRowConverter(
            RowType rowType, boolean legacyTimestampMapping, FieldMatching fieldMatching) {
        final RowDataToAvroConverter[] fieldConverters =
                rowType.getChildren().stream()
                        .map(
                                fieldType ->
                                        createConverter(
                                                fieldType, legacyTimestampMapping, fieldMatching))
                        .toArray(RowDataToAvroConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return fieldMatching == FieldMatching.NAME
                ? new NameMatchingRowConverter(rowType, fieldConverters, fieldGetters)
                : new IndexMatchingRowConverter(fieldConverters, fieldGetters);
    }

    /** Pairs the n-th field of the row with the n-th field of the Avro record schema. */
    private static final class IndexMatchingRowConverter implements RowDataToAvroConverter {

        private static final long serialVersionUID = 1L;

        private final RowDataToAvroConverter[] fieldConverters;
        private final RowData.FieldGetter[] fieldGetters;

        private IndexMatchingRowConverter(
                RowDataToAvroConverter[] fieldConverters, RowData.FieldGetter[] fieldGetters) {
            this.fieldConverters = fieldConverters;
            this.fieldGetters = fieldGetters;
        }

        @Override
        public Object convert(Schema schema, Object object) {
            final RowData row = (RowData) object;
            final List<Schema.Field> fields = schema.getFields();
            final GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < fieldConverters.length; ++i) {
                final Schema.Field schemaField = fields.get(i);
                try {
                    Object avroObject =
                            fieldConverters[i].convert(
                                    schemaField.schema(), fieldGetters[i].getFieldOrNull(row));
                    record.put(i, avroObject);
                } catch (Throwable t) {
                    throw new RuntimeException(
                            String.format("Fail to serialize at field: %s.", schemaField.name()),
                            t);
                }
            }
            return record;
        }
    }

    /**
     * Pairs row fields with Avro record fields by name, so that the two may declare their fields in
     * a different order. See {@link AvroFieldMatcher} for the matching rules.
     */
    private static final class NameMatchingRowConverter implements RowDataToAvroConverter {

        private static final long serialVersionUID = 1L;

        private final RowType rowType;
        private final RowDataToAvroConverter[] fieldConverters;
        private final RowData.FieldGetter[] fieldGetters;

        /**
         * The pairing resolved for the schema seen last. Callers hand the same schema instance to
         * every {@link #convert} call, so this is effectively resolved once. The plan is immutable
         * and safely publishable, hence no synchronization: the worst a racy read can cost is one
         * redundant resolution.
         */
        private transient AvroFieldMatcher.Plan plan;

        private NameMatchingRowConverter(
                RowType rowType,
                RowDataToAvroConverter[] fieldConverters,
                RowData.FieldGetter[] fieldGetters) {
            this.rowType = rowType;
            this.fieldConverters = fieldConverters;
            this.fieldGetters = fieldGetters;
        }

        @Override
        public Object convert(Schema schema, Object object) {
            AvroFieldMatcher.Plan currentPlan = plan;
            if (currentPlan == null || !currentPlan.appliesTo(schema)) {
                currentPlan = AvroFieldMatcher.forSerialization(rowType, schema);
                plan = currentPlan;
            }

            final RowData row = (RowData) object;
            final List<Schema.Field> fields = schema.getFields();
            final GenericRecord record = new GenericData.Record(schema);
            // Avro fields no column maps to keep their declared default rather than staying null.
            currentPlan.fillDefaults(record);

            for (int i = 0; i < fieldConverters.length; ++i) {
                // A serialization plan never leaves a column unmatched, so this is always a field.
                final int avroPos = currentPlan.avroPositionOf(i);
                final Schema.Field schemaField = fields.get(avroPos);
                try {
                    Object avroObject =
                            fieldConverters[i].convert(
                                    schemaField.schema(), fieldGetters[i].getFieldOrNull(row));
                    record.put(avroPos, avroObject);
                } catch (Throwable t) {
                    throw new RuntimeException(
                            String.format("Fail to serialize at field: %s.", schemaField.name()),
                            t);
                }
            }
            return record;
        }
    }

    private static RowDataToAvroConverter createArrayConverter(
            ArrayType arrayType, boolean legacyTimestampMapping, FieldMatching fieldMatching) {
        LogicalType elementType = arrayType.getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToAvroConverter elementConverter =
                createConverter(elementType, legacyTimestampMapping, fieldMatching);

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

    private static RowDataToAvroConverter createMapConverter(
            LogicalType type, boolean legacyTimestampMapping, FieldMatching fieldMatching) {
        LogicalType valueType = extractValueTypeToAvroMap(type);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        final RowDataToAvroConverter valueConverter =
                createConverter(valueType, legacyTimestampMapping, fieldMatching);

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema valueSchema = schema.getValueType();
                final MapData mapData = (MapData) object;
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                final Map<Object, Object> map =
                        CollectionUtil.newHashMapWithExpectedSize(mapData.size());
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
