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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
@Internal
public class AvroToRowDataConverters {

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowDataConverter createRowConverter(RowType rowType) {
        return createRowConverter(rowType, true);
    }

    public static AvroToRowDataConverter createRowConverter(
            RowType rowType, boolean legacyTimestampMapping) {
        return createRowConverterInternal(
                rowType, legacyTimestampMapping, null, FieldMatching.INDEX);
    }

    /**
     * Creates a runtime converter that maps Avro records onto the given row type using the given
     * field matching strategy.
     *
     * <p>Prefer {@link #createRowConverter(Schema, RowType, FieldMatching)} where the reader schema
     * is known: passing it lets a field mismatch be reported when the converter is created rather
     * than when the first record arrives.
     */
    public static AvroToRowDataConverter createRowConverter(
            RowType rowType, FieldMatching fieldMatching) {
        return createRowConverterInternal(rowType, true, null, fieldMatching);
    }

    /**
     * Creates a runtime converter that maps records of the given reader schema onto the given row
     * type using the given field matching strategy.
     */
    public static AvroToRowDataConverter createRowConverter(
            Schema readerSchema, RowType rowType, FieldMatching fieldMatching) {
        return createRowConverter(readerSchema, rowType, true, fieldMatching);
    }

    /**
     * Creates a runtime converter that maps records of the given reader schema onto the given row
     * type using the given field matching strategy.
     */
    public static AvroToRowDataConverter createRowConverter(
            Schema readerSchema,
            RowType rowType,
            boolean legacyTimestampMapping,
            FieldMatching fieldMatching) {
        return createRowConverterInternal(
                rowType, legacyTimestampMapping, readerSchema, fieldMatching);
    }

    private static AvroToRowDataConverter createRowConverterInternal(
            RowType rowType,
            boolean legacyTimestampMapping,
            @Nullable Schema schema,
            FieldMatching fieldMatching) {
        final int arity = rowType.getFieldCount();
        final Schema recordSchema = asRecordSchema(schema);

        // Resolving here is not what the runtime converter uses - the reader schema instance seen
        // at runtime is a different one, because the schema travels to the task as a string. It
        // serves two other purposes: reporting a field mismatch while the job is still being
        // assembled, and telling each nested converter which Avro field it is going to read.
        final AvroFieldMatcher.Plan plan =
                fieldMatching == FieldMatching.NAME && recordSchema != null
                        ? AvroFieldMatcher.forDeserialization(rowType, recordSchema)
                        : null;

        final AvroToRowDataConverter[] fieldConverters = new AvroToRowDataConverter[arity];
        for (int i = 0; i < arity; i++) {
            fieldConverters[i] =
                    createNullableConverter(
                            rowType.getTypeAt(i),
                            legacyTimestampMapping,
                            avroFieldSchema(recordSchema, plan, i),
                            fieldMatching);
        }

        if (fieldMatching == FieldMatching.NAME) {
            return new NameMatchingRowConverter(rowType, fieldConverters);
        }

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                // avro always deserialize successfully even though the type isn't matched
                // so no need to throw exception about which field can't be deserialized
                row.setField(i, fieldConverters[i].convert(record.get(i)));
            }
            return row;
        };
    }

    /**
     * Reads Avro record fields into the row field of the same name, so that the two may declare
     * their fields in a different order. See {@link AvroFieldMatcher} for the matching rules.
     */
    private static final class NameMatchingRowConverter implements AvroToRowDataConverter {

        private static final long serialVersionUID = 1L;

        private final RowType rowType;
        private final AvroToRowDataConverter[] fieldConverters;

        /**
         * The pairing resolved for the schema seen last. Records of a stream all share one reader
         * schema instance, so this is effectively resolved once. The plan is immutable and safely
         * publishable, hence no synchronization: the worst a racy read can cost is one redundant
         * resolution.
         */
        private transient AvroFieldMatcher.Plan plan;

        private NameMatchingRowConverter(
                RowType rowType, AvroToRowDataConverter[] fieldConverters) {
            this.rowType = rowType;
            this.fieldConverters = fieldConverters;
        }

        @Override
        public Object convert(Object avroObject) {
            final IndexedRecord record = (IndexedRecord) avroObject;
            final Schema recordSchema = record.getSchema();

            AvroFieldMatcher.Plan currentPlan = plan;
            if (currentPlan == null || !currentPlan.appliesTo(recordSchema)) {
                currentPlan = AvroFieldMatcher.forDeserialization(rowType, recordSchema);
                plan = currentPlan;
            }

            final GenericRowData row = new GenericRowData(fieldConverters.length);
            for (int i = 0; i < fieldConverters.length; ++i) {
                final int avroPos = currentPlan.avroPositionOf(i);
                // A column the record has no field for stays null; the plan already refused the
                // case where that would violate a NOT NULL column.
                if (avroPos != AvroFieldMatcher.UNMATCHED) {
                    // avro always deserialize successfully even though the type isn't matched
                    // so no need to throw exception about which field can't be deserialized
                    row.setField(i, fieldConverters[i].convert(record.get(avroPos)));
                }
            }
            return row;
        }
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroToRowDataConverter createNullableConverter(
            LogicalType type,
            boolean legacyTimestampMapping,
            @Nullable Schema schema,
            FieldMatching fieldMatching) {
        final AvroToRowDataConverter converter =
                createConverter(type, legacyTimestampMapping, schema, fieldMatching);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static AvroToRowDataConverter createConverter(
            LogicalType type,
            boolean legacyTimestampMapping,
            @Nullable Schema schema,
            FieldMatching fieldMatching) {
        switch (type.getTypeRoot()) {
            case NULL:
                return avroObject -> null;
            case TINYINT:
                return avroObject -> ((Integer) avroObject).byteValue();
            case SMALLINT:
                return avroObject -> ((Integer) avroObject).shortValue();
            case BOOLEAN: // boolean
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
            case DOUBLE: // double
                return avroObject -> avroObject;
            case DATE:
                return AvroToRowDataConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return AvroToRowDataConverters::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return AvroToRowDataConverters::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (legacyTimestampMapping) {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                } else {
                    return AvroToRowDataConverters::convertToTimestamp;
                }
            case CHAR:
            case VARCHAR:
                return avroObject -> StringData.fromString(avroObject.toString());
            case BINARY:
            case VARBINARY:
                return AvroToRowDataConverters::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter(
                        (ArrayType) type,
                        legacyTimestampMapping,
                        elementSchemaOf(schema),
                        fieldMatching);
            case ROW:
                return createRowConverterInternal(
                        (RowType) type, legacyTimestampMapping, schema, fieldMatching);
            case MAP:
            case MULTISET:
                return createMapConverter(
                        type, legacyTimestampMapping, valueSchemaOf(schema), fieldMatching);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return avroObject -> {
            final byte[] bytes;
            if (avroObject instanceof GenericFixed) {
                bytes = ((GenericFixed) avroObject).bytes();
            } else if (avroObject instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) avroObject;
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
            } else {
                bytes = (byte[]) avroObject;
            }
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private static AvroToRowDataConverter createArrayConverter(
            ArrayType arrayType,
            boolean legacyTimestampMapping,
            @Nullable Schema elementSchema,
            FieldMatching fieldMatching) {
        final AvroToRowDataConverter elementConverter =
                createNullableConverter(
                        arrayType.getElementType(),
                        legacyTimestampMapping,
                        elementSchema,
                        fieldMatching);
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return avroObject -> {
            final List<?> list = (List<?>) avroObject;
            final int length = list.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, length);
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArrayData(array);
        };
    }

    private static AvroToRowDataConverter createMapConverter(
            LogicalType type,
            boolean legacyTimestampMapping,
            @Nullable Schema valueSchema,
            FieldMatching fieldMatching) {
        final AvroToRowDataConverter keyConverter =
                createConverter(
                        DataTypes.STRING().getLogicalType(),
                        legacyTimestampMapping,
                        null,
                        fieldMatching);
        final AvroToRowDataConverter valueConverter =
                createNullableConverter(
                        extractValueTypeToAvroMap(type),
                        legacyTimestampMapping,
                        valueSchema,
                        fieldMatching);

        return avroObject -> {
            final Map<?, ?> map = (Map<?, ?>) avroObject;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    // -------------------------------------------------------------------------------------
    // Reader schema navigation
    //
    // The reader schema is optional throughout: it is only used to validate eagerly and to hand
    // each nested converter its own schema. Wherever a schema cannot be narrowed down to a single
    // Avro type - a true multi-type union, say - navigation yields null and the converters fall
    // back to resolving from the record they are handed at runtime.
    // -------------------------------------------------------------------------------------

    private static @Nullable Schema avroFieldSchema(
            @Nullable Schema recordSchema,
            @Nullable AvroFieldMatcher.Plan plan,
            int rowFieldIndex) {
        if (recordSchema == null) {
            return null;
        }
        final int position = plan == null ? rowFieldIndex : plan.avroPositionOf(rowFieldIndex);
        final List<Schema.Field> fields = recordSchema.getFields();
        if (position == AvroFieldMatcher.UNMATCHED || position >= fields.size()) {
            return null;
        }
        return fields.get(position).schema();
    }

    private static @Nullable Schema asRecordSchema(@Nullable Schema schema) {
        final Schema resolved = unwrapNullableUnion(schema);
        return resolved != null && resolved.getType() == Schema.Type.RECORD ? resolved : null;
    }

    private static @Nullable Schema elementSchemaOf(@Nullable Schema schema) {
        final Schema resolved = unwrapNullableUnion(schema);
        return resolved != null && resolved.getType() == Schema.Type.ARRAY
                ? resolved.getElementType()
                : null;
    }

    private static @Nullable Schema valueSchemaOf(@Nullable Schema schema) {
        final Schema resolved = unwrapNullableUnion(schema);
        return resolved != null && resolved.getType() == Schema.Type.MAP
                ? resolved.getValueType()
                : null;
    }

    private static @Nullable Schema unwrapNullableUnion(@Nullable Schema schema) {
        if (schema == null || schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        Schema resolved = null;
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                continue;
            }
            if (resolved != null) {
                // More than one non-null branch: the reader schema of a field cannot be pinned
                // down statically, so give up rather than guess.
                return null;
            }
            resolved = branch;
        }
        return resolved;
    }

    private static TimestampData convertToTimestamp(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else if (object instanceof Instant) {
            millis = ((Instant) object).toEpochMilli();
        } else if (object instanceof LocalDateTime) {
            return TimestampData.fromLocalDateTime((LocalDateTime) object);
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTimestamp(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIMESTAMP logical type. Received: " + object);
            }
        }
        return TimestampData.fromEpochMillis(millis);
    }

    private static int convertToDate(Object object) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                return (int) jodaConverter.convertDate(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for DATE logical type. Received: " + object);
            }
        }
    }

    private static int convertToTime(Object object) {
        final int millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTime(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIME logical type. Received: " + object);
            }
        }
        return millis;
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof GenericFixed) {
            return ((GenericFixed) object).bytes();
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            return (byte[]) object;
        }
    }
}
