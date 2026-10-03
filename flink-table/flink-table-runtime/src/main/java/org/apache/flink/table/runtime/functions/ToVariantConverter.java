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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.UuidUtils;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder.FieldEntry;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantTypeException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts internal data of a SQL type into a {@link Variant}, like {@code CAST(x AS VARIANT)}.
 *
 * <p>An {@code ARRAY} becomes a variant array. A {@code ROW} or {@code STRUCTURED} value becomes a
 * variant object keyed by its field names, and a {@code MAP} one keyed by its character string
 * keys. A {@code NULL} element, field or map value becomes a variant null. A leaf is stored like
 * the cast stores it on its own, see {@link VariantCastUtils}, and a nested {@code VARIANT} is
 * embedded as is.
 *
 * <p>The converter is created once per type and writes a whole value into one builder, so a nested
 * value is encoded in a single pass. It keeps no state between calls.
 */
@Internal
public final class ToVariantConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sourceType;
    private final ValueWriter writer;

    private ToVariantConverter(String sourceType, ValueWriter writer) {
        this.sourceType = sourceType;
        this.writer = writer;
    }

    /** Creates a converter for a type that {@code LogicalTypeCasts} allows to cast to VARIANT. */
    public static ToVariantConverter create(LogicalType type) {
        return new ToVariantConverter(type.asSummaryString(), createWriter(type));
    }

    /**
     * Converts a value that is not {@code NULL}.
     *
     * @throws TableRuntimeException if the value has a {@code NULL} map key, a timestamp outside
     *     the range of its variant kind, or does not fit into the 16 MiB of a {@code VARIANT}
     */
    public Variant convert(Object value) {
        // A MAP can hold the same key twice at runtime. The builder keeps the last one, like the
        // MAP constructor does.
        final BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(true);
        try {
            writer.write(builder, value);
            return builder.build();
        } catch (VariantTypeException e) {
            if (e != BinaryVariantInternalBuilder.VARIANT_SIZE_LIMIT_EXCEPTION) {
                throw e;
            }
            throw new TableRuntimeException(
                    String.format(
                            "Cannot cast a value of type %s to VARIANT. A VARIANT is limited to "
                                    + "16 MiB.",
                            sourceType));
        }
    }

    @FunctionalInterface
    private interface ValueWriter extends Serializable {
        void write(BinaryVariantInternalBuilder builder, Object value);
    }

    private static ValueWriter createNullableWriter(LogicalType type) {
        final ValueWriter writer = createWriter(type);
        return (builder, value) -> {
            if (value == null) {
                builder.appendNull();
            } else {
                writer.write(builder, value);
            }
        };
    }

    private static ValueWriter createWriter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (builder, value) -> builder.appendNull();
            case BOOLEAN:
                return (builder, value) -> builder.appendBoolean((Boolean) value);
            case TINYINT:
                return (builder, value) -> builder.appendByte((Byte) value);
            case SMALLINT:
                return (builder, value) -> builder.appendShort((Short) value);
            case INTEGER:
                return (builder, value) -> builder.appendInt((Integer) value);
            case BIGINT:
                return (builder, value) -> builder.appendLong((Long) value);
            case FLOAT:
                return (builder, value) -> builder.appendFloat((Float) value);
            case DOUBLE:
                return (builder, value) -> builder.appendDouble((Double) value);
            case DECIMAL:
                return (builder, value) ->
                        builder.appendDecimal(((DecimalData) value).toBigDecimal());
            case CHAR:
            case VARCHAR:
                return (builder, value) -> builder.appendString(value.toString());
            case BINARY:
            case VARBINARY:
                return (builder, value) -> builder.appendBinary((byte[]) value);
            case DATE:
                return (builder, value) -> builder.appendDate((Integer) value);
            case TIME_WITHOUT_TIME_ZONE:
                // TIME is millisecond-of-day at runtime, the variant kind holds microseconds.
                return (builder, value) -> builder.appendTime((Integer) value * 1_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    return (builder, value) ->
                            VariantCastUtils.appendTimestamp(
                                    builder, (TimestampData) value, precision);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    return (builder, value) ->
                            VariantCastUtils.appendTimestampLtz(
                                    builder, (TimestampData) value, precision);
                }
            case UUID:
                return (builder, value) -> builder.appendUuid(UuidUtils.fromBytes((byte[]) value));
            case VARIANT:
                return (builder, value) -> builder.appendVariant((BinaryVariant) value);
            case ARRAY:
                return createArrayWriter((ArrayType) type);
            case MAP:
                return createMapWriter((MapType) type);
            case ROW:
            case STRUCTURED_TYPE:
                return createRowWriter(type);
            case DISTINCT_TYPE:
                return createWriter(((DistinctType) type).getSourceType());
            default:
                throw new IllegalArgumentException("Cannot cast " + type + " to VARIANT.");
        }
    }

    private static ValueWriter createArrayWriter(ArrayType type) {
        final ArrayData.ElementGetter elementGetter = createElementGetter(type.getElementType());
        final ValueWriter elementWriter = createNullableWriter(type.getElementType());
        return (builder, value) -> {
            final ArrayData array = (ArrayData) value;
            final int size = array.size();
            final int start = builder.getWritePos();
            final ArrayList<Integer> offsets = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                offsets.add(builder.getWritePos() - start);
                elementWriter.write(builder, elementGetter.getElementOrNull(array, i));
            }
            builder.finishWritingArray(start, offsets);
        };
    }

    private static ValueWriter createMapWriter(MapType type) {
        final ArrayData.ElementGetter keyGetter = createElementGetter(type.getKeyType());
        final ArrayData.ElementGetter valueGetter = createElementGetter(type.getValueType());
        final ValueWriter valueWriter = createNullableWriter(type.getValueType());
        final String mapType = type.asSummaryString();
        return (builder, value) -> {
            final MapData map = (MapData) value;
            final ArrayData keys = map.keyArray();
            final ArrayData values = map.valueArray();
            final int size = map.size();
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final Object key = keyGetter.getElementOrNull(keys, i);
                if (key == null) {
                    throw new TableRuntimeException(
                            String.format(
                                    "Cannot cast a value of type %s with a NULL key to VARIANT. "
                                            + "A VARIANT object key cannot be NULL.",
                                    mapType));
                }
                final String name = key.toString();
                fields.add(
                        new FieldEntry(name, builder.addKey(name), builder.getWritePos() - start));
                valueWriter.write(builder, valueGetter.getElementOrNull(values, i));
            }
            builder.finishWritingObject(start, fields);
        };
    }

    private static ValueWriter createRowWriter(LogicalType type) {
        final List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(type);
        final String[] fieldNames = LogicalTypeChecks.getFieldNames(type).toArray(new String[0]);
        final int fieldCount = fieldTypes.size();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
        final ValueWriter[] fieldWriters = new ValueWriter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = createFieldGetter(fieldTypes.get(i), i);
            fieldWriters[i] = createNullableWriter(fieldTypes.get(i));
        }
        return (builder, value) -> {
            final RowData row = (RowData) value;
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                final String name = fieldNames[i];
                fields.add(
                        new FieldEntry(name, builder.addKey(name), builder.getWritePos() - start));
                fieldWriters[i].write(builder, fieldGetters[i].getFieldOrNull(row));
            }
            builder.finishWritingObject(start, fields);
        };
    }

    // A NULL type has no accessor, but its values are always NULL, which becomes a variant null.
    private static ArrayData.ElementGetter createElementGetter(LogicalType type) {
        return type.is(LogicalTypeRoot.NULL)
                ? (array, pos) -> null
                : ArrayData.createElementGetter(type);
    }

    private static RowData.FieldGetter createFieldGetter(LogicalType type, int pos) {
        return type.is(LogicalTypeRoot.NULL) ? row -> null : RowData.createFieldGetter(type, pos);
    }
}
