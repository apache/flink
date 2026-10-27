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

package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantFieldGetter;
import org.apache.flink.types.variant.VariantTypeException;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Converts a {@link Variant} into internal data of a SQL type, for {@code CAST(v AS T)}. {@link
 * VariantCastUtils} defines how each leaf is read.
 *
 * <p>A {@code ROW} takes object fields by name and fails on a missing one. A nested variant null
 * becomes {@code NULL}, fails for a {@code NOT NULL} type, and stays a variant null for a {@code
 * VARIANT} type. A top-level variant null fails, so a cast to a nullable type maps it to {@code
 * NULL} first.
 */
@Internal
public final class FromVariantConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ValueReader reader;

    private FromVariantConverter(final ValueReader reader) {
        this.reader = reader;
    }

    /**
     * Creates a converter for a type that VARIANT casts to.
     *
     * @param sessionZone renders a {@code TIMESTAMP_LTZ} value read as a string
     * @throws IllegalArgumentException if VARIANT cannot be cast to the type
     */
    public static FromVariantConverter create(final LogicalType type, final ZoneId sessionZone) {
        return new FromVariantConverter(
                createReader(type, TimeZone.getTimeZone(sessionZone), false));
    }

    /**
     * Converts a variant into internal data.
     *
     * @throws TableRuntimeException if a value does not match its type
     * @throws VariantTypeException if the variant is malformed, or a {@code BOOLEAN}, {@code DATE}
     *     or binary value has another kind
     */
    public Object convert(final Variant variant) {
        return reader.read(variant);
    }

    @FunctionalInterface
    private interface ValueReader extends Serializable {
        Object read(Variant variant);
    }

    // With copyVariants, a VARIANT is copied out of its parent rather than returned as a view.
    private static ValueReader createReader(
            final LogicalType type, final TimeZone sessionZone, final boolean copyVariants) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Variant::getBoolean;
            case TINYINT:
                return variant ->
                        (byte)
                                VariantCastUtils.toIntegral(
                                        variant, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT");
            case SMALLINT:
                return variant ->
                        (short)
                                VariantCastUtils.toIntegral(
                                        variant, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT");
            case INTEGER:
                return variant ->
                        (int)
                                VariantCastUtils.toIntegral(
                                        variant, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER");
            case BIGINT:
                return variant ->
                        VariantCastUtils.toIntegral(
                                variant, Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT");
            case FLOAT:
                return VariantCastUtils::toFloat;
            case DOUBLE:
                return VariantCastUtils::toDouble;
            case DECIMAL:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    final int scale = LogicalTypeChecks.getScale(type);
                    return variant -> VariantCastUtils.toDecimal(variant, precision, scale);
                }
            case CHAR:
            case VARCHAR:
                {
                    final int length = LogicalTypeChecks.getLength(type);
                    final boolean charTarget = type.is(LogicalTypeRoot.CHAR);
                    return variant ->
                            VariantCastUtils.toStringValue(
                                    variant, sessionZone, length, charTarget);
                }
            case BINARY:
            case VARBINARY:
                {
                    final int length = LogicalTypeChecks.getLength(type);
                    final boolean fixedLength = type.is(LogicalTypeRoot.BINARY);
                    return variant -> VariantCastUtils.toBytes(variant, length, fixedLength);
                }
            case DATE:
                return variant -> (int) variant.getDate().toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    return variant -> VariantCastUtils.toTime(variant, precision);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    return variant -> VariantCastUtils.toTimestamp(variant, precision);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    return variant -> VariantCastUtils.toTimestampLtz(variant, precision);
                }
            case UUID:
                return VariantCastUtils::toUuid;
            case VARIANT:
                return copyVariants ? FromVariantConverter::copyVariant : variant -> variant;
            case ARRAY:
                return createArrayReader((ArrayType) type, sessionZone, copyVariants);
            case MAP:
                return createMapReader((MapType) type, sessionZone, copyVariants);
            case ROW:
            case STRUCTURED_TYPE:
                return createRowReader(type, sessionZone);
            case DISTINCT_TYPE:
                return createReader(
                        ((DistinctType) type).getSourceType(), sessionZone, copyVariants);
            default:
                throw new IllegalArgumentException("Cannot cast VARIANT to " + type + ".");
        }
    }

    // A nested variant null becomes NULL, or fails for a NOT NULL type. A VARIANT type keeps it.
    private static ValueReader createChildReader(
            final LogicalType type,
            final TimeZone sessionZone,
            final boolean copyVariants,
            final String notNullMessage) {
        final ValueReader reader = createReader(type, sessionZone, copyVariants);
        if (type.is(LogicalTypeRoot.VARIANT)) {
            return reader;
        }
        if (type.isNullable()) {
            return variant -> variant.isNull() ? null : reader.read(variant);
        }
        return variant -> {
            if (variant.isNull()) {
                throw new TableRuntimeException(notNullMessage);
            }
            return reader.read(variant);
        };
    }

    private static ValueReader createArrayReader(
            final ArrayType type, final TimeZone sessionZone, final boolean copyVariants) {
        final LogicalType elementType = type.getElementType();
        final ValueReader elementReader =
                createChildReader(
                        elementType,
                        sessionZone,
                        copyVariants,
                        "Cannot cast a VARIANT null array element to a NOT NULL element type.");
        // A typed array keeps GenericArrayData#toIntArray and its siblings working.
        final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(elementType);
        final String arrayType = type.asSummaryString();
        return variant -> {
            final int size = VariantCastUtils.arraySize(variant, arrayType);
            final Object[] elements = (Object[]) Array.newInstance(elementClass, size);
            for (int i = 0; i < size; i++) {
                elements[i] = elementReader.read(variant.getElement(i));
            }
            return new GenericArrayData(elements);
        };
    }

    private static ValueReader createMapReader(
            final MapType type, final TimeZone sessionZone, final boolean copyVariants) {
        final LogicalType keyType = type.getKeyType();
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new IllegalArgumentException(
                    "Cannot cast VARIANT to " + type + ". A MAP key must be a character string.");
        }
        final int keyLength = LogicalTypeChecks.getLength(keyType);
        final boolean charKey = keyType.is(LogicalTypeRoot.CHAR);
        final ValueReader valueReader =
                createChildReader(
                        type.getValueType(),
                        sessionZone,
                        copyVariants,
                        "Cannot cast a VARIANT null object value to a NOT NULL map value type.");
        final String mapType = type.asSummaryString();
        return variant -> {
            VariantCastUtils.requireObject(variant, mapType);
            final List<String> names = variant.getFieldNames();
            final Map<Object, Object> map = new HashMap<>(names.size());
            for (final String name : names) {
                final Object value = valueReader.read(variant.getField(name));
                map.put(VariantCastUtils.variantKey(name, keyLength, charKey), value);
            }
            return new GenericMapData(map);
        };
    }

    // A ROW copies every VARIANT below it, like a value read back from a binary row.
    private static ValueReader createRowReader(final LogicalType type, final TimeZone sessionZone) {
        final List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(type);
        final String[] fieldNames = LogicalTypeChecks.getFieldNames(type).toArray(new String[0]);
        final int fieldCount = fieldTypes.size();
        final String rowType = type.asSummaryString();
        final VariantFieldGetter[] fieldGetters = new VariantFieldGetter[fieldCount];
        final ValueReader[] fieldReaders = new ValueReader[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = new VariantFieldGetter(fieldNames[i]);
            fieldReaders[i] =
                    createChildReader(
                            fieldTypes.get(i),
                            sessionZone,
                            true,
                            nullFieldError(rowType, fieldNames[i]));
        }
        return variant -> {
            VariantCastUtils.requireObject(variant, rowType);
            final GenericRowData row = new GenericRowData(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                // getField returns null for a field the object does not have.
                final Variant field = fieldGetters[i].getField(variant);
                if (field == null) {
                    throw new TableRuntimeException(missingFieldError(rowType, fieldNames[i]));
                }
                row.setField(i, fieldReaders[i].read(field));
            }
            return row;
        };
    }

    private static Variant copyVariant(final Variant variant) {
        final BinaryVariant view = (BinaryVariant) variant;
        return new BinaryVariant(view.getValue(), view.getMetadata());
    }

    private static String missingFieldError(final String rowType, final String fieldName) {
        return String.format(
                "Cannot cast the VARIANT object to %s because the field '%s' is not present in "
                        + "the VARIANT.",
                rowType, fieldName);
    }

    private static String nullFieldError(final String rowType, final String fieldName) {
        return String.format(
                "Cannot cast the VARIANT object to %s because the field '%s' is a VARIANT null "
                        + "and the target does not accept NULL.",
                rowType, fieldName);
    }
}
