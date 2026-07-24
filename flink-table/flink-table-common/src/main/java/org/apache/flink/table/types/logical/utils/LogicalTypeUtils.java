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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utilities for handling {@link LogicalType}s. */
@Internal
public final class LogicalTypeUtils {

    private static final String ATOMIC_FIELD_NAME = "f0";

    private static final TimeAttributeRemover TIME_ATTRIBUTE_REMOVER = new TimeAttributeRemover();
    private static final NullabilityNormalizer NULLABILITY_NORMALIZER = new NullabilityNormalizer();

    public static LogicalType removeTimeAttributes(LogicalType logicalType) {
        return logicalType.accept(TIME_ATTRIBUTE_REMOVER);
    }

    public static LogicalType normalizeNullability(LogicalType logicalType) {
        return logicalType.accept(NULLABILITY_NORMALIZER);
    }

    /**
     * Returns true when new types are structurally equal to old types ignoring nullability, and no
     * type narrows from nullable to non-nullable.
     */
    public static boolean areTypesCompatibleAfterNullabilityWidening(
            LogicalType[] newTypes, LogicalType[] oldTypes) {
        if (newTypes == oldTypes) {
            return true;
        }
        if (newTypes == null || oldTypes == null || newTypes.length != oldTypes.length) {
            return false;
        }
        for (int i = 0; i < newTypes.length; i++) {
            if (!isTypeCompatibleAfterNullabilityWidening(newTypes[i], oldTypes[i])) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTypeCompatibleAfterNullabilityWidening(
            LogicalType newType, LogicalType oldType) {
        if (newType == oldType) {
            return true;
        }
        if (newType == null || oldType == null) {
            return false;
        }
        return normalizeNullability(newType).equals(normalizeNullability(oldType))
                && !hasNullabilityNarrowing(newType, oldType);
    }

    /**
     * Returns the conversion class for the given {@link LogicalType} that is used by the table
     * runtime as internal data structure.
     *
     * @see RowData
     */
    public static Class<?> toInternalConversionClass(LogicalType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return DecimalData.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return Integer.class;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.class;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException("Unsupported type: " + type);
            case ARRAY:
                return ArrayData.class;
            case MULTISET:
            case MAP:
                return MapData.class;
            case ROW:
            case STRUCTURED_TYPE:
                return RowData.class;
            case DISTINCT_TYPE:
                return toInternalConversionClass(((DistinctType) type).getSourceType());
            case RAW:
                return RawValueData.class;
            case NULL:
                return Object.class;
            case DESCRIPTOR:
                return ColumnList.class;
            case VARIANT:
                return Variant.class;
            case BITMAP:
                return Bitmap.class;
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Converts any logical type to a row type. Composite types are converted to a row type. Atomic
     * types are wrapped into a field.
     */
    public static RowType toRowType(LogicalType t) {
        switch (t.getTypeRoot()) {
            case ROW:
                return (RowType) t;
            case STRUCTURED_TYPE:
                final StructuredType structuredType = (StructuredType) t;
                final List<RowField> fields =
                        structuredType.getAttributes().stream()
                                .map(
                                        attribute ->
                                                new RowField(
                                                        attribute.getName(),
                                                        attribute.getType(),
                                                        attribute.getDescription().orElse(null)))
                                .collect(Collectors.toList());
                return new RowType(structuredType.isNullable(), fields);
            case DISTINCT_TYPE:
                return toRowType(((DistinctType) t).getSourceType());
            default:
                return RowType.of(t);
        }
    }

    /** Returns a unique name for an atomic type. */
    public static String getAtomicName(List<String> existingNames) {
        int i = 0;
        String fieldName = ATOMIC_FIELD_NAME;
        while ((null != existingNames) && existingNames.contains(fieldName)) {
            fieldName = ATOMIC_FIELD_NAME + "_" + i++;
        }
        return fieldName;
    }

    /** Renames the fields of the given {@link RowType}. */
    public static RowType renameRowFields(RowType rowType, List<String> newFieldNames) {
        Preconditions.checkArgument(
                rowType.getFieldCount() == newFieldNames.size(),
                "Row length and new names must match.");
        final List<RowField> newFields =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                pos -> {
                                    final RowField oldField = rowType.getFields().get(pos);
                                    return new RowField(
                                            newFieldNames.get(pos),
                                            oldField.getType(),
                                            oldField.getDescription().orElse(null));
                                })
                        .collect(Collectors.toList());
        return new RowType(rowType.isNullable(), newFields);
    }

    // --------------------------------------------------------------------------------------------

    private static class TimeAttributeRemover extends LogicalTypeDuplicator {

        @Override
        public LogicalType visit(TimestampType timestampType) {
            return new TimestampType(timestampType.isNullable(), timestampType.getPrecision());
        }

        @Override
        public LogicalType visit(ZonedTimestampType zonedTimestampType) {
            return new ZonedTimestampType(
                    zonedTimestampType.isNullable(), zonedTimestampType.getPrecision());
        }

        @Override
        public LogicalType visit(LocalZonedTimestampType localZonedTimestampType) {
            return new LocalZonedTimestampType(
                    localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
        }
    }

    private static boolean hasNullabilityNarrowing(LogicalType newType, LogicalType oldType) {
        // reject narrowing: nullable -> non-nullable
        if (oldType.isNullable() && !newType.isNullable()) {
            return true;
        }

        if (newType.is(LogicalTypeRoot.UNRESOLVED) || oldType.is(LogicalTypeRoot.UNRESOLVED)) {
            return false;
        }

        if (newType instanceof StructuredType && oldType instanceof StructuredType) {
            StructuredType newStructuredType = (StructuredType) newType;
            StructuredType oldStructuredType = (StructuredType) oldType;
            if (newStructuredType.getSuperType().isPresent()
                    != oldStructuredType.getSuperType().isPresent()) {
                return true;
            }
            if (newStructuredType.getSuperType().isPresent()
                    && hasNullabilityNarrowing(
                            newStructuredType.getSuperType().get(),
                            oldStructuredType.getSuperType().get())) {
                return true;
            }
        }

        List<LogicalType> newChildren = newType.getChildren();
        List<LogicalType> oldChildren = oldType.getChildren();
        if (newChildren.size() != oldChildren.size()) {
            return true;
        }
        for (int i = 0; i < newChildren.size(); i++) {
            if (hasNullabilityNarrowing(newChildren.get(i), oldChildren.get(i))) {
                return true;
            }
        }
        return false;
    }

    private static class NullabilityNormalizer extends LogicalTypeDuplicator {

        @Override
        public LogicalType visit(ArrayType arrayType) {
            return super.visit(arrayType).copy(true);
        }

        @Override
        public LogicalType visit(MultisetType multisetType) {
            return super.visit(multisetType).copy(true);
        }

        @Override
        public LogicalType visit(MapType mapType) {
            return super.visit(mapType).copy(true);
        }

        @Override
        public LogicalType visit(RowType rowType) {
            return super.visit(rowType).copy(true);
        }

        @Override
        public LogicalType visit(DistinctType distinctType) {
            return super.visit(distinctType).copy(true);
        }

        @Override
        public LogicalType visit(StructuredType structuredType) {
            return super.visit(structuredType).copy(true);
        }

        @Override
        protected LogicalType defaultMethod(LogicalType logicalType) {
            return logicalType.copy(true);
        }
    }
}
