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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.formats.parquet.row.ParquetRowDataWriter.timestampToInt96;
import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.computeMinBytesForDecimalPrecision;
import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.is32BitDecimal;
import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.is64BitDecimal;
import static org.apache.parquet.filter2.predicate.Operators.Column;

/**
 * Class that converts {@link Expression} to {@link FilterPredicate} to work with Parquet Filter
 * PushDown.
 */
public class ParquetFilters {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetFilters.class);

    private final ImmutableMap<FunctionDefinition, Function<CallExpression, FilterPredicate>>
            filters =
                    new ImmutableMap.Builder<
                                    FunctionDefinition, Function<CallExpression, FilterPredicate>>()
                            .put(
                                    BuiltInFunctionDefinitions.AND,
                                    call -> convertBinaryLogical(call, FilterApi::and))
                            .put(
                                    BuiltInFunctionDefinitions.OR,
                                    call -> convertBinaryLogical(call, FilterApi::or))
                            .put(BuiltInFunctionDefinitions.NOT, this::not)
                            .put(BuiltInFunctionDefinitions.IS_NULL, this::isNull)
                            .put(BuiltInFunctionDefinitions.IS_NOT_NULL, this::isNotNull)
                            .put(BuiltInFunctionDefinitions.IS_TRUE, this::isTrue)
                            .put(BuiltInFunctionDefinitions.IS_NOT_TRUE, this::isNotTrue)
                            .put(BuiltInFunctionDefinitions.IS_FALSE, this::isFalse)
                            .put(BuiltInFunctionDefinitions.IS_NOT_FALSE, this::isNotFalse)
                            .put(
                                    BuiltInFunctionDefinitions.EQUALS,
                                    call ->
                                            convertBinaryOperation(
                                                    call, ParquetFilters::eq, ParquetFilters::eq))
                            .put(
                                    BuiltInFunctionDefinitions.NOT_EQUALS,
                                    call ->
                                            convertBinaryOperation(
                                                    call,
                                                    ParquetFilters::notEq,
                                                    ParquetFilters::notEq))
                            .put(
                                    BuiltInFunctionDefinitions.LESS_THAN,
                                    call ->
                                            convertBinaryOperation(
                                                    call, ParquetFilters::lt, ParquetFilters::lt))
                            .put(
                                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                    call ->
                                            convertBinaryOperation(
                                                    call,
                                                    ParquetFilters::ltEq,
                                                    ParquetFilters::ltEq))
                            .put(
                                    BuiltInFunctionDefinitions.GREATER_THAN,
                                    call ->
                                            convertBinaryOperation(
                                                    call, ParquetFilters::gt, ParquetFilters::gt))
                            .put(
                                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                    call ->
                                            convertBinaryOperation(
                                                    call,
                                                    ParquetFilters::gtEq,
                                                    ParquetFilters::gtEq))
                            .build();

    private final boolean utcTimestamp;

    public ParquetFilters(boolean utcTimestamp) {
        this.utcTimestamp = utcTimestamp;
    }

    public FilterPredicate toParquetPredicate(Expression expression) {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            if (filters.get(callExp.getFunctionDefinition()) == null) {
                // unsupported predicate
                LOG.debug(
                        "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                        expression);
                return null;
            }
            return filters.get(callExp.getFunctionDefinition()).apply(callExp);
        } else {
            // unsupported predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    expression);
            return null;
        }
    }

    private FilterPredicate convertBinaryLogical(
            CallExpression callExp,
            BiFunction<FilterPredicate, FilterPredicate, FilterPredicate> func) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        Expression left = callExp.getChildren().get(0);
        Expression right = callExp.getChildren().get(1);
        FilterPredicate c1 = toParquetPredicate(left);
        FilterPredicate c2 = toParquetPredicate(right);
        return (c1 == null || c2 == null) ? null : func.apply(c1, c2);
    }

    private FilterPredicate convertBinaryOperation(
            CallExpression callExp,
            Function<Tuple2<Column<?>, Comparable<?>>, FilterPredicate> func,
            Function<Tuple2<Column<?>, Comparable<?>>, FilterPredicate> reverseFunc) {
        if (!isBinaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported  predicate[{}] cannot be pushed into ParquetFileFormatFactory.",
                    callExp);
            return null;
        }
        String colName = getColumnName(callExp);
        DataType colType = getColType(callExp);
        Optional<?> optionalLiteral = getLiteral(callExp);
        DataType litType = getLiteralType(callExp);
        Tuple2<Column<?>, Comparable<?>> columnLiteralPair;
        if (optionalLiteral.isPresent()) {
            columnLiteralPair =
                    getColumnLiteralPair(colName, colType, litType, optionalLiteral.get());
        } else {
            columnLiteralPair = Tuple2.of(getColumn(colName, colType), null);
        }
        if (columnLiteralPair == null) {
            // unsupported type
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    callExp);
            return null;
        }
        return literalOnRight(callExp)
                ? func.apply(columnLiteralPair)
                : reverseFunc.apply(columnLiteralPair);
    }

    private FilterPredicate not(CallExpression callExp) {
        if (callExp.getChildren().size() != 1) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    callExp);
            return null;
        }
        FilterPredicate predicate = toParquetPredicate(callExp.getChildren().get(0));
        return predicate != null ? FilterApi.not(predicate) : null;
    }

    private FilterPredicate isNull(CallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    callExp);
            return null;
        }
        String colName = getColumnName(callExp);
        DataType colType =
                ((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType();
        if (colType == null) {
            // unsupported type
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.",
                    callExp);
            return null;
        }
        Column<?> column = getColumn(colName, colType);
        if (column == null) {
            // unsupported type
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    callExp);
            return null;
        }
        return eq(new Tuple2<>(column, null));
    }

    private FilterPredicate isNotNull(CallExpression callExp) {
        FilterPredicate isNullPredicate = isNull(callExp);
        return isNullPredicate == null ? null : FilterApi.not(isNullPredicate);
    }

    private FilterPredicate isTrue(CallExpression callExp) {
        DataType colType =
                ((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType();
        String colName = getColumnName(callExp);
        Column<?> column = getColumn(colName, colType);
        return eq(Tuple2.of(column, true));
    }

    private FilterPredicate isNotTrue(CallExpression callExp) {
        return FilterApi.not(isTrue(callExp));
    }

    private FilterPredicate isFalse(CallExpression callExp) {
        DataType colType =
                ((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType();
        String colName = getColumnName(callExp);
        Column<?> column = getColumn(colName, colType);
        return eq(Tuple2.of(column, false));
    }

    private FilterPredicate isNotFalse(CallExpression callExp) {
        return FilterApi.not(isFalse(callExp));
    }

    private static FilterPredicate eq(Tuple2<Column<?>, Comparable<?>> columnPair) {
        Column<?> column = columnPair.f0;
        if (!(column instanceof Operators.SupportsEqNotEq)) {
            LOG.warn(
                    "Unsupported equal filter for column {}, type {}.",
                    column.getColumnPath(),
                    column.getColumnType());
            return null;
        }
        return FilterApi.eq(
                (Column & Operators.SupportsEqNotEq) column, (Comparable) columnPair.f1);
    }

    private static FilterPredicate notEq(Tuple2<Column<?>, Comparable<?>> columnPair) {
        Column<?> column = columnPair.f0;
        if (!(column instanceof Operators.SupportsEqNotEq)) {
            LOG.warn(
                    "Unsupported not equal filter for column {}, type {}.",
                    column.getColumnPath(),
                    column.getColumnType());
            return null;
        }
        return FilterApi.notEq(
                (Column & Operators.SupportsEqNotEq) column, (Comparable) columnPair.f1);
    }

    private static FilterPredicate lt(Tuple2<Column<?>, Comparable<?>> columnPair) {
        Column<?> column = columnPair.f0;
        if (!(column instanceof Operators.SupportsLtGt)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported less than filter for column: %s.",
                            column.getColumnPath()));
        }
        return FilterApi.lt((Column & Operators.SupportsLtGt) column, (Comparable) columnPair.f1);
    }

    private static FilterPredicate ltEq(Tuple2<Column<?>, Comparable<?>> columnPair) {
        Column<?> column = columnPair.f0;
        if (!(column instanceof Operators.SupportsLtGt)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported less than or equal filter for column: %s.",
                            column.getColumnPath()));
        }
        return FilterApi.ltEq((Column & Operators.SupportsLtGt) column, (Comparable) columnPair.f1);
    }

    private static FilterPredicate gt(Tuple2<Column<?>, Comparable<?>> columnPair) {
        Column<?> column = columnPair.f0;
        if (!(column instanceof Operators.SupportsLtGt)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported greater than filter for column: %s.",
                            column.getColumnPath()));
        }
        return FilterApi.gt((Column & Operators.SupportsLtGt) column, (Comparable) columnPair.f1);
    }

    private static FilterPredicate gtEq(Tuple2<Column<?>, Comparable<?>> columnPair) {
        Column<?> column = columnPair.f0;
        if (!(column instanceof Operators.SupportsLtGt)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported greater than or equal filter for column: %s.",
                            column.getColumnPath()));
        }
        return FilterApi.gtEq((Column & Operators.SupportsLtGt) column, (Comparable) columnPair.f1);
    }

    /** Get the tuple (Column, Comparable) required to construct the FilterPredicate. */
    private Tuple2<Column<?>, Comparable<?>> getColumnLiteralPair(
            String colName, DataType colType, DataType litType, Object literalValue) {
        Column<?> column = getColumn(colName, colType);
        if (column == null) {
            return null;
        }

        // we first cast the literal to the value expected by parquet filter, then cast the value
        // to make it match the column type. the reason is in some case, the literal's type maybe
        // int, but the col's type is float
        Comparable<?> literal = castToColumnType(castLiteral(litType, literalValue), column);
        if (literal == null) {
            return null;
        }
        return Tuple2.of(column, literal);
    }

    /**
     * Return the {@link Column} for parquet format according to the column's name and data type.
     * Return null if encounter unsupported data type.
     */
    private static Column<?> getColumn(String colName, DataType colType) {
        LogicalTypeRoot ltype = colType.getLogicalType().getTypeRoot();
        switch (ltype) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return FilterApi.intColumn(colName);
            case BIGINT:
                return FilterApi.longColumn(colName);
            case FLOAT:
                return FilterApi.floatColumn(colName);
            case DOUBLE:
                return FilterApi.doubleColumn(colName);
            case BOOLEAN:
                return FilterApi.booleanColumn(colName);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return FilterApi.binaryColumn(colName);
            default:
                LOG.warn(
                        "Unsupported filter data type {} for column {} in parquet format .",
                        ltype,
                        colName);
                return null;
        }
    }

    /**
     * Cast the value used in parquet's filter to make it match the type of the corresponding
     * parquet's {@link Column}. Return null if cast fails.
     */
    private Comparable<?> castToColumnType(Comparable<?> literal, Column<?> column) {
        if (column instanceof Operators.IntColumn) {
            return literal instanceof Number ? ((Number) literal).intValue() : null;
        } else if (column instanceof Operators.LongColumn) {
            return literal instanceof Number ? ((Number) literal).longValue() : null;
        } else if (column instanceof Operators.FloatColumn) {
            return literal instanceof Number ? ((Number) literal).floatValue() : null;
        } else if (column instanceof Operators.DoubleColumn) {
            return literal instanceof Number ? ((Number) literal).doubleValue() : null;
        } else if (column instanceof Operators.BooleanColumn) {
            return literal instanceof Boolean ? literal : null;
        } else if (column instanceof Operators.BinaryColumn) {
            return literal instanceof Binary ? literal : null;
        }
        return literal;
    }

    /**
     * Cast the literal value to the corresponding value needed in parquet filter.
     *
     * @param litType the data type of the literal value
     * @param literal the literal value of the column needed to cast
     * @return the cast value, return null if the data type is not supported to do parquet filter.
     */
    private Comparable<?> castLiteral(DataType litType, Object literal) {
        LogicalTypeRoot ltype = litType.getLogicalType().getTypeRoot();
        switch (ltype) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ((Number) literal).intValue();
            case BIGINT:
                return ((Number) literal).longValue();
            case FLOAT:
                return ((Number) literal).floatValue();
            case DOUBLE:
                return ((Number) literal).doubleValue();
            case BOOLEAN:
                return (Boolean) literal;
            case CHAR:
            case VARCHAR:
                return Binary.fromString((String) literal);
            case BINARY:
            case VARBINARY:
                return Binary.fromConstantByteArray((byte[]) literal);
            case DECIMAL:
                final int precision = ((DecimalType) litType.getLogicalType()).getPrecision();
                final int scale = ((DecimalType) litType.getLogicalType()).getScale();
                final DecimalData value =
                        DecimalData.fromBigDecimal((BigDecimal) literal, precision, scale);
                if (value == null) {
                    LOG.warn("The precision overflows for decimal {} in parquet format.", literal);
                    return null;
                }
                return castDecimalToBinary(value);
            case DATE:
                return ((LocalDate) literal).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return ((LocalTime) literal).toNanoOfDay() / 1_000_000L;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampToInt96(
                        TimestampData.fromLocalDateTime((LocalDateTime) literal), utcTimestamp);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampToInt96(TimestampData.fromInstant((Instant) literal), utcTimestamp);
            default:
                LOG.warn(
                        "Encounter an unsupported literal value {} to do parquet filter, whose data type is {}.",
                        literal,
                        ltype);
                return null;
        }
    }

    private Binary castDecimalToBinary(DecimalData decimalData) {
        int precision = decimalData.precision();
        int numBytes = computeMinBytesForDecimalPrecision(precision);
        byte[] decimalBytes;
        // 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
        // optimizer for UnscaledBytesWriter
        if (is32BitDecimal(precision) || is64BitDecimal(precision)) {
            decimalBytes = longUnscaledBytesEncode(decimalData, numBytes);
        } else {
            // 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
            decimalBytes = unscaledBytesEncode(decimalData, numBytes);
        }
        return Binary.fromConstantByteArray(decimalBytes, 0, numBytes);
    }

    private byte[] longUnscaledBytesEncode(DecimalData decimalData, int numBytes) {
        byte[] decimalBuffer = new byte[numBytes];
        int initShift = 8 * (numBytes - 1);
        long unscaledLong = decimalData.toUnscaledLong();
        int i = 0;
        int shift = initShift;
        while (i < numBytes) {
            decimalBuffer[i] = (byte) (unscaledLong >> shift);
            i += 1;
            shift -= 8;
        }
        return decimalBuffer;
    }

    private byte[] unscaledBytesEncode(DecimalData decimalData, int numBytes) {
        byte[] decimalBuffer = new byte[numBytes];
        byte[] bytes = decimalData.toUnscaledBytes();
        if (bytes.length == numBytes) {
            // Avoid copy.
            return decimalData.toUnscaledBytes();
        } else {
            byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
            Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
            System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
            return decimalBuffer;
        }
    }

    private static Optional<?> getLiteral(CallExpression comp) {
        if (literalOnRight(comp)) {
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) comp.getChildren().get(1);
            return valueLiteralExpression.getValueAs(
                    valueLiteralExpression.getOutputDataType().getConversionClass());
        } else {
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) comp.getChildren().get(0);
            return valueLiteralExpression.getValueAs(
                    valueLiteralExpression.getOutputDataType().getConversionClass());
        }
    }

    private static String getColumnName(CallExpression comp) {
        if (literalOnRight(comp)) {
            return ((FieldReferenceExpression) comp.getChildren().get(0)).getName();
        } else {
            return ((FieldReferenceExpression) comp.getChildren().get(1)).getName();
        }
    }

    private static boolean isUnaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 1
                && isRef(callExpression.getChildren().get(0));
    }

    private static boolean isBinaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 2
                && (isRef(callExpression.getChildren().get(0))
                                && isLit(callExpression.getChildren().get(1))
                        || isLit(callExpression.getChildren().get(0))
                                && isRef(callExpression.getChildren().get(1)));
    }

    private static DataType getLiteralType(CallExpression callExp) {
        if (literalOnRight(callExp)) {
            return ((ValueLiteralExpression) callExp.getChildren().get(1)).getOutputDataType();
        } else {
            return ((ValueLiteralExpression) callExp.getChildren().get(0)).getOutputDataType();
        }
    }

    private static DataType getColType(CallExpression callExp) {
        if (literalOnRight(callExp)) {
            return ((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType();
        } else {
            return ((FieldReferenceExpression) callExp.getChildren().get(1)).getOutputDataType();
        }
    }

    private static boolean literalOnRight(CallExpression comp) {
        if (comp.getChildren().size() == 1
                && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
            return true;
        } else if (isLit(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
            return false;
        } else if (isRef(comp.getChildren().get(0)) && isLit(comp.getChildren().get(1))) {
            return true;
        } else {
            throw new RuntimeException("Invalid binary comparison.");
        }
    }

    private static boolean isLit(Expression expression) {
        return expression instanceof ValueLiteralExpression;
    }

    private static boolean isRef(Expression expression) {
        return expression instanceof FieldReferenceExpression;
    }
}
