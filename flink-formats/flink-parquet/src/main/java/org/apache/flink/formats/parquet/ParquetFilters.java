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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.formats.parquet.row.ParquetRowDataWriter.timestampToInt96;
import static org.apache.parquet.filter2.predicate.Operators.Column;

/**
 * Class that converts {@link Expression} to {@link FilterPredicate} to work with Parquet Filter
 * PushDown.
 */
public class ParquetFilters {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetFilters.class);

    private final ImmutableMap<String, Function<ParquetFilterCallExpression, FilterPredicate>>
            filters =
                    new ImmutableMap.Builder<
                                    String,
                                    Function<ParquetFilterCallExpression, FilterPredicate>>()
                            .put(
                                    BuiltInFunctionDefinitions.AND.getName(),
                                    call -> convertBinaryLogical(call, FilterApi::and))
                            .put(
                                    BuiltInFunctionDefinitions.OR.getName(),
                                    call -> convertBinaryLogical(call, FilterApi::or))
                            .put(BuiltInFunctionDefinitions.NOT.getName(), this::not)
                            .put(BuiltInFunctionDefinitions.IS_NULL.getName(), this::isNull)
                            .put(BuiltInFunctionDefinitions.IS_NOT_NULL.getName(), this::isNotNull)
                            .put(BuiltInFunctionDefinitions.IS_TRUE.getName(), this::isTrue)
                            .put(BuiltInFunctionDefinitions.IS_NOT_TRUE.getName(), this::isNotTrue)
                            .put(BuiltInFunctionDefinitions.IS_FALSE.getName(), this::isFalse)
                            .put(
                                    BuiltInFunctionDefinitions.IS_NOT_FALSE.getName(),
                                    this::isNotFalse)
                            .put(
                                    BuiltInFunctionDefinitions.EQUALS.getName(),
                                    call ->
                                            convertBinaryOperation(
                                                    call, ParquetFilters::eq, ParquetFilters::eq))
                            .put(
                                    BuiltInFunctionDefinitions.NOT_EQUALS.getName(),
                                    call ->
                                            convertBinaryOperation(
                                                    call,
                                                    ParquetFilters::notEq,
                                                    ParquetFilters::notEq))
                            .put(
                                    BuiltInFunctionDefinitions.LESS_THAN.getName(),
                                    call ->
                                            convertBinaryOperation(
                                                    call, ParquetFilters::lt, ParquetFilters::lt))
                            .put(
                                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.getName(),
                                    call ->
                                            convertBinaryOperation(
                                                    call,
                                                    ParquetFilters::ltEq,
                                                    ParquetFilters::ltEq))
                            .put(
                                    BuiltInFunctionDefinitions.GREATER_THAN.getName(),
                                    call ->
                                            convertBinaryOperation(
                                                    call, ParquetFilters::gt, ParquetFilters::gt))
                            .put(
                                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.getName(),
                                    call ->
                                            convertBinaryOperation(
                                                    call,
                                                    ParquetFilters::gtEq,
                                                    ParquetFilters::gtEq))
                            .build();

    private final boolean utcTimestamp;
    private final MessageType schema;

    public ParquetFilters(boolean utcTimestamp, MessageType schema) {
        this.utcTimestamp = utcTimestamp;
        this.schema = schema;
    }

    @VisibleForTesting
    FilterPredicate toParquetPredicate(Expression expression) {
        return toParquetPredicate(toParquetFilterExpression(expression));
    }

    public FilterPredicate toParquetPredicate(ParquetFilterExpression expression) {
        if (expression instanceof ParquetFilterCallExpression) {
            ParquetFilterCallExpression callExpression = (ParquetFilterCallExpression) expression;
            if (filters.get(callExpression.functionName) == null) {
                // unsupported predicate
                LOG.debug(
                        "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                        callExpression);
                return null;
            }
            return filters.get(callExpression.functionName).apply(callExpression);
        } else {
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    expression);
            return null;
        }
    }

    private FilterPredicate convertBinaryLogical(
            ParquetFilterExpression callExp,
            BiFunction<FilterPredicate, FilterPredicate, FilterPredicate> func) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        ParquetFilterExpression left = callExp.getChildren().get(0);
        ParquetFilterExpression right = callExp.getChildren().get(1);
        FilterPredicate c1 = toParquetPredicate(left);
        FilterPredicate c2 = toParquetPredicate(right);
        return (c1 == null || c2 == null) ? null : func.apply(c1, c2);
    }

    private FilterPredicate convertBinaryOperation(
            ParquetFilterCallExpression callExp,
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

    private FilterPredicate not(ParquetFilterExpression callExp) {
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

    private FilterPredicate isNull(ParquetFilterCallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into ParquetFileFormatFactory.",
                    callExp);
            return null;
        }
        String colName = getColumnName(callExp);
        DataType colType =
                ((ParquetFilterFieldReferenceExpression) callExp.getChildren().get(0))
                        .getOutputDataType();
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

    private FilterPredicate isNotNull(ParquetFilterCallExpression callExp) {
        FilterPredicate isNullPredicate = isNull(callExp);
        return isNullPredicate == null ? null : FilterApi.not(isNullPredicate);
    }

    private FilterPredicate isTrue(ParquetFilterCallExpression callExp) {
        DataType colType =
                ((ParquetFilterFieldReferenceExpression) callExp.getChildren().get(0))
                        .getOutputDataType();
        String colName = getColumnName(callExp);
        Column<?> column = getColumn(colName, colType);
        return eq(Tuple2.of(column, true));
    }

    private FilterPredicate isNotTrue(ParquetFilterCallExpression callExp) {
        return FilterApi.not(isTrue(callExp));
    }

    private FilterPredicate isFalse(ParquetFilterCallExpression callExp) {
        DataType colType =
                ((ParquetFilterFieldReferenceExpression) callExp.getChildren().get(0))
                        .getOutputDataType();
        String colName = getColumnName(callExp);
        Column<?> column = getColumn(colName, colType);
        return eq(Tuple2.of(column, false));
    }

    private FilterPredicate isNotFalse(ParquetFilterCallExpression callExp) {
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
        Comparable<?> literal =
                castToColumnType(castLiteral(colName, litType, literalValue), column);
        if (literal == null) {
            return null;
        }
        return Tuple2.of(column, literal);
    }

    /**
     * Return the {@link Column} for parquet format according to the column's name and data type.
     * Return null if encounter unsupported data type.
     */
    private Column<?> getColumn(String colName, DataType colType) {
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return FilterApi.binaryColumn(colName);
            case DECIMAL:
                PrimitiveType.PrimitiveTypeName typeName =
                        getColumnParquetType(colName).asPrimitiveType().getPrimitiveTypeName();
                switch (typeName) {
                    case INT32:
                        return FilterApi.intColumn(colName);
                    case INT64:
                        return FilterApi.longColumn(colName);
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        return FilterApi.binaryColumn(colName);
                    default:
                        LOG.warn(
                                "The column is declared as DECIMAL in Flink, but the actual type stored in parquet is %s,"
                                        + " which is not supported to do parquet filter. ");
                        return null;
                }
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
    private Comparable<?> castLiteral(String columnName, DataType litType, Object literal) {
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
                BigDecimal decimal = (BigDecimal) literal;
                PrimitiveType type = getColumnParquetType(columnName).asPrimitiveType();
                switch (type.getPrimitiveTypeName()) {
                    case INT32:
                        return decimal.unscaledValue().intValue();
                    case INT64:
                        return decimal.unscaledValue().longValue();
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        return castDecimalToBinary((BigDecimal) literal, type.getTypeLength());
                    default:
                        LOG.warn(
                                "The column {} is declared as DECIMAL in Flink, but the actual type stored in parquet is {},"
                                        + " which is not supported to do parquet filter. ",
                                columnName,
                                type);
                        return null;
                }
            case DATE:
                return ((LocalDate) literal).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                type = getColumnParquetType(columnName).asPrimitiveType();
                if (type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
                    return ((LocalTime) literal).toNanoOfDay() / 1_000_000L;
                } else {
                    // not to support filter push down for time annotated with INT64  since Flink's
                    // parquet format reader also doesn't support it
                    LOG.warn(
                            "The column {} is stored as parquet's type {},"
                                    + " which is not supported to do parquet filter. ",
                            columnName,
                            type);
                    return null;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                type = getColumnParquetType(columnName).asPrimitiveType();
                if (type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                    return timestampToInt96(
                            TimestampData.fromLocalDateTime((LocalDateTime) literal), utcTimestamp);
                } else {
                    // not to support filter push down for timestamp annotated with INT64/INT32
                    // since Flink's parquet format reader also doesn't support it
                    LOG.warn(
                            "The column {} is stored as parquet's type {},"
                                    + " which is not supported to do parquet filter. ",
                            columnName,
                            type);
                    return null;
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                type = getColumnParquetType(columnName).asPrimitiveType();
                if (type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                    return timestampToInt96(
                            TimestampData.fromInstant((Instant) literal), utcTimestamp);
                } else {
                    // not to support filter push down for timestamp annotated with INT64/INT32
                    // since Flink's parquet format reader also doesn't support it
                    LOG.warn(
                            "The column {} is stored as parquet's type {},"
                                    + " which is not supported to do parquet filter. ",
                            columnName,
                            type);
                    return null;
                }
            default:
                LOG.warn(
                        "Encounter an unsupported literal value {} to do parquet filter, whose data type is {}.",
                        literal,
                        ltype);
                return null;
        }
    }

    private Type getColumnParquetType(String colName) {
        return schema.getFields().get(schema.getFieldIndex(colName));
    }

    private Binary castDecimalToBinary(BigDecimal bigDecimal, int numBytes) {
        return Binary.fromConstantByteArray(decimalToBytes(bigDecimal, numBytes));
    }

    private byte[] decimalToBytes(BigDecimal bigDecimal, int numBytes) {
        byte[] decimalBuffer = new byte[numBytes];
        byte[] bytes = bigDecimal.unscaledValue().toByteArray();
        if (bytes.length == numBytes) {
            return bytes;
        } else {
            byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
            Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
            System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
            return decimalBuffer;
        }
    }

    private static Optional<?> getLiteral(ParquetFilterCallExpression comp) {
        if (literalOnRight(comp)) {
            ParquetFilterValueLiteralExpression valueLiteralExpression =
                    (ParquetFilterValueLiteralExpression) comp.getChildren().get(1);
            return Optional.ofNullable(valueLiteralExpression.value);
        } else {
            ParquetFilterValueLiteralExpression valueLiteralExpression =
                    (ParquetFilterValueLiteralExpression) comp.getChildren().get(0);
            return Optional.ofNullable(valueLiteralExpression.value);
        }
    }

    private static String getColumnName(ParquetFilterCallExpression comp) {
        if (literalOnRight(comp)) {
            return ((ParquetFilterFieldReferenceExpression) comp.getChildren().get(0)).name;
        } else {
            return ((ParquetFilterFieldReferenceExpression) comp.getChildren().get(1)).name;
        }
    }

    private static boolean isUnaryValid(ParquetFilterCallExpression callExpression) {
        return callExpression.getChildren().size() == 1
                && isRef(callExpression.getChildren().get(0));
    }

    private static boolean isBinaryValid(ParquetFilterCallExpression callExpression) {
        return callExpression.getChildren().size() == 2
                && (isRef(callExpression.getChildren().get(0))
                                && isLit(callExpression.getChildren().get(1))
                        || isLit(callExpression.getChildren().get(0))
                                && isRef(callExpression.getChildren().get(1)));
    }

    private static DataType getLiteralType(ParquetFilterCallExpression callExp) {
        if (literalOnRight(callExp)) {
            return ((ParquetFilterValueLiteralExpression) callExp.getChildren().get(1))
                    .getOutputDataType();
        } else {
            return ((ParquetFilterValueLiteralExpression) callExp.getChildren().get(0))
                    .getOutputDataType();
        }
    }

    private static DataType getColType(ParquetFilterCallExpression callExp) {
        if (literalOnRight(callExp)) {
            return ((ParquetFilterFieldReferenceExpression) callExp.getChildren().get(0))
                    .getOutputDataType();
        } else {
            return ((ParquetFilterFieldReferenceExpression) callExp.getChildren().get(1))
                    .getOutputDataType();
        }
    }

    private static boolean literalOnRight(ParquetFilterCallExpression comp) {
        if (comp.getChildren().size() == 1
                && comp.getChildren().get(0) instanceof ParquetFilterFieldReferenceExpression) {
            return true;
        } else if (isLit(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
            return false;
        } else if (isRef(comp.getChildren().get(0)) && isLit(comp.getChildren().get(1))) {
            return true;
        } else {
            throw new RuntimeException("Invalid binary comparison.");
        }
    }

    private static boolean isLit(ParquetFilterExpression expression) {
        return expression instanceof ParquetFilterValueLiteralExpression;
    }

    private static boolean isRef(ParquetFilterExpression expression) {
        return expression instanceof ParquetFilterFieldReferenceExpression;
    }

    /** A visitor to convert {@link Expression} to {@link ParquetFilterExpression}. */
    private static class ParquetExpressionVisitor
            implements ExpressionVisitor<ParquetFilterExpression> {

        @Override
        public ParquetFilterExpression visit(CallExpression call) {
            String functionName = call.getFunctionName();
            List<ParquetFilterExpression> args = new ArrayList<>();
            for (Expression expression : call.getChildren()) {
                args.add(expression.accept(this));
            }
            return new ParquetFilterCallExpression(functionName, args);
        }

        @Override
        public ParquetFilterExpression visit(ValueLiteralExpression valueLiteral) {
            return new ParquetFilterValueLiteralExpression(
                    valueLiteral
                            .getValueAs(valueLiteral.getOutputDataType().getConversionClass())
                            .orElse(null),
                    valueLiteral.getOutputDataType());
        }

        @Override
        public ParquetFilterExpression visit(FieldReferenceExpression fieldReference) {
            return new ParquetFilterFieldReferenceExpression(
                    fieldReference.getName(), fieldReference.getOutputDataType());
        }

        @Override
        public ParquetFilterExpression visit(TypeLiteralExpression typeLiteral) {
            return null;
        }

        @Override
        public ParquetFilterExpression visit(Expression other) {
            return null;
        }
    }

    /** Convert {@link Expression} to {@link ParquetFilterExpression}. */
    public static ParquetFilterExpression toParquetFilterExpression(Expression expression) {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            return callExp.accept(new ParquetExpressionVisitor());
        } else {
            return null;
        }
    }

    /**
     * Counterpart of {@link Expression}. The filter {@link Expression} to be pushed down isn't
     * serializable, so we can't keep it as a field in {@link ParquetColumnarRowInputFormat} which
     * requires all field to be serializable. Also, we can't keep the generated {@link
     * FilterPredicate} in {@link ParquetColumnarRowInputFormat} for the {@link FilterPredicate}
     * should be generated during running for we need to know the parquet's schema info which is
     * available in running is to create correct {@link FilterPredicate}.
     *
     * <p>So, we introduce the intermediate class which is serializable and keeps the needed
     * information to generate correct parquet's {@link FilterPredicate}.
     */
    public interface ParquetFilterExpression extends Serializable {
        default List<ParquetFilterExpression> getChildren() {
            return Collections.emptyList();
        }
    }

    /** Counterpart of {@link CallExpression}. */
    public static class ParquetFilterCallExpression implements ParquetFilterExpression {
        private final String functionName;
        private final List<ParquetFilterExpression> args;

        public ParquetFilterCallExpression(
                String functionName, List<ParquetFilterExpression> args) {
            this.functionName = functionName;
            this.args = args;
        }

        @Override
        public List<ParquetFilterExpression> getChildren() {
            return args;
        }
    }

    /** Counterpart of {@link ValueLiteralExpression}. */
    public static class ParquetFilterValueLiteralExpression implements ParquetFilterExpression {
        private final @Nullable Object value;
        private final DataType dataType;

        public ParquetFilterValueLiteralExpression(@Nullable Object value, DataType dataType) {
            this.value = value; // can be null
            this.dataType = dataType;
        }

        public DataType getOutputDataType() {
            return dataType;
        }
    }

    /** Counterpart of {@link FieldReferenceExpression}. */
    public static class ParquetFilterFieldReferenceExpression implements ParquetFilterExpression {
        private final String name;
        private final DataType dataType;

        public ParquetFilterFieldReferenceExpression(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        public DataType getOutputDataType() {
            return dataType;
        }

        public String getName() {
            return name;
        }
    }
}
