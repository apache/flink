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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.utils.DateTimeUtils;

import com.google.common.collect.BoundType;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonExistsErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlTrimFunction;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities to map between symbols from both Calcite and Flink. It also defines a {@link
 * SerializableSymbol} format independent of concrete implementation classes.
 */
@Internal
public final class SymbolUtil {

    private static final Map<Class<?>, String> calciteToSymbolKind = new HashMap<>();
    private static final Map<SerializableSymbol, Enum<?>> serializableToCalcite = new HashMap<>();
    private static final Map<Enum<?>, SerializableSymbol> calciteToSerializable = new HashMap<>();
    private static final Map<Enum<?>, Enum<?>> calciteToCommon = new HashMap<>();
    private static final Map<Enum<?>, Enum<?>> commonToCalcite = new HashMap<>();
    private static final Map<Enum<?>, Enum<?>> calciteToInternalCommon = new HashMap<>();
    private static final Map<Enum<?>, Enum<?>> internalCommonToCalcite = new HashMap<>();

    static {
        // TRIM
        addSymbolMapping(null, null, SqlTrimFunction.Flag.BOTH, "TRIM", "BOTH");
        addSymbolMapping(null, null, SqlTrimFunction.Flag.LEADING, "TRIM", "LEADING");
        addSymbolMapping(null, null, SqlTrimFunction.Flag.TRAILING, "TRIM", "TRAILING");

        // JSON_EXISTS_ON_ERROR
        addSymbolMapping(
                JsonExistsOnError.TRUE,
                null,
                SqlJsonExistsErrorBehavior.TRUE,
                "JSON_EXISTS_ON_ERROR",
                "TRUE");
        addSymbolMapping(
                JsonExistsOnError.FALSE,
                null,
                SqlJsonExistsErrorBehavior.FALSE,
                "JSON_EXISTS_ON_ERROR",
                "FALSE");
        addSymbolMapping(
                JsonExistsOnError.UNKNOWN,
                null,
                SqlJsonExistsErrorBehavior.UNKNOWN,
                "JSON_EXISTS_ON_ERROR",
                "UNKNOWN");
        addSymbolMapping(
                JsonExistsOnError.ERROR,
                null,
                SqlJsonExistsErrorBehavior.ERROR,
                "JSON_EXISTS_ON_ERROR",
                "ERROR");

        // JSON_VALUE_ON_EMPTY_OR_ERROR
        addSymbolMapping(
                JsonValueOnEmptyOrError.ERROR,
                null,
                SqlJsonValueEmptyOrErrorBehavior.ERROR,
                "JSON_VALUE_ON_EMPTY_OR_ERROR",
                "ERROR");
        addSymbolMapping(
                JsonValueOnEmptyOrError.NULL,
                null,
                SqlJsonValueEmptyOrErrorBehavior.NULL,
                "JSON_VALUE_ON_EMPTY_OR_ERROR",
                "NULL");
        addSymbolMapping(
                JsonValueOnEmptyOrError.DEFAULT,
                null,
                SqlJsonValueEmptyOrErrorBehavior.DEFAULT,
                "JSON_VALUE_ON_EMPTY_OR_ERROR",
                "DEFAULT");

        // JSON_QUERY_WRAPPER
        addSymbolMapping(
                JsonQueryWrapper.WITHOUT_ARRAY,
                null,
                SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY,
                "JSON_QUERY_WRAPPER",
                "WITHOUT_ARRAY");
        addSymbolMapping(
                JsonQueryWrapper.CONDITIONAL_ARRAY,
                null,
                SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
                "JSON_QUERY_WRAPPER",
                "CONDITIONAL_ARRAY");
        addSymbolMapping(
                JsonQueryWrapper.UNCONDITIONAL_ARRAY,
                null,
                SqlJsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
                "JSON_QUERY_WRAPPER",
                "WITH_UNCONDITIONAL_ARRAY");

        // JSON_QUERY_ON_EMPTY_OR_ERROR
        addSymbolMapping(
                JsonQueryOnEmptyOrError.ERROR,
                null,
                SqlJsonQueryEmptyOrErrorBehavior.ERROR,
                "JSON_QUERY_ON_EMPTY_OR_ERROR",
                "ERROR");
        addSymbolMapping(
                JsonQueryOnEmptyOrError.NULL,
                null,
                SqlJsonQueryEmptyOrErrorBehavior.NULL,
                "JSON_QUERY_ON_EMPTY_OR_ERROR",
                "NULL");
        addSymbolMapping(
                JsonQueryOnEmptyOrError.EMPTY_ARRAY,
                null,
                SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                "JSON_QUERY_ON_EMPTY_OR_ERROR",
                "EMPTY_ARRAY");
        addSymbolMapping(
                JsonQueryOnEmptyOrError.EMPTY_OBJECT,
                null,
                SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                "JSON_QUERY_ON_EMPTY_OR_ERROR",
                "EMPTY_OBJECT");

        // JSON_ON_NULL
        addSymbolMapping(
                JsonOnNull.NULL,
                null,
                SqlJsonConstructorNullClause.NULL_ON_NULL,
                "JSON_ON_NULL",
                "NULL");
        addSymbolMapping(
                JsonOnNull.ABSENT,
                null,
                SqlJsonConstructorNullClause.ABSENT_ON_NULL,
                "JSON_ON_NULL",
                "ABSENT");

        // JSON_EMPTY_OR_ERROR
        addSymbolMapping(null, null, SqlJsonEmptyOrError.EMPTY, "JSON_EMPTY_OR_ERROR", "EMPTY");
        addSymbolMapping(null, null, SqlJsonEmptyOrError.ERROR, "JSON_EMPTY_OR_ERROR", "ERROR");

        // TIME_UNIT_RANGE
        addSymbolMapping(
                TimeIntervalUnit.MILLENNIUM,
                DateTimeUtils.TimeUnitRange.MILLENNIUM,
                TimeUnitRange.MILLENNIUM,
                "TIME_UNIT_RANGE",
                "MILLENNIUM");
        addSymbolMapping(
                TimeIntervalUnit.CENTURY,
                DateTimeUtils.TimeUnitRange.CENTURY,
                TimeUnitRange.CENTURY,
                "TIME_UNIT_RANGE",
                "CENTURY");
        addSymbolMapping(
                TimeIntervalUnit.DECADE,
                DateTimeUtils.TimeUnitRange.DECADE,
                TimeUnitRange.DECADE,
                "TIME_UNIT_RANGE",
                "DECADE");
        addSymbolMapping(
                TimeIntervalUnit.YEAR,
                DateTimeUtils.TimeUnitRange.YEAR,
                TimeUnitRange.YEAR,
                "TIME_UNIT_RANGE",
                "YEAR");
        addSymbolMapping(
                TimeIntervalUnit.YEAR_TO_MONTH,
                DateTimeUtils.TimeUnitRange.YEAR_TO_MONTH,
                TimeUnitRange.YEAR_TO_MONTH,
                "TIME_UNIT_RANGE",
                "YEAR_TO_MONTH");
        addSymbolMapping(
                TimeIntervalUnit.MONTH,
                DateTimeUtils.TimeUnitRange.MONTH,
                TimeUnitRange.MONTH,
                "TIME_UNIT_RANGE",
                "MONTH");
        addSymbolMapping(
                TimeIntervalUnit.DAY,
                DateTimeUtils.TimeUnitRange.DAY,
                TimeUnitRange.DAY,
                "TIME_UNIT_RANGE",
                "DAY");
        addSymbolMapping(
                TimeIntervalUnit.DAY_TO_HOUR,
                DateTimeUtils.TimeUnitRange.DAY_TO_HOUR,
                TimeUnitRange.DAY_TO_HOUR,
                "TIME_UNIT_RANGE",
                "DAY_TO_HOUR");
        addSymbolMapping(
                TimeIntervalUnit.DAY_TO_MINUTE,
                DateTimeUtils.TimeUnitRange.DAY_TO_MINUTE,
                TimeUnitRange.DAY_TO_MINUTE,
                "TIME_UNIT_RANGE",
                "DAY_TO_MINUTE");
        addSymbolMapping(
                TimeIntervalUnit.DAY_TO_SECOND,
                DateTimeUtils.TimeUnitRange.DAY_TO_SECOND,
                TimeUnitRange.DAY_TO_SECOND,
                "TIME_UNIT_RANGE",
                "DAY_TO_SECOND");
        addSymbolMapping(
                TimeIntervalUnit.HOUR,
                DateTimeUtils.TimeUnitRange.HOUR,
                TimeUnitRange.HOUR,
                "TIME_UNIT_RANGE",
                "HOUR");
        addSymbolMapping(
                TimeIntervalUnit.HOUR_TO_MINUTE,
                DateTimeUtils.TimeUnitRange.HOUR_TO_MINUTE,
                TimeUnitRange.HOUR_TO_MINUTE,
                "TIME_UNIT_RANGE",
                "HOUR_TO_MINUTE");
        addSymbolMapping(
                TimeIntervalUnit.HOUR_TO_SECOND,
                DateTimeUtils.TimeUnitRange.HOUR_TO_SECOND,
                TimeUnitRange.HOUR_TO_SECOND,
                "TIME_UNIT_RANGE",
                "HOUR_TO_SECOND");
        addSymbolMapping(
                TimeIntervalUnit.MINUTE,
                DateTimeUtils.TimeUnitRange.MINUTE,
                TimeUnitRange.MINUTE,
                "TIME_UNIT_RANGE",
                "MINUTE");
        addSymbolMapping(
                TimeIntervalUnit.MINUTE_TO_SECOND,
                DateTimeUtils.TimeUnitRange.MINUTE_TO_SECOND,
                TimeUnitRange.MINUTE_TO_SECOND,
                "TIME_UNIT_RANGE",
                "MINUTE_TO_SECOND");
        addSymbolMapping(
                TimeIntervalUnit.SECOND,
                DateTimeUtils.TimeUnitRange.SECOND,
                TimeUnitRange.SECOND,
                "TIME_UNIT_RANGE",
                "SECOND");
        addSymbolMapping(
                TimeIntervalUnit.MILLISECOND,
                DateTimeUtils.TimeUnitRange.MILLISECOND,
                TimeUnitRange.MILLISECOND,
                "TIME_UNIT_RANGE",
                "MILLISECOND");
        addSymbolMapping(
                TimeIntervalUnit.MICROSECOND,
                DateTimeUtils.TimeUnitRange.MICROSECOND,
                TimeUnitRange.MICROSECOND,
                "TIME_UNIT_RANGE",
                "MICROSECOND");
        addSymbolMapping(
                TimeIntervalUnit.NANOSECOND,
                DateTimeUtils.TimeUnitRange.NANOSECOND,
                TimeUnitRange.NANOSECOND,
                "TIME_UNIT_RANGE",
                "NANOSECOND");
        addSymbolMapping(
                TimeIntervalUnit.EPOCH,
                DateTimeUtils.TimeUnitRange.EPOCH,
                TimeUnitRange.EPOCH,
                "TIME_UNIT_RANGE",
                "EPOCH");
        addSymbolMapping(
                TimeIntervalUnit.QUARTER,
                DateTimeUtils.TimeUnitRange.QUARTER,
                TimeUnitRange.QUARTER,
                "TIME_UNIT_RANGE",
                "QUARTER");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.ISOYEAR,
                TimeUnitRange.ISOYEAR,
                "TIME_UNIT_RANGE",
                "ISOYEAR");
        addSymbolMapping(
                TimeIntervalUnit.WEEK,
                DateTimeUtils.TimeUnitRange.WEEK,
                TimeUnitRange.WEEK,
                "TIME_UNIT_RANGE",
                "WEEK");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.MILLISECOND,
                TimeUnitRange.MILLISECOND,
                "TIME_UNIT_RANGE",
                "MILLISECOND");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.MICROSECOND,
                TimeUnitRange.MICROSECOND,
                "TIME_UNIT_RANGE",
                "MICROSECOND");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.NANOSECOND,
                TimeUnitRange.NANOSECOND,
                "TIME_UNIT_RANGE",
                "NANOSECOND");
        addSymbolMapping(
                null, DateTimeUtils.TimeUnitRange.DOW, TimeUnitRange.DOW, "TIME_UNIT_RANGE", "DOW");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.ISODOW,
                TimeUnitRange.ISODOW,
                "TIME_UNIT_RANGE",
                "ISODOW");
        addSymbolMapping(
                null, DateTimeUtils.TimeUnitRange.DOY, TimeUnitRange.DOY, "TIME_UNIT_RANGE", "DOY");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.EPOCH,
                TimeUnitRange.EPOCH,
                "TIME_UNIT_RANGE",
                "EPOCH");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.DECADE,
                TimeUnitRange.DECADE,
                "TIME_UNIT_RANGE",
                "DECADE");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.CENTURY,
                TimeUnitRange.CENTURY,
                "TIME_UNIT_RANGE",
                "CENTURY");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnitRange.MILLENNIUM,
                TimeUnitRange.MILLENNIUM,
                "TIME_UNIT_RANGE",
                "MILLENNIUM");

        // TIME_UNIT
        addSymbolMapping(
                TimePointUnit.YEAR,
                DateTimeUtils.TimeUnit.YEAR,
                TimeUnit.YEAR,
                "TIME_UNIT",
                "YEAR");
        addSymbolMapping(
                TimePointUnit.MONTH,
                DateTimeUtils.TimeUnit.MONTH,
                TimeUnit.MONTH,
                "TIME_UNIT",
                "MONTH");
        addSymbolMapping(
                TimePointUnit.DAY, DateTimeUtils.TimeUnit.DAY, TimeUnit.DAY, "TIME_UNIT", "DAY");
        addSymbolMapping(
                TimePointUnit.HOUR,
                DateTimeUtils.TimeUnit.HOUR,
                TimeUnit.HOUR,
                "TIME_UNIT",
                "HOUR");
        addSymbolMapping(
                TimePointUnit.MINUTE,
                DateTimeUtils.TimeUnit.MINUTE,
                TimeUnit.MINUTE,
                "TIME_UNIT",
                "MINUTE");
        addSymbolMapping(
                TimePointUnit.SECOND,
                DateTimeUtils.TimeUnit.SECOND,
                TimeUnit.SECOND,
                "TIME_UNIT",
                "SECOND");
        addSymbolMapping(
                TimePointUnit.QUARTER,
                DateTimeUtils.TimeUnit.QUARTER,
                TimeUnit.QUARTER,
                "TIME_UNIT",
                "QUARTER");
        addSymbolMapping(
                TimePointUnit.WEEK,
                DateTimeUtils.TimeUnit.WEEK,
                TimeUnit.WEEK,
                "TIME_UNIT",
                "WEEK");
        addSymbolMapping(
                TimePointUnit.MILLISECOND,
                DateTimeUtils.TimeUnit.MILLISECOND,
                TimeUnit.MILLISECOND,
                "TIME_UNIT",
                "MILLISECOND");
        addSymbolMapping(
                TimePointUnit.MICROSECOND,
                DateTimeUtils.TimeUnit.MICROSECOND,
                TimeUnit.MICROSECOND,
                "TIME_UNIT",
                "MICROSECOND");
        addSymbolMapping(null, DateTimeUtils.TimeUnit.DOW, TimeUnit.DOW, "TIME_UNIT", "DOW");
        addSymbolMapping(null, DateTimeUtils.TimeUnit.DOY, TimeUnit.DOY, "TIME_UNIT", "DOY");
        addSymbolMapping(null, DateTimeUtils.TimeUnit.EPOCH, TimeUnit.EPOCH, "TIME_UNIT", "EPOCH");
        addSymbolMapping(
                null, DateTimeUtils.TimeUnit.DECADE, TimeUnit.DECADE, "TIME_UNIT", "DECADE");
        addSymbolMapping(
                null, DateTimeUtils.TimeUnit.CENTURY, TimeUnit.CENTURY, "TIME_UNIT", "CENTURY");
        addSymbolMapping(
                null,
                DateTimeUtils.TimeUnit.MILLENNIUM,
                TimeUnit.MILLENNIUM,
                "TIME_UNIT",
                "MILLENNIUM");

        // MATCH_RECOGNIZE_AFTER_OPTION
        addSymbolMapping(
                null,
                null,
                SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW,
                "MATCH_RECOGNIZE_AFTER_OPTION",
                "SKIP_TO_NEXT_ROW");
        addSymbolMapping(
                null,
                null,
                SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW,
                "MATCH_RECOGNIZE_AFTER_OPTION",
                "SKIP_PAST_LAST_ROW");

        // SYNTAX
        addSymbolMapping(null, null, SqlSyntax.FUNCTION, "SYNTAX", "FUNCTION");
        addSymbolMapping(null, null, SqlSyntax.FUNCTION_STAR, "SYNTAX", "FUNCTION_STAR");
        addSymbolMapping(null, null, SqlSyntax.BINARY, "SYNTAX", "BINARY");
        addSymbolMapping(null, null, SqlSyntax.PREFIX, "SYNTAX", "PREFIX");
        addSymbolMapping(null, null, SqlSyntax.POSTFIX, "SYNTAX", "POSTFIX");
        addSymbolMapping(null, null, SqlSyntax.SPECIAL, "SYNTAX", "SPECIAL");
        addSymbolMapping(null, null, SqlSyntax.FUNCTION_ID, "SYNTAX", "FUNCTION_ID");
        addSymbolMapping(null, null, SqlSyntax.INTERNAL, "SYNTAX", "INTERNAL");

        // BOUND
        addSymbolMapping(null, null, BoundType.OPEN, "BOUND", "OPEN");
        addSymbolMapping(null, null, BoundType.CLOSED, "BOUND", "CLOSED");

        // UNKNOWN_AS
        addSymbolMapping(null, null, RexUnknownAs.TRUE, "UNKNOWN_AS", "TRUE");
        addSymbolMapping(null, null, RexUnknownAs.FALSE, "UNKNOWN_AS", "FALSE");
        addSymbolMapping(null, null, RexUnknownAs.UNKNOWN, "UNKNOWN_AS", "UNKNOWN");
    }

    /**
     * Converts from a common to a Calcite symbol. The common symbol can be a publicly exposed one
     * such as {@link TimeIntervalUnit} or internal one such as {@link DateTimeUtils.TimeUnitRange}.
     */
    public static Enum<?> commonToCalcite(Enum<?> commonSymbol) {
        checkCommonSymbol(commonSymbol);
        Enum<?> calciteSymbol = commonToCalcite.get(commonSymbol);
        if (calciteSymbol == null) {
            calciteSymbol = internalCommonToCalcite.get(commonSymbol);
            if (calciteSymbol == null) {
                throw new UnsupportedOperationException(
                        String.format("Cannot map '%s' to an internal symbol.", commonSymbol));
            }
        }
        return calciteSymbol;
    }

    /**
     * Converts from Calcite to a common symbol. The common symbol can be a publicly exposed one
     * such as {@link TimeIntervalUnit} or internal one such as {@link DateTimeUtils.TimeUnitRange}.
     * Since the common symbol is optional, the input is returned as a fallback.
     */
    public static Enum<?> calciteToCommon(Enum<?> calciteSymbol, boolean preferInternal) {
        checkCalciteSymbol(calciteSymbol);
        Enum<?> internalCommonSymbol =
                preferInternal ? calciteToInternalCommon.get(calciteSymbol) : null;
        if (internalCommonSymbol == null) {
            internalCommonSymbol = calciteToCommon.get(calciteSymbol);
            if (internalCommonSymbol == null) {
                // for cases that have no common representation
                // e.g. TRIM
                return calciteSymbol;
            }
        }
        return internalCommonSymbol;
    }

    public static SerializableSymbol calciteToSerializable(Enum<?> calciteSymbol) {
        final SerializableSymbol serializableSymbol = calciteToSerializable.get(calciteSymbol);
        if (serializableSymbol == null) {
            throw new TableException(
                    String.format(
                            "Symbol class '%s' has no serializable representation.",
                            calciteSymbol.getClass().getName()));
        }
        return serializableSymbol;
    }

    public static Enum<?> serializableToCalcite(SerializableSymbol serializableSymbol) {
        final Enum<?> calciteSymbol = serializableToCalcite.get(serializableSymbol);
        if (calciteSymbol == null) {
            throw new TableException(
                    String.format(
                            "Cannot find a corresponding symbol class for '%s'.",
                            serializableSymbol));
        }
        return calciteSymbol;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Enum<T>> T serializableToCalcite(
            Class<T> calciteSymbolClass, String value) {
        final String symbolKind = calciteToSymbolKind.get(calciteSymbolClass);
        if (symbolKind == null) {
            throw new TableException(
                    String.format(
                            "Cannot find a corresponding symbol kind for class '%s'.",
                            calciteSymbolClass.getName()));
        }
        return (T) serializableToCalcite.get(SerializableSymbol.of(symbolKind, value));
    }

    // --------------------------------------------------------------------------------------------
    // SerializableSymbol
    // --------------------------------------------------------------------------------------------

    /** Serializable representation of a symbol that can be used for persistence. */
    public static class SerializableSymbol {
        private final String kind;
        private final String value;

        private SerializableSymbol(String kind, String value) {
            this.kind = kind;
            this.value = value;
        }

        public static SerializableSymbol of(String kind, String value) {
            return new SerializableSymbol(kind, value);
        }

        public String getKind() {
            return kind;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SerializableSymbol that = (SerializableSymbol) o;
            return kind.equals(that.kind) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, value);
        }

        @Override
        public String toString() {
            return kind + '.' + value;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static void addSymbolMapping(
            @Nullable TableSymbol commonSymbol,
            @Nullable Enum<?> commonInternalSymbol,
            Enum<?> calciteSymbol,
            String serializableKind,
            String serializableValue) {
        checkNotNull(calciteSymbol, "Calcite symbol must not be null.");
        checkNotNull(serializableKind, "Serializable kind must not be null.");
        checkNotNull(serializableValue, "Serializable value must not be null.");
        final SerializableSymbol serializableSymbol =
                SerializableSymbol.of(serializableKind, serializableValue);
        checkCalciteSymbol(calciteSymbol);
        final Class<?> calciteSymbolClass = calciteSymbol.getDeclaringClass();
        if (calciteToSymbolKind.containsKey(calciteSymbolClass)) {
            checkArgument(
                    calciteToSymbolKind.get(calciteSymbolClass).equals(serializableKind),
                    "All Calcite symbols should map to the same kind.");
        } else {
            calciteToSymbolKind.put(calciteSymbolClass, serializableKind);
        }

        serializableToCalcite.put(serializableSymbol, calciteSymbol);
        calciteToSerializable.put(calciteSymbol, serializableSymbol);
        if (commonSymbol != null) {
            final Enum<?> commonSymbolEnum = (Enum<?>) commonSymbol;
            checkCommonSymbol(commonSymbolEnum);
            calciteToCommon.put(calciteSymbol, commonSymbolEnum);
            commonToCalcite.put(commonSymbolEnum, calciteSymbol);
        }
        if (commonInternalSymbol != null) {
            checkCommonSymbol(commonInternalSymbol);
            calciteToInternalCommon.put(calciteSymbol, commonInternalSymbol);
            internalCommonToCalcite.put(commonInternalSymbol, calciteSymbol);
        }
    }

    private static void checkCalciteSymbol(Enum<?> calciteSymbol) {
        final String className = calciteSymbol.getClass().getName();
        // some Calcite symbols directly rely on Guava classes (e.g. Sarg literal boundaries)
        checkArgument(
                className.startsWith("org.apache.calcite.")
                        || className.startsWith("com.google.common.collect."),
                "Class '%s' is not a Calcite symbol.",
                calciteSymbol);
    }

    private static void checkCommonSymbol(Enum<?> commonInternalSymbol) {
        checkArgument(
                commonInternalSymbol.getClass().getName().startsWith("org.apache.flink.table."),
                "Class '%s' is not a Flink symbol.",
                commonInternalSymbol);
    }

    private SymbolUtil() {
        // no instantiation
    }
}
