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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.HOUR;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.MINUTE;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.SECOND;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.APPROXIMATE_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.BINARY_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.DATETIME;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.EXACT_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.INTERVAL;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.TIME;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.TIMESTAMP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MAP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MULTISET;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.NULL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.RAW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;
import static org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.MONTH;
import static org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.YEAR;
import static org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/** Utilities for merging multiple {@link LogicalType}. */
@Internal
public final class LogicalTypeMerging {

    // mappings for interval generalization
    private static final Map<YearMonthResolution, List<YearMonthResolution>>
            YEAR_MONTH_RES_TO_BOUNDARIES = new HashMap<>();
    private static final Map<List<YearMonthResolution>, YearMonthResolution>
            YEAR_MONTH_BOUNDARIES_TO_RES = new HashMap<>();
    private static final int MINIMUM_ADJUSTED_SCALE = 6;

    static {
        addYearMonthMapping(YEAR, YEAR);
        addYearMonthMapping(MONTH, MONTH);
        addYearMonthMapping(YEAR_TO_MONTH, YEAR, MONTH);
    }

    private static final Map<DayTimeResolution, List<DayTimeResolution>>
            DAY_TIME_RES_TO_BOUNDARIES = new HashMap<>();
    private static final Map<List<DayTimeResolution>, DayTimeResolution>
            DAY_TIME_BOUNDARIES_TO_RES = new HashMap<>();

    static {
        addDayTimeMapping(DAY, DAY);
        addDayTimeMapping(DAY_TO_HOUR, DAY, HOUR);
        addDayTimeMapping(DAY_TO_MINUTE, DAY, MINUTE);
        addDayTimeMapping(DAY_TO_SECOND, DAY, SECOND);
        addDayTimeMapping(HOUR, HOUR);
        addDayTimeMapping(HOUR_TO_MINUTE, HOUR, MINUTE);
        addDayTimeMapping(HOUR_TO_SECOND, HOUR, SECOND);
        addDayTimeMapping(MINUTE, MINUTE);
        addDayTimeMapping(MINUTE_TO_SECOND, MINUTE, SECOND);
        addDayTimeMapping(SECOND, SECOND);
    }

    private static void addYearMonthMapping(
            YearMonthResolution to, YearMonthResolution... boundaries) {
        final List<YearMonthResolution> boundariesList = Arrays.asList(boundaries);
        YEAR_MONTH_RES_TO_BOUNDARIES.put(to, boundariesList);
        YEAR_MONTH_BOUNDARIES_TO_RES.put(boundariesList, to);
    }

    private static void addDayTimeMapping(DayTimeResolution to, DayTimeResolution... boundaries) {
        final List<DayTimeResolution> boundariesList = Arrays.asList(boundaries);
        DAY_TIME_RES_TO_BOUNDARIES.put(to, boundariesList);
        DAY_TIME_BOUNDARIES_TO_RES.put(boundariesList, to);
    }

    /**
     * Returns the most common, more general {@link LogicalType} for a given set of types. If such a
     * type exists, all given types can be casted to this more general type.
     *
     * <p>For example: {@code [INT, BIGINT, DECIMAL(2, 2)]} would lead to {@code DECIMAL(21, 2)}.
     *
     * <p>This class aims to be compatible with the SQL standard. It is inspired by Apache Calcite's
     * {@code SqlTypeFactoryImpl#leastRestrictive} method.
     */
    public static Optional<LogicalType> findCommonType(List<LogicalType> types) {
        Preconditions.checkArgument(types.size() > 0, "List of types must not be empty.");

        // collect statistics first
        boolean hasRawType = false;
        boolean hasNullType = false;
        boolean hasNullableTypes = false;
        for (LogicalType type : types) {
            final LogicalTypeRoot typeRoot = type.getTypeRoot();
            if (typeRoot == RAW) {
                hasRawType = true;
            } else if (typeRoot == NULL) {
                hasNullType = true;
            }
            if (type.isNullable()) {
                hasNullableTypes = true;
            }
        }

        final List<LogicalType> normalizedTypes =
                types.stream().map(t -> t.copy(true)).collect(Collectors.toList());

        LogicalType foundType = findCommonNullableType(normalizedTypes, hasRawType, hasNullType);
        if (foundType == null) {
            foundType = findCommonCastableType(normalizedTypes);
        }

        if (foundType != null) {
            final LogicalType typeWithNullability = foundType.copy(hasNullableTypes);
            // NULL is reserved for untyped literals only
            if (hasRoot(typeWithNullability, NULL)) {
                return Optional.empty();
            }
            return Optional.of(typeWithNullability);
        }
        return Optional.empty();
    }

    // ========================= Decimal Precision Deriving ==========================
    // Adopted from "https://docs.microsoft.com/en-us/sql/t-sql/data-types/precision-
    // scale-and-length-transact-sql"
    //
    // Operation    Result Precision                        Result Scale
    // e1 + e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
    // e1 - e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
    // e1 * e2      p1 + p2 + 1                             s1 + s2
    // e1 / e2      p1 - s1 + s2 + max(6, s1 + p2 + 1)      max(6, s1 + p2 + 1)
    // e1 % e2      min(p1-s1, p2-s2) + max(s1, s2)         max(s1, s2)
    //
    // Also, if the precision / scale are out of the range, the scale may be sacrificed
    // in order to prevent the truncation of the integer part of the decimals.

    /** Finds the result type of a decimal division operation. */
    public static DecimalType findDivisionDecimalType(
            int precision1, int scale1, int precision2, int scale2) {
        int scale = Math.max(6, scale1 + precision2 + 1);
        int precision = precision1 - scale1 + scale2 + scale;
        return adjustPrecisionScale(precision, scale);
    }

    /** Finds the result type of a decimal modulo operation. */
    public static DecimalType findModuloDecimalType(
            int precision1, int scale1, int precision2, int scale2) {
        final int scale = Math.max(scale1, scale2);
        int precision = Math.min(precision1 - scale1, precision2 - scale2) + scale;
        return adjustPrecisionScale(precision, scale);
    }

    /** Finds the result type of a decimal multiplication operation. */
    public static DecimalType findMultiplicationDecimalType(
            int precision1, int scale1, int precision2, int scale2) {
        int scale = scale1 + scale2;
        int precision = precision1 + precision2 + 1;
        return adjustPrecisionScale(precision, scale);
    }

    /** Finds the result type of a decimal addition operation. */
    public static DecimalType findAdditionDecimalType(
            int precision1, int scale1, int precision2, int scale2) {
        final int scale = Math.max(scale1, scale2);
        int precision = Math.max(precision1 - scale1, precision2 - scale2) + scale + 1;
        return adjustPrecisionScale(precision, scale);
    }

    /** Finds the result type of a decimal rounding operation. */
    public static DecimalType findRoundDecimalType(int precision, int scale, int round) {
        if (round >= scale) {
            return new DecimalType(false, precision, scale);
        }
        if (round < 0) {
            return new DecimalType(
                    false, Math.min(DecimalType.MAX_PRECISION, 1 + precision - scale), 0);
        }
        // 0 <= r < s
        // NOTE: rounding may increase the digits by 1, therefore we need +1 on precisions.
        return new DecimalType(false, 1 + precision - scale + round, round);
    }

    /** Finds the result type of a decimal average aggregation. */
    public static LogicalType findAvgAggType(LogicalType argType) {
        final LogicalType resultType;
        if (hasRoot(argType, LogicalTypeRoot.DECIMAL)) {
            // a hack to make legacy types possible until we drop them
            if (argType instanceof LegacyTypeInformationType) {
                return argType;
            }
            // adopted from
            // https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql
            // however, we count by BIGINT, therefore divide by DECIMAL(20,0),
            // but the end result is actually the same, which is DECIMAL(38, MAX(6, s)).
            resultType = LogicalTypeMerging.findDivisionDecimalType(38, getScale(argType), 20, 0);
        } else {
            resultType = argType;
        }
        return resultType.copy(argType.isNullable());
    }

    /** Finds the result type of a decimal sum aggregation. */
    public static LogicalType findSumAggType(LogicalType argType) {
        // adopted from
        // https://docs.microsoft.com/en-us/sql/t-sql/functions/sum-transact-sql
        final LogicalType resultType;
        if (hasRoot(argType, LogicalTypeRoot.DECIMAL)) {
            // a hack to make legacy types possible until we drop them
            if (argType instanceof LegacyTypeInformationType) {
                return argType;
            }
            resultType = new DecimalType(false, 38, getScale(argType));
        } else {
            resultType = argType;
        }
        return resultType.copy(argType.isNullable());
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Scale adjustment implementation is inspired to SQLServer's one. In particular, when a result
     * precision is greater than MAX_PRECISION, the corresponding scale is reduced to prevent the
     * integral part of a result from being truncated.
     *
     * <p>https://docs.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql
     */
    private static DecimalType adjustPrecisionScale(int precision, int scale) {
        if (precision <= DecimalType.MAX_PRECISION) {
            // Adjustment only needed when we exceed max precision
            return new DecimalType(false, precision, scale);
        } else {
            int digitPart = precision - scale;
            // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value;
            // otherwise preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
            int minScalePart = Math.min(scale, MINIMUM_ADJUSTED_SCALE);
            int adjustScale = Math.max(DecimalType.MAX_PRECISION - digitPart, minScalePart);
            return new DecimalType(false, DecimalType.MAX_PRECISION, adjustScale);
        }
    }

    private static @Nullable LogicalType findCommonCastableType(List<LogicalType> normalizedTypes) {
        LogicalType resultType = normalizedTypes.get(0);

        for (LogicalType type : normalizedTypes) {
            final LogicalTypeRoot typeRoot = type.getTypeRoot();

            // NULL does not affect the result of this loop
            if (typeRoot == NULL) {
                continue;
            }

            if (supportsImplicitCast(resultType, type)) {
                resultType = type;
            } else {
                if (!supportsImplicitCast(type, resultType)) {
                    return null;
                }
            }
        }

        return resultType;
    }

    @SuppressWarnings("ConstantConditions")
    private static @Nullable LogicalType findCommonNullableType(
            List<LogicalType> normalizedTypes, boolean hasRawType, boolean hasNullType) {

        // all RAW types must be equal
        if (hasRawType) {
            return findExactlySameType(normalizedTypes);
        }

        LogicalType resultType = null;

        for (LogicalType type : normalizedTypes) {
            final LogicalTypeRoot typeRoot = type.getTypeRoot();

            // NULL does not affect the result of this loop
            if (typeRoot == NULL) {
                continue;
            }

            // if result type is still null, consider the current type as a potential
            // result type candidate
            if (resultType == null) {
                resultType = type;
            }

            // find special patterns
            final LogicalType patternType = findCommonTypePattern(resultType, type);
            if (patternType != null) {
                resultType = patternType;
                continue;
            }

            // for types of family CONSTRUCTED
            if (typeRoot == ARRAY) {
                return findCommonArrayType(normalizedTypes);
            } else if (typeRoot == MULTISET) {
                return findCommonMultisetType(normalizedTypes);
            } else if (typeRoot == MAP) {
                return findCommonMapType(normalizedTypes);
            } else if (typeRoot == ROW) {
                return findCommonRowType(normalizedTypes);
            }

            // exit if two completely different types are compared (e.g. ROW and INT)
            // this simplifies the following lines as we compare same interval families for example
            if (!areSimilarTypes(resultType, type)) {
                return null;
            }

            // for types of family CHARACTER_STRING or BINARY_STRING
            if (hasFamily(type, CHARACTER_STRING) || hasFamily(type, BINARY_STRING)) {
                final int length = combineLength(resultType, type);

                if (hasRoot(resultType, VARCHAR) || hasRoot(resultType, VARBINARY)) {
                    // variable length types remain variable length types
                    resultType = createStringType(resultType.getTypeRoot(), length);
                } else if (getLength(resultType) != getLength(type)) {
                    // for different fixed lengths
                    // this is different from the SQL standard but prevents whitespace
                    // padding/modification of strings
                    if (hasRoot(resultType, CHAR)) {
                        resultType = createStringType(VARCHAR, length);
                    } else if (hasRoot(resultType, BINARY)) {
                        resultType = createStringType(VARBINARY, length);
                    }
                } else {
                    // for same type with same length
                    resultType = createStringType(typeRoot, length);
                }
            }
            // for EXACT_NUMERIC types
            else if (hasFamily(type, EXACT_NUMERIC)) {
                if (hasFamily(resultType, EXACT_NUMERIC)) {
                    resultType = createCommonExactNumericType(resultType, type);
                } else if (hasFamily(resultType, APPROXIMATE_NUMERIC)) {
                    // the result is already approximate
                    if (typeRoot == DECIMAL) {
                        // in case of DECIMAL we enforce DOUBLE
                        resultType = new DoubleType();
                    }
                } else {
                    return null;
                }
            }
            // for APPROXIMATE_NUMERIC types
            else if (hasFamily(type, APPROXIMATE_NUMERIC)) {
                if (hasFamily(resultType, APPROXIMATE_NUMERIC)) {
                    resultType = createCommonApproximateNumericType(resultType, type);
                } else if (hasFamily(resultType, EXACT_NUMERIC)) {
                    // the result was exact so far
                    if (typeRoot == DECIMAL) {
                        // in case of DECIMAL we enforce DOUBLE
                        resultType = new DoubleType();
                    } else {
                        // enforce an approximate result
                        resultType = type;
                    }
                } else {
                    return null;
                }
            }
            // for DATE
            else if (hasRoot(type, DATE)) {
                if (hasRoot(resultType, DATE)) {
                    resultType = new DateType(); // for enabling findCommonTypePattern
                } else {
                    return null;
                }
            }
            // for TIME
            else if (hasFamily(type, TIME)) {
                if (hasFamily(resultType, TIME)) {
                    resultType = new TimeType(combinePrecision(resultType, type));
                } else {
                    return null;
                }
            }
            // for TIMESTAMP
            else if (hasFamily(type, TIMESTAMP)) {
                if (hasFamily(resultType, TIMESTAMP)) {
                    resultType = createCommonTimestampType(resultType, type);
                } else {
                    return null;
                }
            }
            // for day-time intervals
            else if (typeRoot == INTERVAL_DAY_TIME) {
                resultType =
                        createCommonDayTimeIntervalType(
                                (DayTimeIntervalType) resultType, (DayTimeIntervalType) type);
            }
            // for year-month intervals
            else if (typeRoot == INTERVAL_YEAR_MONTH) {
                resultType =
                        createCommonYearMonthIntervalType(
                                (YearMonthIntervalType) resultType, (YearMonthIntervalType) type);
            }
            // other types are handled by findCommonCastableType
            else {
                return null;
            }
        }

        // NULL type only
        if (resultType == null && hasNullType) {
            return new NullType();
        }

        return resultType;
    }

    private static boolean areSimilarTypes(LogicalType left, LogicalType right) {
        // two types are similar iff they can be the operands of an SQL equality predicate

        // similarity based on families
        if (hasFamily(left, CHARACTER_STRING) && hasFamily(right, CHARACTER_STRING)) {
            return true;
        } else if (hasFamily(left, BINARY_STRING) && hasFamily(right, BINARY_STRING)) {
            return true;
        } else if (hasFamily(left, NUMERIC) && hasFamily(right, NUMERIC)) {
            return true;
        } else if (hasFamily(left, TIME) && hasFamily(right, TIME)) {
            return true;
        } else if (hasFamily(left, TIMESTAMP) && hasFamily(right, TIMESTAMP)) {
            return true;
        }
        // similarity based on root
        return left.getTypeRoot() == right.getTypeRoot();
    }

    private static @Nullable LogicalType findExactlySameType(List<LogicalType> normalizedTypes) {
        final LogicalType firstType = normalizedTypes.get(0);
        for (LogicalType type : normalizedTypes) {
            if (!type.equals(firstType)) {
                return null;
            }
        }
        return firstType;
    }

    private static @Nullable LogicalType findCommonTypePattern(
            LogicalType resultType, LogicalType type) {
        if (hasFamily(resultType, DATETIME) && hasFamily(type, INTERVAL)) {
            return resultType;
        } else if (hasFamily(resultType, INTERVAL) && hasFamily(type, DATETIME)) {
            return type;
        } else if ((hasFamily(resultType, TIMESTAMP) || hasRoot(resultType, DATE))
                && hasFamily(type, EXACT_NUMERIC)) {
            return resultType;
        } else if (hasFamily(resultType, EXACT_NUMERIC)
                && (hasFamily(type, TIMESTAMP) || hasRoot(type, DATE))) {
            return type;
        }
        // for "DATETIME + EXACT_NUMERIC", EXACT_NUMERIC is always treated as an interval of days
        // therefore, TIME + EXACT_NUMERIC is not supported
        return null;
    }

    private static @Nullable LogicalType findCommonArrayType(List<LogicalType> normalizedTypes) {
        final List<LogicalType> children = findCommonChildrenTypes(normalizedTypes);
        if (children == null) {
            return null;
        }
        return new ArrayType(children.get(0));
    }

    private static @Nullable LogicalType findCommonMultisetType(List<LogicalType> normalizedTypes) {
        final List<LogicalType> children = findCommonChildrenTypes(normalizedTypes);
        if (children == null) {
            return null;
        }
        return new MultisetType(children.get(0));
    }

    private static @Nullable LogicalType findCommonMapType(List<LogicalType> normalizedTypes) {
        final List<LogicalType> children = findCommonChildrenTypes(normalizedTypes);
        if (children == null) {
            return null;
        }
        return new MapType(children.get(0), children.get(1));
    }

    private static @Nullable LogicalType findCommonRowType(List<LogicalType> normalizedTypes) {
        final List<LogicalType> children = findCommonChildrenTypes(normalizedTypes);
        if (children == null) {
            return null;
        }
        final RowType firstType = (RowType) normalizedTypes.get(0);
        final List<RowType.RowField> newFields =
                IntStream.range(0, children.size())
                        .mapToObj(
                                pos -> {
                                    final LogicalType newType = children.get(pos);
                                    final RowType.RowField originalField =
                                            firstType.getFields().get(pos);
                                    if (originalField.getDescription().isPresent()) {
                                        return new RowType.RowField(
                                                originalField.getName(),
                                                newType,
                                                originalField.getDescription().get());
                                    } else {
                                        return new RowType.RowField(
                                                originalField.getName(), newType);
                                    }
                                })
                        .collect(Collectors.toList());
        return new RowType(newFields);
    }

    private static @Nullable List<LogicalType> findCommonChildrenTypes(
            List<LogicalType> normalizedTypes) {
        final LogicalType firstType = normalizedTypes.get(0);
        final LogicalTypeRoot typeRoot = firstType.getTypeRoot();
        final int numberOfChildren = firstType.getChildren().size();

        for (LogicalType type : normalizedTypes) {
            // all types must have the same root
            if (type.getTypeRoot() != typeRoot) {
                return null;
            }
            // all types must have the same number of children
            if (type.getChildren().size() != numberOfChildren) {
                return null;
            }
        }

        // recursively compute column-wise least restrictive
        final List<LogicalType> resultChildren = new ArrayList<>(numberOfChildren);
        for (int i = 0; i < numberOfChildren; i++) {
            final Optional<LogicalType> childType =
                    findCommonType(new ChildTypeView(normalizedTypes, i));
            if (!childType.isPresent()) {
                return null;
            }
            resultChildren.add(childType.get());
        }
        // no child should be empty at this point
        return resultChildren;
    }

    private static LogicalType createCommonExactNumericType(
            LogicalType resultType, LogicalType type) {
        // same EXACT_NUMERIC types
        if (type.equals(resultType)) {
            return resultType;
        }

        final LogicalTypeRoot resultTypeRoot = resultType.getTypeRoot();
        final LogicalTypeRoot typeRoot = type.getTypeRoot();

        // no DECIMAL types involved
        if (resultTypeRoot != DECIMAL && typeRoot != DECIMAL) {
            // type root contains order of precision
            if (getPrecision(type) > getPrecision(resultType)) {
                return type;
            }
            return resultType;
        }

        // determine DECIMAL with precision (p), scale (s) and number of whole digits (d):
        // d = max(p1 - s1, p2 - s2)
        // s <= max(s1, s2)
        // p = s + d
        final int p1 = getPrecision(resultType);
        final int p2 = getPrecision(type);
        final int s1 = getScale(resultType);
        final int s2 = getScale(type);
        final int maxPrecision = DecimalType.MAX_PRECISION;

        int d = Math.max(p1 - s1, p2 - s2);
        d = Math.min(d, maxPrecision);

        int s = Math.max(s1, s2);
        s = Math.min(s, maxPrecision - d);

        final int p = d + s;

        return new DecimalType(p, s);
    }

    private static LogicalType createCommonApproximateNumericType(
            LogicalType resultType, LogicalType type) {
        if (hasRoot(resultType, DOUBLE) || hasRoot(type, DOUBLE)) {
            return new DoubleType();
        }
        return resultType;
    }

    private static LogicalType createCommonTimestampType(LogicalType resultType, LogicalType type) {
        // same types
        if (type.equals(resultType)) {
            return resultType;
        }

        final LogicalTypeRoot resultTypeRoot = resultType.getTypeRoot();
        final LogicalTypeRoot typeRoot = type.getTypeRoot();
        final int precision = combinePrecision(resultType, type);

        // same type roots
        if (typeRoot == resultTypeRoot) {
            return createTimestampType(resultTypeRoot, precision);
        }

        // generalize to zoned type
        if (typeRoot == TIMESTAMP_WITH_TIME_ZONE || resultTypeRoot == TIMESTAMP_WITH_TIME_ZONE) {
            return createTimestampType(TIMESTAMP_WITH_TIME_ZONE, precision);
        } else if (typeRoot == TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || resultTypeRoot == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return createTimestampType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision);
        }
        return createTimestampType(TIMESTAMP_WITHOUT_TIME_ZONE, precision);
    }

    private static LogicalType createCommonDayTimeIntervalType(
            DayTimeIntervalType resultType, DayTimeIntervalType type) {
        final int maxDayPrecision = Math.max(resultType.getDayPrecision(), type.getDayPrecision());
        final int maxFractionalPrecision =
                Math.max(resultType.getFractionalPrecision(), type.getFractionalPrecision());
        return new DayTimeIntervalType(
                combineIntervalResolutions(
                        DayTimeResolution.values(),
                        DAY_TIME_RES_TO_BOUNDARIES,
                        DAY_TIME_BOUNDARIES_TO_RES,
                        resultType.getResolution(),
                        type.getResolution()),
                maxDayPrecision,
                maxFractionalPrecision);
    }

    private static LogicalType createCommonYearMonthIntervalType(
            YearMonthIntervalType resultType, YearMonthIntervalType type) {
        final int maxYearPrecision =
                Math.max(resultType.getYearPrecision(), type.getYearPrecision());
        return new YearMonthIntervalType(
                combineIntervalResolutions(
                        YearMonthResolution.values(),
                        YEAR_MONTH_RES_TO_BOUNDARIES,
                        YEAR_MONTH_BOUNDARIES_TO_RES,
                        resultType.getResolution(),
                        type.getResolution()),
                maxYearPrecision);
    }

    private static LogicalType createTimestampType(LogicalTypeRoot typeRoot, int precision) {
        switch (typeRoot) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(precision);
            case TIMESTAMP_WITH_TIME_ZONE:
                return new ZonedTimestampType(precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(precision);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static LogicalType createStringType(LogicalTypeRoot typeRoot, int length) {
        switch (typeRoot) {
            case CHAR:
                if (length == 0) {
                    return CharType.ofEmptyLiteral();
                }
                return new CharType(length);
            case VARCHAR:
                if (length == 0) {
                    return VarCharType.ofEmptyLiteral();
                }
                return new VarCharType(length);
            case BINARY:
                if (length == 0) {
                    return BinaryType.ofEmptyLiteral();
                }
                return new BinaryType(length);
            case VARBINARY:
                if (length == 0) {
                    return VarBinaryType.ofEmptyLiteral();
                }
                return new VarBinaryType(length);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static <T extends Enum<T>> T combineIntervalResolutions(
            T[] res,
            Map<T, List<T>> resToBoundaries,
            Map<List<T>, T> boundariesToRes,
            T left,
            T right) {
        final List<T> leftBoundaries = resToBoundaries.get(left);
        final T leftStart = leftBoundaries.get(0);
        final T leftEnd = leftBoundaries.get(leftBoundaries.size() - 1);

        final List<T> rightBoundaries = resToBoundaries.get(right);
        final T rightStart = rightBoundaries.get(0);
        final T rightEnd = rightBoundaries.get(rightBoundaries.size() - 1);

        final T combinedStart = res[Math.min(leftStart.ordinal(), rightStart.ordinal())];
        final T combinedEnd = res[Math.max(leftEnd.ordinal(), rightEnd.ordinal())];

        if (combinedStart == combinedEnd) {
            return boundariesToRes.get(Collections.singletonList(combinedStart));
        }
        return boundariesToRes.get(Arrays.asList(combinedStart, combinedEnd));
    }

    private static int combinePrecision(LogicalType resultType, LogicalType type) {
        final int p1 = getPrecision(resultType);
        final int p2 = getPrecision(type);

        return Math.max(p1, p2);
    }

    private static int combineLength(LogicalType resultType, LogicalType right) {
        return Math.max(getLength(resultType), getLength(right));
    }

    /** A list that creates a view of all children at the given position. */
    private static class ChildTypeView extends AbstractList<LogicalType> {

        private final List<LogicalType> types;
        private final int childPos;

        ChildTypeView(List<LogicalType> types, int childPos) {
            this.types = types;
            this.childPos = childPos;
        }

        @Override
        public LogicalType get(int index) {
            return types.get(index).getChildren().get(childPos);
        }

        @Override
        public int size() {
            return types.size();
        }
    }

    private LogicalTypeMerging() {
        // no instantiation
    }
}
