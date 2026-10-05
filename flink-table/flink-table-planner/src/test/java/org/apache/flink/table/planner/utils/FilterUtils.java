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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.runtime.functions.VariantCastUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOWER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TRY_CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.UPPER;

/** Utils for catalog and source to filter partition or row. */
public class FilterUtils {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Casts of a VARIANT element that can be evaluated, by target type, using the same {@link
     * VariantCastUtils} helpers as the generated code of {@code VariantToPrimitiveCastRule}.
     */
    private static final Map<LogicalTypeRoot, BiFunction<Variant, LogicalType, Object>>
            VARIANT_CASTS = new EnumMap<>(LogicalTypeRoot.class);

    static {
        VARIANT_CASTS.put(LogicalTypeRoot.BOOLEAN, (variant, type) -> variant.getBoolean());
        VARIANT_CASTS.put(
                LogicalTypeRoot.TINYINT,
                (variant, type) ->
                        (byte)
                                VariantCastUtils.toIntegral(
                                        variant, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT"));
        VARIANT_CASTS.put(
                LogicalTypeRoot.SMALLINT,
                (variant, type) ->
                        (short)
                                VariantCastUtils.toIntegral(
                                        variant, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT"));
        VARIANT_CASTS.put(
                LogicalTypeRoot.INTEGER,
                (variant, type) ->
                        (int)
                                VariantCastUtils.toIntegral(
                                        variant, Integer.MIN_VALUE, Integer.MAX_VALUE, "INT"));
        VARIANT_CASTS.put(
                LogicalTypeRoot.BIGINT,
                (variant, type) ->
                        VariantCastUtils.toIntegral(
                                variant, Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT"));
        VARIANT_CASTS.put(
                LogicalTypeRoot.FLOAT, (variant, type) -> VariantCastUtils.toFloat(variant));
        VARIANT_CASTS.put(
                LogicalTypeRoot.DOUBLE, (variant, type) -> VariantCastUtils.toDouble(variant));
        VARIANT_CASTS.put(
                LogicalTypeRoot.CHAR,
                (variant, type) ->
                        VariantCastUtils.toStringValue(
                                        variant, UTC, ((CharType) type).getLength(), true)
                                .toString());
        VARIANT_CASTS.put(
                LogicalTypeRoot.VARCHAR,
                (variant, type) ->
                        VariantCastUtils.toStringValue(
                                        variant, UTC, ((VarCharType) type).getLength(), false)
                                .toString());
    }

    public static boolean shouldPushDown(ResolvedExpression expr, Set<String> filterableFields) {
        if (expr instanceof CallExpression && expr.getChildren().size() == 2) {
            return shouldPushDownUnaryExpression(
                            expr.getResolvedChildren().get(0), filterableFields)
                    && shouldPushDownUnaryExpression(
                            expr.getResolvedChildren().get(1), filterableFields);
        }
        return false;
    }

    public static boolean isRetainedAfterApplyingFilterPredicates(
            List<ResolvedExpression> predicates,
            Function<String, ?> getter,
            @Nullable Function<int[], ?> nestedFieldGetter) {
        for (ResolvedExpression predicate : predicates) {
            if (predicate instanceof CallExpression) {
                FunctionDefinition definition =
                        ((CallExpression) predicate).getFunctionDefinition();
                boolean result = false;
                if (definition.equals(BuiltInFunctionDefinitions.OR)) {
                    // nested filter, such as (key1 > 2 or key2 > 3)
                    for (Expression expr : predicate.getChildren()) {
                        if (!(expr instanceof CallExpression && expr.getChildren().size() == 2)) {
                            throw new TableException(expr + " not supported!");
                        }
                        result =
                                binaryFilterApplies(
                                        (CallExpression) expr, getter, nestedFieldGetter);
                        if (result) {
                            break;
                        }
                    }
                } else if (predicate.getChildren().size() == 2) {
                    result =
                            binaryFilterApplies(
                                    (CallExpression) predicate, getter, nestedFieldGetter);
                } else {
                    throw new UnsupportedOperationException(
                            String.format("Unsupported expr: %s.", predicate));
                }
                if (!result) {
                    return false;
                }
            } else {
                throw new UnsupportedOperationException(
                        String.format("Unsupported expr: %s.", predicate));
            }
        }
        return true;
    }

    private static boolean shouldPushDownUnaryExpression(
            ResolvedExpression expr, Set<String> filterableFields) {
        // validate that type is comparable
        if (!isComparable(expr.getOutputDataType().getConversionClass())) {
            return false;
        }
        if (expr instanceof FieldReferenceExpression) {
            if (filterableFields.contains(((FieldReferenceExpression) expr).getName())) {
                return true;
            }
        }

        if (expr instanceof NestedFieldReferenceExpression) {
            if (filterableFields.contains(((NestedFieldReferenceExpression) expr).getName())) {
                return true;
            }
        }

        if (expr instanceof ValueLiteralExpression) {
            return true;
        }

        if (isElementAccess(expr, filterableFields)) {
            return true;
        }

        // A cast makes an element of a VARIANT comparable, e.g. CAST(v['k'] AS INT).
        if (isCall(expr, CAST) || isCall(expr, TRY_CAST)) {
            final ResolvedExpression element = expr.getResolvedChildren().get(0);
            return VARIANT_CASTS.containsKey(
                            expr.getOutputDataType().getLogicalType().getTypeRoot())
                    && element.getOutputDataType().getLogicalType().is(LogicalTypeRoot.VARIANT)
                    && isElementAccess(element, filterableFields);
        }

        if (expr instanceof CallExpression && expr.getChildren().size() == 1) {
            if (((CallExpression) expr).getFunctionDefinition().equals(UPPER)
                    || ((CallExpression) expr).getFunctionDefinition().equals(LOWER)) {
                return shouldPushDownUnaryExpression(
                        expr.getResolvedChildren().get(0), filterableFields);
            }
        }
        // other resolved expressions return false
        return false;
    }

    private static boolean isElementAccess(ResolvedExpression expr, Set<String> filterableFields) {
        if (!isCall(expr, AT)) {
            return false;
        }
        final ResolvedExpression container = expr.getResolvedChildren().get(0);
        final ResolvedExpression key = expr.getResolvedChildren().get(1);
        return isFilterableField(container, filterableFields)
                && key instanceof ValueLiteralExpression
                && isEvaluableKey(
                        container.getOutputDataType().getLogicalType(),
                        key.getOutputDataType().getLogicalType());
    }

    /** A map lookup only finds the key if the literal has the key's Java type. */
    private static boolean isEvaluableKey(LogicalType containerType, LogicalType keyType) {
        if (!containerType.is(LogicalTypeRoot.MAP)) {
            return true;
        }
        final LogicalType mapKeyType = ((MapType) containerType).getKeyType();
        return mapKeyType.is(keyType.getTypeRoot())
                || (mapKeyType.is(LogicalTypeFamily.CHARACTER_STRING)
                        && keyType.is(LogicalTypeFamily.CHARACTER_STRING));
    }

    private static boolean isCall(Expression expr, FunctionDefinition definition) {
        return expr instanceof CallExpression
                && ((CallExpression) expr).getFunctionDefinition().equals(definition);
    }

    private static boolean isFilterableField(
            ResolvedExpression expr, Set<String> filterableFields) {
        if (expr instanceof FieldReferenceExpression) {
            return filterableFields.contains(((FieldReferenceExpression) expr).getName());
        }
        if (expr instanceof NestedFieldReferenceExpression) {
            return filterableFields.contains(((NestedFieldReferenceExpression) expr).getName());
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean binaryFilterApplies(
            CallExpression binExpr,
            Function<String, ?> getter,
            Function<int[], ?> nestedFieldGetter) {
        List<Expression> children = binExpr.getChildren();
        Preconditions.checkArgument(children.size() == 2);

        Comparable lhsValue = getValue(children.get(0), getter, nestedFieldGetter);
        Comparable rhsValue = getValue(children.get(1), getter, nestedFieldGetter);
        if (lhsValue == null || rhsValue == null) {
            // A comparison with NULL is never true, e.g. for an array index out of bounds.
            return false;
        }
        FunctionDefinition functionDefinition = binExpr.getFunctionDefinition();
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) > 0;
        } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) < 0;
        } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) >= 0;
        } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) <= 0;
        } else if (BuiltInFunctionDefinitions.EQUALS.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) == 0;
        } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) != 0;
        } else {
            throw new UnsupportedOperationException("Unsupported operator: " + functionDefinition);
        }
    }

    /** Returns NULL for a missing field, an out-of-range index or a container of the wrong kind. */
    private static @Nullable Variant getVariantElement(Variant variant, Object key) {
        if (key instanceof String) {
            return variant.isObject() ? variant.getField((String) key) : null;
        }
        final int index = ((Number) key).intValue();
        return variant.isArray() && 1 <= index && index <= variant.getArraySize()
                ? variant.getElement(index - 1)
                : null;
    }

    /** Casts a VARIANT element like the generated code: an error yields NULL for TRY_CAST. */
    private static @Nullable Object castVariant(
            @Nullable Variant variant, LogicalType targetType, boolean tryCast) {
        if (variant == null || variant.isNull()) {
            return null;
        }
        try {
            return VARIANT_CASTS.get(targetType.getTypeRoot()).apply(variant, targetType);
        } catch (RuntimeException e) {
            if (tryCast) {
                return null;
            }
            throw e;
        }
    }

    private static boolean isComparable(Class<?> clazz) {
        return Comparable.class.isAssignableFrom(clazz);
    }

    private static Comparable<?> getValue(
            Expression expr, Function<String, ?> getter, Function<int[], ?> nestedFieldGetter) {
        return (Comparable<?>) getRawValue(expr, getter, nestedFieldGetter);
    }

    private static Object getRawValue(
            Expression expr, Function<String, ?> getter, Function<int[], ?> nestedFieldGetter) {
        if (expr instanceof ValueLiteralExpression) {
            Optional<?> value =
                    ((ValueLiteralExpression) expr)
                            .getValueAs(
                                    ((ValueLiteralExpression) expr)
                                            .getOutputDataType()
                                            .getConversionClass());
            return value.orElse(null);
        }

        if (expr instanceof FieldReferenceExpression) {
            return getter.apply(((FieldReferenceExpression) expr).getName());
        }

        if (expr instanceof NestedFieldReferenceExpression) {
            if (nestedFieldGetter != null) {
                return nestedFieldGetter.apply(
                        ((NestedFieldReferenceExpression) expr).getFieldIndices());
            } else {
                throw new RuntimeException("NestedFieldReferenceExpression not supported!");
            }
        }

        if (isCall(expr, CAST) || isCall(expr, TRY_CAST)) {
            return castVariant(
                    (Variant) getRawValue(expr.getChildren().get(0), getter, nestedFieldGetter),
                    ((CallExpression) expr).getOutputDataType().getLogicalType(),
                    isCall(expr, TRY_CAST));
        }

        if (isCall(expr, AT)) {
            final Object container =
                    getRawValue(expr.getChildren().get(0), getter, nestedFieldGetter);
            final Object key = getRawValue(expr.getChildren().get(1), getter, nestedFieldGetter);
            if (container == null || key == null) {
                return null;
            }
            if (container instanceof Map) {
                return ((Map<?, ?>) container).get(key);
            }
            if (container.getClass().isArray()) {
                // SQL array indices are 1-based.
                final int index = ((Number) key).intValue();
                return 1 <= index && index <= Array.getLength(container)
                        ? Array.get(container, index - 1)
                        : null;
            }
            if (container instanceof Variant) {
                return getVariantElement((Variant) container, key);
            }
            throw new UnsupportedOperationException(
                    String.format("Unsupported container for %s: %s.", expr, container.getClass()));
        }

        if (expr instanceof CallExpression && expr.getChildren().size() == 1) {
            Object child = getValue(expr.getChildren().get(0), getter, nestedFieldGetter);
            FunctionDefinition functionDefinition = ((CallExpression) expr).getFunctionDefinition();
            if (functionDefinition.equals(UPPER)) {
                return child.toString().toUpperCase();
            } else if (functionDefinition.equals(LOWER)) {
                return child.toString().toLowerCase();
            } else {
                throw new UnsupportedOperationException(
                        String.format("Unrecognized function definition: %s.", functionDefinition));
            }
        }
        throw new UnsupportedOperationException(expr + " not supported!");
    }
}
