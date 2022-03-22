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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/** This class resolves {@link CastRule} using the input and the target type. */
@Internal
public class CastRuleProvider {

    /* ------- Singleton declaration ------- */

    private static final CastRuleProvider INSTANCE = new CastRuleProvider();

    static {
        INSTANCE
                // Numeric rules
                .addRule(DecimalToDecimalCastRule.INSTANCE)
                .addRule(NumericPrimitiveToDecimalCastRule.INSTANCE)
                .addRule(DecimalToNumericPrimitiveCastRule.INSTANCE)
                .addRule(NumericPrimitiveCastRule.INSTANCE)
                // Boolean <-> numeric rules
                .addRule(BooleanToNumericCastRule.INSTANCE)
                .addRule(NumericToBooleanCastRule.INSTANCE)
                // To string rules
                .addRule(NumericToStringCastRule.INSTANCE)
                .addRule(BooleanToStringCastRule.INSTANCE)
                .addRule(BinaryToStringCastRule.INSTANCE)
                .addRule(TimestampToStringCastRule.INSTANCE)
                .addRule(TimeToStringCastRule.INSTANCE)
                .addRule(DateToStringCastRule.INSTANCE)
                .addRule(IntervalToStringCastRule.INSTANCE)
                .addRule(ArrayToStringCastRule.INSTANCE)
                .addRule(MapAndMultisetToStringCastRule.INSTANCE)
                .addRule(StructuredToStringCastRule.INSTANCE)
                .addRule(RowToStringCastRule.INSTANCE)
                .addRule(RawToStringCastRule.INSTANCE)
                // From string rules
                .addRule(StringToBooleanCastRule.INSTANCE)
                .addRule(StringToDecimalCastRule.INSTANCE)
                .addRule(StringToNumericPrimitiveCastRule.INSTANCE)
                .addRule(StringToDateCastRule.INSTANCE)
                .addRule(StringToTimeCastRule.INSTANCE)
                .addRule(StringToTimestampCastRule.INSTANCE)
                .addRule(StringToBinaryCastRule.INSTANCE)
                // Date/Time/Timestamp rules
                .addRule(TimestampToTimestampCastRule.INSTANCE)
                .addRule(TimestampToDateCastRule.INSTANCE)
                .addRule(TimestampToTimeCastRule.INSTANCE)
                .addRule(DateToTimestampCastRule.INSTANCE)
                .addRule(TimeToTimestampCastRule.INSTANCE)
                .addRule(NumericToTimestampCastRule.INSTANCE)
                .addRule(TimestampToNumericCastRule.INSTANCE)
                // To binary rules
                .addRule(BinaryToBinaryCastRule.INSTANCE)
                .addRule(RawToBinaryCastRule.INSTANCE)
                // Collection rules
                .addRule(ArrayToArrayCastRule.INSTANCE)
                .addRule(MapToMapAndMultisetToMultisetCastRule.INSTANCE)
                .addRule(RowToRowCastRule.INSTANCE)
                // Special rules
                .addRule(CharVarCharTrimPadCastRule.INSTANCE)
                .addRule(NullToStringCastRule.INSTANCE)
                .addRule(IdentityCastRule.INSTANCE);
    }

    /* ------- Entrypoint ------- */

    /**
     * Resolve a {@link CastRule} for the provided input type and target type. Returns {@code null}
     * if no rule can be resolved.
     */
    public static @Nullable CastRule<?, ?> resolve(LogicalType inputType, LogicalType targetType) {
        return INSTANCE.internalResolve(inputType, targetType);
    }

    /**
     * Returns {@code true} if and only if a {@link CastRule} can be resolved for the provided input
     * type and target type.
     */
    public static boolean exists(LogicalType inputType, LogicalType targetType) {
        return resolve(inputType, targetType) != null;
    }

    /**
     * Resolves the rule and returns the result of {@link CastRule#canFail(LogicalType,
     * LogicalType)}. Fails with {@link NullPointerException} if the rule cannot be resolved.
     */
    public static boolean canFail(LogicalType inputType, LogicalType targetType) {
        return Preconditions.checkNotNull(
                        resolve(inputType, targetType), "Cast rule cannot be resolved")
                .canFail(inputType, targetType);
    }

    /**
     * Create a {@link CastExecutor} for the provided input type and target type. Returns {@code
     * null} if no rule can be resolved.
     *
     * @see CastRule#create(CastRule.Context, LogicalType, LogicalType)
     */
    public static @Nullable CastExecutor<?, ?> create(
            CastRule.Context context, LogicalType inputLogicalType, LogicalType targetLogicalType) {
        CastRule<?, ?> rule = INSTANCE.internalResolve(inputLogicalType, targetLogicalType);
        if (rule == null) {
            return null;
        }
        return rule.create(context, inputLogicalType, targetLogicalType);
    }

    /**
     * Create a {@link CastCodeBlock} for the provided input type and target type. Returns {@code
     * null} if no rule can be resolved or the resolved rule is not instance of {@link
     * CodeGeneratorCastRule}.
     *
     * @see CodeGeneratorCastRule#generateCodeBlock(CodeGeneratorCastRule.Context, String, String,
     *     LogicalType, LogicalType)
     */
    @SuppressWarnings("rawtypes")
    public static @Nullable CastCodeBlock generateCodeBlock(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String inputIsNullTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        CastRule<?, ?> rule = INSTANCE.internalResolve(inputLogicalType, targetLogicalType);
        if (!(rule instanceof CodeGeneratorCastRule)) {
            return null;
        }
        return ((CodeGeneratorCastRule) rule)
                .generateCodeBlock(
                        context, inputTerm, inputIsNullTerm, inputLogicalType, targetLogicalType);
    }

    /**
     * Create a {@link CastExecutor} and execute the cast on the provided {@code value}. Fails with
     * {@link IllegalArgumentException} if the rule cannot be resolved, or with an exception from
     * the {@link CastExecutor} itself if the rule can fail.
     */
    @SuppressWarnings("unchecked")
    public static @Nullable Object cast(
            CastRule.Context context,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType,
            Object value) {
        CastExecutor<Object, Object> castExecutor =
                (CastExecutor<Object, Object>)
                        CastRuleProvider.create(context, inputLogicalType, targetLogicalType);

        if (castExecutor == null) {
            throw new NullPointerException(
                    "Unsupported casting from " + inputLogicalType + " to " + targetLogicalType);
        }

        return castExecutor.cast(value);
    }

    /**
     * This method wraps {@link #generateCodeBlock(CodeGeneratorCastRule.Context, String, String,
     * LogicalType, LogicalType)}, but adding the assumption that the inputTerm is always non-null.
     * Used by {@link CodeGeneratorCastRule}s which checks for nullability, rather than deferring
     * the check to the rules.
     */
    static CastCodeBlock generateAlwaysNonNullCodeBlock(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (inputLogicalType instanceof NullType) {
            return generateCodeBlock(
                    context, inputTerm, "true", inputLogicalType, targetLogicalType);
        }
        return generateCodeBlock(
                context, inputTerm, "false", inputLogicalType.copy(false), targetLogicalType);
    }

    /* ------ Implementation ------ */

    // Map<Target family or root, Map<Input family or root, rule>>
    private final Map<Object, Map<Object, CastRule<?, ?>>> rules = new HashMap<>();
    private final List<CastRule<?, ?>> rulesWithCustomPredicate = new ArrayList<>();

    private CastRuleProvider addRule(CastRule<?, ?> rule) {
        CastRulePredicate predicate = rule.getPredicateDefinition();

        for (LogicalType targetType : predicate.getTargetTypes()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetType, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (LogicalTypeRoot targetTypeRoot : predicate.getTargetTypeRoots()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeRoot, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (LogicalTypeFamily targetTypeFamily : predicate.getTargetTypeFamilies()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeFamily, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }

        if (predicate.getCustomPredicate().isPresent()) {
            rulesWithCustomPredicate.add(rule);
        }

        return this;
    }

    private CastRule<?, ?> internalResolve(LogicalType input, LogicalType target) {
        LogicalType inputType = unwrapDistinct(input);
        LogicalType targetType = unwrapDistinct(target);

        final Iterator<Object> targetTypeRootFamilyIterator =
                Stream.concat(
                                Stream.of(targetType),
                                Stream.<Object>concat(
                                        Stream.of(targetType.getTypeRoot()),
                                        targetType.getTypeRoot().getFamilies().stream()))
                        .iterator();

        // Try lookup by target type root/type families
        while (targetTypeRootFamilyIterator.hasNext()) {
            final Object targetMapKey = targetTypeRootFamilyIterator.next();
            final Map<Object, CastRule<?, ?>> inputTypeToCastRuleMap = rules.get(targetMapKey);

            if (inputTypeToCastRuleMap == null) {
                continue;
            }

            // Try lookup by input type root/type families
            Optional<? extends CastRule<?, ?>> rule =
                    Stream.<Object>concat(
                                    Stream.of(inputType.getTypeRoot()),
                                    inputType.getTypeRoot().getFamilies().stream())
                            .map(inputTypeToCastRuleMap::get)
                            .filter(Objects::nonNull)
                            .findFirst();

            if (rule.isPresent()) {
                return rule.get();
            }
        }

        // Try with the custom predicate rules
        return rulesWithCustomPredicate.stream()
                .filter(
                        r ->
                                r.getPredicateDefinition()
                                        .getCustomPredicate()
                                        .map(p -> p.test(inputType, targetType))
                                        .orElse(false))
                .findFirst()
                .orElse(null);
    }

    private LogicalType unwrapDistinct(LogicalType logicalType) {
        if (logicalType.is(LogicalTypeRoot.DISTINCT_TYPE)) {
            return unwrapDistinct(((DistinctType) logicalType).getSourceType());
        }
        return logicalType;
    }
}
