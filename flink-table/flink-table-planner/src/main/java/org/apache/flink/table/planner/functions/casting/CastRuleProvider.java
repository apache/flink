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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                .addRule(RowToStringCastRule.INSTANCE)
                .addRule(RawToStringCastRule.INSTANCE)
                // Collection rules
                .addRule(ArrayToArrayCastRule.INSTANCE)
                // Special rules
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

    /* ------ Implementation ------ */

    // Map<Target family or root, Map<Input family or root, rule>>
    private final Map<Object, Map<Object, CastRule<?, ?>>> rules = new HashMap<>();
    private final List<CastRule<?, ?>> rulesWithCustomPredicate = new ArrayList<>();

    private CastRuleProvider addRule(CastRule<?, ?> rule) {
        CastRulePredicate predicate = rule.getPredicateDefinition();

        for (LogicalTypeRoot targetTypeRoot : predicate.getTargetTypes()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeRoot, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypes()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (LogicalTypeFamily targetTypeFamily : predicate.getTargetTypeFamilies()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeFamily, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypes()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }

        if (predicate.getCustomPredicate() != null) {
            rulesWithCustomPredicate.add(rule);
        }

        return this;
    }

    private CastRule<?, ?> internalResolve(LogicalType inputType, LogicalType targetType) {
        // Try lookup by target type root/type families
        final Map<Object, CastRule<?, ?>> inputTypeToCastRuleMap =
                lookupTypeInMap(rules, targetType.getTypeRoot());
        CastRule<?, ?> rule;
        if (inputTypeToCastRuleMap != null) {
            // Try lookup by input type root/type families
            rule = lookupTypeInMap(inputTypeToCastRuleMap, inputType.getTypeRoot());
            if (rule != null) {
                return rule;
            }
        }

        // Try with the custom predicate rules
        rule =
                rulesWithCustomPredicate.stream()
                        .filter(
                                r ->
                                        r.getPredicateDefinition()
                                                .getCustomPredicate()
                                                .test(inputType, targetType))
                        .findFirst()
                        .orElse(null);

        return rule;
    }

    /**
     * Function that performs a map lookup first based on the type root, then on any of its
     * families.
     */
    private static <T> T lookupTypeInMap(Map<Object, T> map, LogicalTypeRoot type) {
        T out = map.get(type);
        if (out == null) {
            /* lookup by any family matching */
            for (LogicalTypeFamily family : type.getFamilies()) {
                out = map.get(family);
                if (out != null) {
                    return out;
                }
            }
        }
        return out;
    }
}
