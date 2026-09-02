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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * Base class for the rules that cast a {@link
 * org.apache.flink.table.types.logical.LogicalTypeRoot#VARIANT} to a constructed target, imposing a
 * schema on a variant. A constructed cast is the scalar cast applied to every leaf plus a shape
 * check at each level, so the recursion bottoms out at the same scalar cast the primitive and
 * string rules perform and no new leaf semantics are introduced.
 *
 * <p>A constructed cast can always fail, on a shape mismatch, an unreadable leaf, or a missing
 * {@code NOT NULL} field, so {@code TRY_CAST} wraps the whole value and returns {@code NULL} for
 * any failure rather than a partial result.
 */
abstract class AbstractVariantToConstructedCastRule<OUT>
        extends AbstractNullAwareCodeGeneratorCastRule<Variant, OUT> {

    protected AbstractVariantToConstructedCastRule(CastRulePredicate predicate) {
        super(predicate);
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    /**
     * Treats a variant that stores a JSON {@code null} as a {@code NULL} input, so a top-level JSON
     * null casts to SQL {@code NULL} before any shape check runs. Only applied for a nullable
     * target: a {@code NOT NULL} result cannot carry {@code NULL}, so a null-valued variant then
     * fails the shape check as a regular mismatch.
     */
    @Override
    public CastCodeBlock generateCodeBlock(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String inputIsNullTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (!targetLogicalType.isNullable()) {
            return super.generateCodeBlock(
                    context, inputTerm, inputIsNullTerm, inputLogicalType, targetLogicalType);
        }
        final String isNullTerm =
                "(" + inputIsNullTerm + " || " + methodCall(inputTerm, "isNull") + ")";
        return super.generateCodeBlock(
                context, inputTerm, isNullTerm, inputLogicalType, targetLogicalType);
    }
}
