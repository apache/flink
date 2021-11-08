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

package org.apache.flink.table.planner.functions.casting.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.functions.casting.CastRulePredicate;
import org.apache.flink.table.planner.functions.casting.CodeGeneratorCastRule;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import static org.apache.flink.table.types.logical.LogicalTypeFamily.APPROXIMATE_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.INTEGER_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH;

/**
 * Cast rule for {@link LogicalTypeFamily#INTEGER_NUMERIC} and {@link
 * LogicalTypeFamily#APPROXIMATE_NUMERIC} and {@link LogicalTypeFamily#INTERVAL} conversions.
 */
@Internal
public class NumericPrimitiveCastRule
        extends AbstractExpressionCodeGeneratorCastRule<Number, Number> {

    public static final NumericPrimitiveCastRule INSTANCE = new NumericPrimitiveCastRule();

    private NumericPrimitiveCastRule() {
        super(CastRulePredicate.builder().predicate(NumericPrimitiveCastRule::matches).build());
    }

    private static boolean matches(LogicalType x, LogicalType y) {
        // Exclude identity casting
        if (x.is(y.getTypeRoot())) {
            return false;
        }

        // Conversions between primitive numerics
        if ((x.is(INTEGER_NUMERIC) || x.is(APPROXIMATE_NUMERIC))
                && (y.is(INTEGER_NUMERIC) || y.is(APPROXIMATE_NUMERIC))) {
            return true;
        }

        // Conversions between Interval year month (int) and bigint (long)
        if ((x.is(INTERVAL_YEAR_MONTH) && y.is(BIGINT))
                || (x.is(BIGINT) && y.is(INTERVAL_YEAR_MONTH))) {
            return true;
        }

        // Conversions between Interval day time (long) and integer (int)
        return (x.is(INTERVAL_DAY_TIME) && y.is(INTEGER))
                || (x.is(INTEGER) && y.is(INTERVAL_DAY_TIME));
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return CastRuleUtils.castToPrimitive(targetLogicalType, inputTerm);
    }
}
