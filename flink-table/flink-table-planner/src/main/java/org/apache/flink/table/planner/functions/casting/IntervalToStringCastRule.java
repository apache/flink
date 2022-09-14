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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.lang.reflect.Method;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.INTERVAL_DAY_TIME_TO_STRING;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.INTERVAL_YEAR_MONTH_TO_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;

/** {@link LogicalTypeFamily#INTERVAL} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
class IntervalToStringCastRule extends AbstractCharacterFamilyTargetRule<Object> {

    static final IntervalToStringCastRule INSTANCE = new IntervalToStringCastRule();

    private IntervalToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.INTERVAL)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public String generateStringExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final Method method =
                inputLogicalType.is(LogicalTypeRoot.INTERVAL_YEAR_MONTH)
                        ? INTERVAL_YEAR_MONTH_TO_STRING()
                        : INTERVAL_DAY_TIME_TO_STRING();
        return staticCall(method, inputTerm);
    }
}
