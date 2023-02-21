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

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.DateTimeUtils;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.operator;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * LogicalTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} to {@link LogicalTypeRoot#DATE}.
 */
class TimestampToDateCastRule
        extends AbstractExpressionCodeGeneratorCastRule<TimestampData, Number> {

    static final TimestampToDateCastRule INSTANCE = new TimestampToDateCastRule();

    private TimestampToDateCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .target(LogicalTypeRoot.DATE)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {

        if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return cast(
                    "int",
                    operator(
                            methodCall(inputTerm, "getMillisecond"),
                            "/",
                            DateTimeUtils.MILLIS_PER_DAY));
        } else if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return staticCall(
                    BuiltInMethods.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE(),
                    inputTerm,
                    context.getSessionTimeZoneTerm());
        } else {
            throw new IllegalArgumentException("This is a bug. Please file an issue.");
        }
    }
}
