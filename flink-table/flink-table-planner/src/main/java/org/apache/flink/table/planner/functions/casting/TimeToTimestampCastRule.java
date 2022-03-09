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

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeRoot#TIME_WITHOUT_TIME_ZONE} to {@link
 * LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * LogicalTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} cast rule.
 */
class TimeToTimestampCastRule
        extends AbstractExpressionCodeGeneratorCastRule<Number, TimestampData> {

    static final TimeToTimestampCastRule INSTANCE = new TimeToTimestampCastRule();

    private TimeToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .target(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .target(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {

        if (targetLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return staticCall(BuiltInMethods.TIMESTAMP_FROM_EPOCH_MILLIS(), inputTerm);
        } else if (targetLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return staticCall(
                    BuiltInMethods.TIME_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                    inputTerm,
                    context.getSessionTimeZoneTerm());
        } else {
            throw new IllegalArgumentException("This is a bug. Please file an issue.");
        }
    }
}
