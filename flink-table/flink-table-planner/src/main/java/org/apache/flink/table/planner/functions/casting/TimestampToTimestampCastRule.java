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
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * LogicalTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} to {@link
 * LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}
 * cast rule. Check and adjust if there is the precision changes.
 */
class TimestampToTimestampCastRule
        extends AbstractExpressionCodeGeneratorCastRule<TimestampData, TimestampData> {

    static final TimestampToTimestampCastRule INSTANCE = new TimestampToTimestampCastRule();

    private TimestampToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
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
        final int inputPrecision = LogicalTypeChecks.getPrecision(inputLogicalType);
        int targetPrecision = LogicalTypeChecks.getPrecision(targetLogicalType);

        if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && targetLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            final TimestampKind inputTimestampKind = ((TimestampType) inputLogicalType).getKind();
            final TimestampKind targetTimestampKind = ((TimestampType) targetLogicalType).getKind();
            if (inputTimestampKind == TimestampKind.ROWTIME
                    || inputTimestampKind == TimestampKind.PROCTIME
                    || targetTimestampKind == TimestampKind.ROWTIME
                    || targetTimestampKind == TimestampKind.PROCTIME) {
                targetPrecision = 3;
            }
        }

        final String operand;
        if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && targetLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            operand =
                    staticCall(
                            BuiltInMethods.TIMESTAMP_TO_TIMESTAMP_WITH_LOCAL_ZONE(),
                            inputTerm,
                            context.getSessionTimeZoneTerm());
        } else if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && targetLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            operand =
                    staticCall(
                            BuiltInMethods.TIMESTAMP_WITH_LOCAL_ZONE_TO_TIMESTAMP(),
                            inputTerm,
                            context.getSessionTimeZoneTerm());
        } else {
            operand = inputTerm;
        }

        if (inputPrecision <= targetPrecision) {
            return operand;
        } else {
            return staticCall(BuiltInMethods.TRUNCATE_SQL_TIMESTAMP(), operand, targetPrecision);
        }
    }
}
