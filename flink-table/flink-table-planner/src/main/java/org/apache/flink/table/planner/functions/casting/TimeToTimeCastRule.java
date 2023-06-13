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

import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeRoot#TIME_WITHOUT_TIME_ZONE} to {@link LogicalTypeRoot#TIME_WITHOUT_TIME_ZONE}.
 */
class TimeToTimeCastRule extends AbstractExpressionCodeGeneratorCastRule<Number, Number> {

    static final TimeToTimeCastRule INSTANCE = new TimeToTimeCastRule();

    private TimeToTimeCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .target(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
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

        if (inputPrecision <= targetPrecision) {
            return inputTerm;
        } else {
            return staticCall(BuiltInMethods.TRUNCATE_SQL_TIME(), inputTerm, targetPrecision);
        }
    }
}
