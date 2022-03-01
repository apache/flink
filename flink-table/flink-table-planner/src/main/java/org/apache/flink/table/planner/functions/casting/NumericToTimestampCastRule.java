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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * {@link LogicalTypeFamily#NUMERIC} to {@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * LogicalTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} cast rule. Disable cast conversion between
 * Numeric type and Timestamp type and suggest to use {@code TO_TIMESTAMP()}/{@code
 * TO_TIMESTAMP_LTZ()} instead.
 */
class NumericToTimestampCastRule
        extends AbstractExpressionCodeGeneratorCastRule<Number, TimestampData> {

    static final NumericToTimestampCastRule INSTANCE = new NumericToTimestampCastRule();

    private NumericToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.NUMERIC)
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
            throw new ValidationException(
                    "The cast from NUMERIC type to TIMESTAMP type "
                            + "is not allowed. It's recommended to use "
                            + "TO_TIMESTAMP(FROM_UNIXTIME(numeric_col)) "
                            + "instead, note the numeric is in seconds.");
        } else if (targetLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            throw new ValidationException(
                    "The cast from NUMERIC type"
                            + " to TIMESTAMP_LTZ type is not allowed. It's recommended to use"
                            + " TO_TIMESTAMP_LTZ(numeric_col, precision) instead.");
        } else {
            throw new IllegalArgumentException("This is a bug. Please file an issue.");
        }
    }
}
