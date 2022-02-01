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
 * {@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * LogicalTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} to {@link LogicalTypeFamily#NUMERIC} cast rule.
 * Disable cast conversion between Timestamp type and Numeric type and suggest to use {@code
 * UNIX_TIMESTAMP()} instead (for {@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}.
 */
class TimestampToNumericCastRule
        extends AbstractExpressionCodeGeneratorCastRule<TimestampData, Number> {

    static final TimestampToNumericCastRule INSTANCE = new TimestampToNumericCastRule();

    private TimestampToNumericCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .target(LogicalTypeFamily.NUMERIC)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            throw new ValidationException(
                    "The cast from TIMESTAMP type to NUMERIC type"
                            + " is not allowed. It's recommended to use"
                            + " UNIX_TIMESTAMP(CAST(timestamp_col AS STRING)) instead.");
        } else if (inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            throw new ValidationException(
                    "The cast from" + " TIMESTAMP_LTZ type to NUMERIC type is not allowed.");
        } else {
            throw new IllegalArgumentException("This is a bug. Please file an issue.");
        }
    }
}
