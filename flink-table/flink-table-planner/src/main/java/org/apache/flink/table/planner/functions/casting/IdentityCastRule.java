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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

/**
 * Identity cast rule. For more details on when the rule is applied, check {@link
 * #isIdentityCast(LogicalType, LogicalType)}
 */
class IdentityCastRule extends AbstractCodeGeneratorCastRule<Object, Object>
        implements ExpressionCodeGeneratorCastRule<Object, Object> {

    static final IdentityCastRule INSTANCE = new IdentityCastRule();

    private IdentityCastRule() {
        super(CastRulePredicate.builder().predicate(IdentityCastRule::isIdentityCast).build());
    }

    private static boolean isIdentityCast(
            LogicalType inputLogicalType, LogicalType targetLogicalType) {
        // INTERVAL_YEAR_MONTH and INTEGER uses the same primitive int type
        if ((inputLogicalType.is(LogicalTypeRoot.INTERVAL_YEAR_MONTH)
                        && targetLogicalType.is(LogicalTypeRoot.INTEGER))
                || (inputLogicalType.is(LogicalTypeRoot.INTEGER)
                        && targetLogicalType.is(LogicalTypeRoot.INTERVAL_YEAR_MONTH))) {
            return true;
        }

        // INTERVAL_DAY_TIME and BIGINT uses the same primitive long type
        if ((inputLogicalType.is(LogicalTypeRoot.INTERVAL_DAY_TIME)
                        && targetLogicalType.is(LogicalTypeRoot.BIGINT))
                || (inputLogicalType.is(LogicalTypeRoot.BIGINT)
                        && targetLogicalType.is(LogicalTypeRoot.INTERVAL_DAY_TIME))) {
            return true;
        }

        return LogicalTypeCasts.supportsAvoidingCast(inputLogicalType, targetLogicalType);
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return inputTerm;
    }

    @Override
    public CastCodeBlock generateCodeBlock(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String inputIsNullTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return CastCodeBlock.withoutCode(inputTerm, inputIsNullTerm);
    }
}
