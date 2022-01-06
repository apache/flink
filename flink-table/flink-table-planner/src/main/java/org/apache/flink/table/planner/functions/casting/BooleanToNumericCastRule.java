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

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveLiteralForType;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.DECIMAL_ZERO;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.INTEGRAL_TO_DECIMAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.ternaryOperator;

/** {@link LogicalTypeRoot#BOOLEAN} to {@link LogicalTypeFamily#NUMERIC} conversions. */
class BooleanToNumericCastRule extends AbstractExpressionCodeGeneratorCastRule<Boolean, Number> {

    static final BooleanToNumericCastRule INSTANCE = new BooleanToNumericCastRule();

    private BooleanToNumericCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.BOOLEAN)
                        .target(LogicalTypeFamily.NUMERIC)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return ternaryOperator(
                inputTerm, trueValue(targetLogicalType), falseValue(targetLogicalType));
    }

    private String trueValue(LogicalType target) {
        switch (target.getTypeRoot()) {
            case DECIMAL:
                DecimalType decimalType = (DecimalType) target;
                return staticCall(
                        INTEGRAL_TO_DECIMAL(),
                        1,
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case TINYINT:
                return primitiveLiteralForType((byte) 1);
            case SMALLINT:
                return primitiveLiteralForType((short) 1);
            case INTEGER:
                return primitiveLiteralForType(1);
            case BIGINT:
                return primitiveLiteralForType(1L);
            case FLOAT:
                return primitiveLiteralForType(1f);
            case DOUBLE:
                return primitiveLiteralForType(1d);
        }
        throw new IllegalArgumentException("This is a bug. Please file an issue.");
    }

    private String falseValue(LogicalType target) {
        switch (target.getTypeRoot()) {
            case DECIMAL:
                DecimalType decimalType = (DecimalType) target;
                return staticCall(
                        DECIMAL_ZERO(), decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return primitiveLiteralForType((byte) 0);
            case SMALLINT:
                return primitiveLiteralForType((short) 0);
            case INTEGER:
                return primitiveLiteralForType(0);
            case BIGINT:
                return primitiveLiteralForType(0L);
            case FLOAT:
                return primitiveLiteralForType(0f);
            case DOUBLE:
                return primitiveLiteralForType(0d);
        }
        throw new IllegalArgumentException("This is a bug. Please file an issue.");
    }
}
