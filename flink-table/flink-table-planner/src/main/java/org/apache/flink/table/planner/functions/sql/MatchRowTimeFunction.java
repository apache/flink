/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import java.util.List;

/**
 * The function used to access a rowtime attribute with TIMESTAMP or TIMESTAMP_LTZ type from
 * MATCH_RECOGNIZE clause. The function accepts zero or one operand which is a field reference with
 * rowtime attribute. If there is no operand, the function will return rowtime attribute with
 * TIMESTAMP type. Otherwise, the return type will be same with the operand type.
 */
public class MatchRowTimeFunction extends SqlFunction {

    public MatchRowTimeFunction() {
        super(
                "MATCH_ROWTIME",
                SqlKind.OTHER_FUNCTION,
                null,
                null,
                null,
                SqlFunctionCategory.MATCH_RECOGNIZE);
    }

    @Override
    public String getAllowedSignatures(String opNameToUse) {
        return "MATCH_ROWTIME([rowtime_field])";
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(0, 1);
    }

    public String getSignatureTemplate(final int operandsCount) {
        switch (operandsCount) {
            case 0:
                return "{}";
            case 1:
                return "{0})";
            default:
                throw new AssertionError();
        }
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        List<SqlNode> operands = callBinding.operands();
        int n = operands.size();
        assert n == 0 || n == 1;
        if (n == 0) {
            return true;
        } else {
            SqlNode operand = callBinding.operand(0);
            if (operand.getKind() != SqlKind.IDENTIFIER) {
                if (throwOnFailure) {
                    throw new ValidationException(
                            String.format(
                                    "The function %s requires a field reference as argument, but actual argument is not a simple field reference.",
                                    callBinding.getOperator().getName()));
                } else {
                    return false;
                }
            }
            RelDataType operandType = callBinding.getOperandType(0);
            if (FlinkTypeFactory.isRowtimeIndicatorType(operandType)) {
                return true;
            } else {
                if (throwOnFailure) {
                    throw new ValidationException(
                            String.format(
                                    "The function %s requires argument to be a row time attribute type, but is '%s'.",
                                    callBinding.getOperator().getName(), operandType));
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        // Returns rowtime if there is no argument
        if (opBinding.getOperandCount() == 0) {
            final FlinkTypeFactory typeFactory = (FlinkTypeFactory) opBinding.getTypeFactory();
            return typeFactory.createRowtimeIndicatorType(false, false);
        }
        return opBinding.getOperandType(0);
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }
}
