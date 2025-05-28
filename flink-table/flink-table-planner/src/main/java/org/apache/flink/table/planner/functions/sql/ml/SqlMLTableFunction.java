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

package org.apache.flink.table.planner.functions.sql.ml;

import org.apache.flink.table.api.ValidationException;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Base class for a table-valued function that works with models. Examples include {@code
 * ML_PREDICT}.
 */
public abstract class SqlMLTableFunction extends SqlFunction implements SqlTableFunction {

    private static final String TABLE_INPUT_ERROR =
            "SqlMLTableFunction must have only one table as first operand.";

    protected static final String PARAM_INPUT = "INPUT";
    protected static final String PARAM_MODEL = "MODEL";
    protected static final String PARAM_COLUMN = "ARGS";
    protected static final String PARAM_CONFIG = "CONFIG";

    public SqlMLTableFunction(String name, SqlOperandMetadata operandMetadata) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                null,
                operandMetadata,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        assert call.getOperator() == this;
        final List<SqlNode> operandList = call.getOperandList();

        // ML table function should take only one table as input and use descriptor to reference
        // columns in the table. The scope for descriptor validation should be the input table which
        // is also an operand of the call. We defer the validation of the descriptor since
        // validation here will quality the descriptor columns to be NOT simple name which
        // complicates checks in later stages. We validate the descriptor columns appear in table
        // column in SqlOperandMetadata.
        boolean foundSelect = false;
        for (SqlNode operand : operandList) {
            if (operand.getKind().equals(SqlKind.DESCRIPTOR)) {
                continue;
            }
            if (operand.getKind().equals(SqlKind.SET_SEMANTICS_TABLE)) {
                operand = ((SqlCall) operand).getOperandList().get(0);
                if (foundSelect) {
                    throw new ValidationException(TABLE_INPUT_ERROR);
                }
                foundSelect = true;
            }

            if (operand.getKind().equals(SqlKind.SELECT)) {
                if (foundSelect) {
                    throw new ValidationException(TABLE_INPUT_ERROR);
                }
                foundSelect = true;
            }
            operand.validate(validator, scope);
        }
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return this::inferRowType;
    }

    protected abstract RelDataType inferRowType(SqlOperatorBinding opBinding);
}
