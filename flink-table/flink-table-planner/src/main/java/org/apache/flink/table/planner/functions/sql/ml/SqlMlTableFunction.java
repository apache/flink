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
public abstract class SqlMlTableFunction extends SqlFunction implements SqlTableFunction {

    protected static final String PARAM_DATA = "data";
    protected static final String PARAM_MODEL = "input_model";
    protected static final String PARAM_COLUMN = "input_column";
    protected static final String PARAM_CONFIG = "config";

    public SqlMlTableFunction(String name, SqlOperandMetadata operandMetadata) {
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
        // Validation for DESCRIPTOR is broken, and we
        // make assumptions at different locations those are not validated and not properly scoped.
        // Theoretically, we should scope identifiers of the above to the result of the subquery
        // from the first argument. Unfortunately this breaks at other locations which do not expect
        // it. We run additional validations while deriving the return type, therefore we can skip
        // it here.

        for (SqlNode operand : operandList) {
            if (operand.getKind().equals(SqlKind.DESCRIPTOR)) {
                continue;
            }
            if (operand.getKind().equals(SqlKind.SET_SEMANTICS_TABLE)) {
                operand = ((SqlCall) operand).getOperandList().get(0);
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
