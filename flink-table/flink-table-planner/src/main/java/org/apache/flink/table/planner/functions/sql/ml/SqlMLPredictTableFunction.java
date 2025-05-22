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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.List;

/**
 * SqlMlPredictTableFunction implements an operator for prediction.
 *
 * <p>It allows four parameters:
 *
 * <ol>
 *   <li>a table
 *   <li>a model name
 *   <li>a descriptor to provide a column name from the input table
 *   <li>an optional config map
 * </ol>
 */
public class SqlMLPredictTableFunction extends SqlMLTableFunction {

    public SqlMLPredictTableFunction() {
        super("ML_PREDICT", new PredictOperandMetadata());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides because the first parameter of table-value function windowing is an explicit
     * TABLE parameter, which is not scalar.
     */
    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    @Override
    protected RelDataType inferRowType(SqlOperatorBinding opBinding) {
        // TODO: FLINK-37780 output type based on table schema and model output schema
        // model output schema to be available after integrated with SqlExplicitModelCall
        return opBinding.getOperandType(1);
    }

    private static class PredictOperandMetadata implements SqlOperandMetadata {
        private static final List<String> PARAM_NAMES =
                List.of(PARAM_INPUT, PARAM_MODEL, PARAM_COLUMN, PARAM_CONFIG);
        private static final List<String> MANDATORY_PARAM_NAMES =
                List.of(PARAM_INPUT, PARAM_MODEL, PARAM_COLUMN);

        PredictOperandMetadata() {}

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
            return Collections.nCopies(
                    PARAM_NAMES.size(), typeFactory.createSqlType(SqlTypeName.ANY));
        }

        @Override
        public List<String> paramNames() {
            return PARAM_NAMES;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            // TODO: FLINK-37780 Check operand types after integrated with SqlExplicitModelCall in
            // validator
            return false;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(MANDATORY_PARAM_NAMES.size(), PARAM_NAMES.size());
        }

        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

        @Override
        public boolean isOptional(int i) {
            return i > getOperandCountRange().getMin() && i <= getOperandCountRange().getMax();
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_name, MODEL model_name, DESCRIPTOR(input_columns), [MAP[]]";
        }
    }
}
