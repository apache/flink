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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * SqlSessionTableFunction implements an operator for session.
 *
 * <p>It allows three parameters:
 *
 * <ol>
 *   <li>a table
 *   <li>a descriptor to provide a time attribute column name from the input table
 *   <li>an interval parameter to specify an inactive activity gap to break sessions
 * </ol>
 */
public class SqlSessionTableFunction extends SqlWindowTableFunction {

    public SqlSessionTableFunction() {
        super(SqlKind.SESSION.name(), new OperandMetadataImpl());
    }

    /** Operand type checker for SESSION. */
    private static class OperandMetadataImpl extends AbstractOperandMetadata {
        OperandMetadataImpl() {
            super(ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, PARAM_KEY, PARAM_SESSION_GAP), 3);
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }

            final SqlValidator validator = callBinding.getValidator();
            final SqlNode operand2 = callBinding.operand(2);
            final RelDataType type2 = validator.getValidatedNodeType(operand2);
            if (operand2.getKind() == SqlKind.DESCRIPTOR) {
                final SqlNode operand0 = callBinding.operand(0);
                final RelDataType type = validator.getValidatedNodeType(operand0);
                validateColumnNames(
                        validator, type.getFieldNames(), ((SqlCall) operand2).getOperandList());
            } else if (!SqlTypeUtil.isInterval(type2)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            if (callBinding.getOperandCount() > 3) {
                final RelDataType type3 = validator.getValidatedNodeType(callBinding.operand(3));
                if (!SqlTypeUtil.isInterval(type3)) {
                    return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
                }
            }
            // check time attribute
            return throwExceptionOrReturnFalse(
                    checkTimeColumnDescriptorOperand(callBinding, 1), throwOnFailure);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_name, DESCRIPTOR(timecol), "
                    + "DESCRIPTOR(key) optional, datetime interval)";
        }
    }
}
