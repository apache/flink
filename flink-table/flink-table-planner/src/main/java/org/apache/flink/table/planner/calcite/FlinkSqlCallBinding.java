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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.planner.functions.sql.SqlDefaultOperator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Binding supports to rewrite the DEFAULT operator. */
public class FlinkSqlCallBinding extends SqlCallBinding {

    private final List<RelDataType> fixArgumentTypes;

    private final List<SqlNode> rewrittenOperands;

    public FlinkSqlCallBinding(
            SqlValidator validator, @Nullable SqlValidatorScope scope, SqlCall call) {
        super(validator, scope, call);
        this.fixArgumentTypes = getFixArgumentTypes();
        this.rewrittenOperands = getRewrittenOperands();
    }

    @Override
    public int getOperandCount() {
        return rewrittenOperands.size();
    }

    @Override
    public List<SqlNode> operands() {
        if (isFixedParameters()) {
            return rewrittenOperands;
        } else {
            return super.operands();
        }
    }

    @Override
    public RelDataType getOperandType(int ordinal) {
        if (!isFixedParameters()) {
            return super.getOperandType(ordinal);
        }

        SqlNode operand = rewrittenOperands.get(ordinal);
        if (operand.getKind() == SqlKind.DEFAULT) {
            return fixArgumentTypes.get(ordinal);
        }

        final RelDataType type = SqlTypeUtil.deriveType(this, operand);
        final SqlValidatorNamespace namespace = getValidator().getNamespace(operand);
        return namespace != null ? namespace.getType() : type;
    }

    public boolean isFixedParameters() {
        return !fixArgumentTypes.isEmpty();
    }

    private List<RelDataType> getFixArgumentTypes() {
        SqlOperandTypeChecker sqlOperandTypeChecker = getOperator().getOperandTypeChecker();
        if (sqlOperandTypeChecker instanceof SqlOperandMetadata
                && sqlOperandTypeChecker.isFixedParameters()) {
            return ((SqlOperandMetadata) sqlOperandTypeChecker).paramTypes(getTypeFactory());
        }
        return Collections.emptyList();
    }

    private List<SqlNode> getRewrittenOperands() {
        if (!isFixedParameters()) {
            return super.operands();
        }

        List<SqlNode> rewrittenOperands = new ArrayList<>();
        for (SqlNode operand : super.operands()) {
            if (operand instanceof SqlCall
                    && ((SqlCall) operand).getOperator() == SqlStdOperatorTable.DEFAULT) {
                rewrittenOperands.add(
                        new SqlDefaultOperator(fixArgumentTypes.get(rewrittenOperands.size()))
                                .createCall(SqlParserPos.ZERO));
            } else {
                rewrittenOperands.add(operand);
            }
        }
        return rewrittenOperands;
    }
}
