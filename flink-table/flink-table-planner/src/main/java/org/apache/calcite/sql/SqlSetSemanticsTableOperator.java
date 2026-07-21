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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Copied from Calcite because of CALCITE-7660. Should be removed after upgrade to Calcite 1.43.0.
 */
public class SqlSetSemanticsTableOperator extends SqlInternalOperator {

    // ~ Constructors -----------------------------------------------------------

    public SqlSetSemanticsTableOperator() {
        super("SET_SEMANTICS_TABLE", SqlKind.SET_SEMANTICS_TABLE);
    }

    @Override
    public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
        assert operands.length == 3;
        SqlNode partitionList = operands[1];
        SqlNode orderList = operands[2];
        assert (partitionList != null && !SqlNodeList.isEmptyList(partitionList))
                || (orderList != null && !SqlNodeList.isEmptyList(orderList));
        return super.createCall(functionQualifier, pos, operands);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        call.operand(0).unparse(writer, 0, 0);

        SqlNodeList partitionList = call.operand(1);
        if (!partitionList.isEmpty()) {
            writer.sep("PARTITION BY");
            // FLINK MODIFICATION BEGIN
            final SqlWriter.Frame partitionFrame =
                    partitionList.size() == 1
                            ? writer.startList("", "")
                            : writer.startList("(", ")");
            // FLINK MODIFICATION END
            partitionList.unparse(writer, 0, 0);
            writer.endList(partitionFrame);
        }
        SqlNodeList orderList = call.operand(2);
        if (!orderList.isEmpty()) {
            writer.sep("ORDER BY");
            // FLINK MODIFICATION BEGIN
            final SqlWriter.Frame orderFrame =
                    orderList.size() == 1 ? writer.startList("", "") : writer.startList("(", ")");
            orderList.unparse(writer, 0, 0);
            writer.endList(orderFrame);
            // FLINK MODIFICATION END
        }
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        final List<SqlNode> operands = call.getOperandList();
        return requireNonNull(validator.deriveType(scope, operands.get(0)));
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return false;
    }
}
