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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** The class copied from Calcite in order to turn off COALESCE rewrite with CASE ... WHEN ... */
public class SqlCoalesceFunction extends SqlFunction {
    // ~ Constructors -----------------------------------------------------------

    public SqlCoalesceFunction() {
        // NOTE jvs 26-July-2006:  We fill in the type strategies here,
        // but normally they are not used because the validator invokes
        // rewriteCall to convert COALESCE into CASE early.  However,
        // validator rewrite can optionally be disabled, in which case these
        // strategies are used.
        super(
                "COALESCE",
                SqlKind.COALESCE,
                ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.LEAST_NULLABLE),
                null,
                OperandTypes.SAME_VARIADIC,
                SqlFunctionCategory.SYSTEM);
    }

    // ~ Methods ----------------------------------------------------------------

    // ----- FLINK MODIFICATION BEGIN -----
    // override SqlOperator
    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        validateQuantifier(validator, call); // check DISTINCT/ALL

        List<SqlNode> operands = call.getOperandList();

        if (operands.size() == 1) {
            return operands.get(0);
        }

        SqlParserPos pos = call.getParserPosition();
        List<SqlNode> nodes = new ArrayList<>();
        for (SqlNode operand : operands) {
            if (!SqlUtil.isNullLiteral(operand, false)) {
                nodes.add(operand);
            }
        }

        if (nodes.isEmpty()) {
            return SqlLiteral.createNull(pos);
        }
        if (nodes.size() == 1) {
            return nodes.get(0);
        }

        return new SqlBasicCall(this, nodes, pos);
    }

    // ----- FLINK MODIFICATION END -----

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return requireNonNull(super.getReturnTypeInference(), "returnTypeInference");
    }
}
