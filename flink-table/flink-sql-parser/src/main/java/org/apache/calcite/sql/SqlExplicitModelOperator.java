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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** SqlExplicitModelOperator is a SQL operator that represents an explicit model. */
public class SqlExplicitModelOperator extends SqlPrefixOperator {

    public SqlExplicitModelOperator(
            int prec,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker) {
        super(
                "MODEL",
                SqlKind.OTHER_FUNCTION,
                prec,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker);
    }

    @Override
    public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
        pos = pos.plusAll(operands);
        return new SqlExplicitModelCall(
                this, ImmutableNullableList.copyOf(operands), pos, functionQualifier);
    }
}
