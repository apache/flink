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
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/** SqlExplicitModelOperator is a SQL operator that represents an explicit model. */
public class SqlExplicitModelOperator extends SqlPrefixOperator {

    public SqlExplicitModelOperator(int prec) {
        super("MODEL", SqlKind.OTHER_FUNCTION, prec, null, null, null);
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

    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        throw SqlUtil.newContextException(
                call.pos, RESOURCE.objectNotFound(call.operand(0).toString()));
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        throw SqlUtil.newContextException(
                call.pos, RESOURCE.objectNotFound(call.operand(0).toString()));
    }
}
