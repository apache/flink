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

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Alter Function Sql Call. */
public class SqlAlterFunction extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER FUNCTION", SqlKind.OTHER_DDL);

    private final SqlIdentifier functionIdentifier;

    private final SqlCharStringLiteral functionClassName;

    private final String functionLanguage;

    private final boolean ifExists;

    private final boolean isSystemFunction;

    private final boolean isTemporary;

    public SqlAlterFunction(
            SqlParserPos pos,
            SqlIdentifier functionIdentifier,
            SqlCharStringLiteral functionClassName,
            String functionLanguage,
            boolean ifExists,
            boolean isTemporary,
            boolean isSystemFunction) {
        super(pos);
        this.functionIdentifier =
                requireNonNull(functionIdentifier, "functionIdentifier should not be null");
        this.functionClassName =
                requireNonNull(functionClassName, "functionClassName should not be null");
        this.isSystemFunction = requireNonNull(isSystemFunction);
        this.isTemporary = isTemporary;
        this.functionLanguage = functionLanguage;
        this.ifExists = ifExists;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER");
        if (isTemporary) {
            writer.keyword("TEMPORARY");
        }
        if (isSystemFunction) {
            writer.keyword("SYSTEM");
        }
        writer.keyword("FUNCTION");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        functionIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        functionClassName.unparse(writer, leftPrec, rightPrec);
        if (functionLanguage != null) {
            writer.keyword("LANGUAGE");
            writer.keyword(functionLanguage);
        }
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(functionIdentifier, functionClassName);
    }

    public String getFunctionLanguage() {
        return functionLanguage;
    }

    public SqlCharStringLiteral getFunctionClassName() {
        return this.functionClassName;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isSystemFunction() {
        return isSystemFunction;
    }

    public boolean isIfExists() {
        return this.ifExists;
    }

    public String[] getFunctionIdentifier() {
        return functionIdentifier.names.toArray(new String[0]);
    }
}
