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

package org.apache.flink.sql.parser.ddl.connection;

import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * ALTER CONNECTION [IF EXISTS] [[catalogName.] dataBasesName.]connectionName RESET ( 'key1' [,
 * 'key2']...).
 */
public class SqlAlterConnectionReset extends SqlAlterConnection {
    private static final SqlSpecialOperator RESET_OPERATOR =
            new SqlSpecialOperator("ALTER CONNECTION RESET", SqlKind.OTHER_DDL) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlAlterConnectionReset(
                            pos,
                            (SqlIdentifier) operands[0],
                            ((SqlLiteral) operands[2]).booleanValue(),
                            (SqlNodeList) operands[1]);
                }
            };

    private final SqlNodeList optionKeyList;

    public SqlAlterConnectionReset(
            SqlParserPos pos,
            SqlIdentifier connectionName,
            boolean ifConnectionExists,
            SqlNodeList optionKeyList) {
        super(pos, connectionName, ifConnectionExists);
        this.optionKeyList = requireNonNull(optionKeyList, "optionKeyList should not be null");
    }

    @Override
    public SqlOperator getOperator() {
        return RESET_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(
                name,
                optionKeyList,
                SqlLiteral.createBoolean(ifConnectionExists, SqlParserPos.ZERO));
    }

    public Set<String> getResetKeys() {
        return SqlParseUtils.extractSet(optionKeyList, SqlParseUtils::extractString);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseResetOptions(optionKeyList, writer, leftPrec, rightPrec);
    }
}
