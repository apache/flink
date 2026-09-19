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

package org.apache.flink.sql.parser.ddl.table;

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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/** ALTER TABLE [IF EXISTS] [[catalogName.] dataBasesName].tableName RESET ( 'key1' [, 'key2']*). */
public class SqlAlterTableReset extends SqlAlterTable {
    private static final SqlSpecialOperator RESET_OPERATOR =
            new SqlSpecialOperator("ALTER TABLE RESET", SqlKind.ALTER_TABLE) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlAlterTableReset(
                            pos,
                            (SqlIdentifier) operands[0],
                            (SqlNodeList) operands[1],
                            ((SqlLiteral) operands[2]).booleanValue());
                }
            };

    private final SqlNodeList propertyKeyList;

    public SqlAlterTableReset(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList propertyKeyList,
            boolean ifTableExists) {
        super(pos, tableName, null, ifTableExists);
        this.propertyKeyList =
                requireNonNull(propertyKeyList, "propertyKeyList should not be null");
    }

    @Override
    public SqlOperator getOperator() {
        return RESET_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableIdentifier,
                propertyKeyList,
                SqlLiteral.createBoolean(ifTableExists, SqlParserPos.ZERO));
    }

    public SqlNodeList getPropertyKeyList() {
        return propertyKeyList;
    }

    public Set<String> getResetKeys() {
        return SqlParseUtils.extractSet(propertyKeyList, SqlParseUtils::extractString);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseResetOptions(propertyKeyList, writer, leftPrec, rightPrec);
    }
}
