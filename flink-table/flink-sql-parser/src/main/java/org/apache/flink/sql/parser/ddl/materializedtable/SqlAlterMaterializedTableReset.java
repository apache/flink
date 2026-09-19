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

package org.apache.flink.sql.parser.ddl.materializedtable;

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

/**
 * SqlNode to describe ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name RESET ('key')
 * clause.
 */
public class SqlAlterMaterializedTableReset extends SqlAlterMaterializedTable {

    private static final SqlSpecialOperator RESET_OPERATOR =
            new SqlSpecialOperator("ALTER MATERIALIZED TABLE RESET", SqlKind.ALTER_TABLE) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlAlterMaterializedTableReset(
                            pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
                }
            };

    private final SqlNodeList propertyKeyList;

    public SqlAlterMaterializedTableReset(
            SqlParserPos pos, SqlIdentifier tableName, SqlNodeList propertyKeyList) {
        super(pos, tableName);
        this.propertyKeyList = propertyKeyList;
    }

    public SqlNodeList getPropertyKeyList() {
        return propertyKeyList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, propertyKeyList);
    }

    @Override
    public SqlOperator getOperator() {
        return RESET_OPERATOR;
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseResetOptions(propertyKeyList, writer, leftPrec, rightPrec);
    }
}
