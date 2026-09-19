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

package org.apache.flink.sql.parser.ddl.catalog;

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
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** ALTER CATALOG catalog_name SET (key1=val1, ...). */
public class SqlAlterCatalogOptions extends SqlAlterCatalog {

    private static final SqlSpecialOperator OPTIONS_OPERATOR =
            new SqlSpecialOperator("ALTER CATALOG OPTIONS", SqlKind.OTHER_DDL) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlAlterCatalogOptions(
                            pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
                }
            };

    private final SqlNodeList propertyList;

    public SqlAlterCatalogOptions(
            SqlParserPos position, SqlIdentifier catalogName, SqlNodeList propertyList) {
        super(position, catalogName);
        this.propertyList = requireNonNull(propertyList, "propertyList cannot be null");
    }

    @Override
    public SqlOperator getOperator() {
        return OPTIONS_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, propertyList);
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public Map<String, String> getProperties() {
        return SqlParseUtils.extractMap(propertyList);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseSetOptions(propertyList, writer, leftPrec, rightPrec);
    }
}
