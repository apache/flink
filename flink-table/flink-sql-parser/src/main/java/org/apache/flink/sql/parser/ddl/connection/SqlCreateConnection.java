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

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateObject;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

/**
 * {@link SqlNode} to describe the CREATE CONNECTION syntax. CREATE [TEMPORARY] [SYSTEM] CONNECTION
 * [IF NOT EXISTS] [[catalogName.] dataBasesName].connectionName [COMMENT connection_comment] WITH
 * (name=value, [name=value]*).
 */
public class SqlCreateConnection extends SqlCreateObject implements ExtendedSqlNode {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE CONNECTION", SqlKind.OTHER_DDL);

    private final boolean isSystem;

    public SqlCreateConnection(
            SqlParserPos pos,
            SqlIdentifier connectionName,
            SqlCharStringLiteral comment,
            SqlNodeList propertyList,
            boolean isTemporary,
            boolean isSystem,
            boolean ifNotExists) {
        super(
                OPERATOR,
                pos,
                connectionName,
                isTemporary,
                false,
                ifNotExists,
                propertyList,
                comment);
        this.isSystem = isSystem;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, comment, properties);
    }

    public boolean isSystem() {
        return isSystem;
    }

    @Override
    public void validate() throws SqlValidateException {
        if (properties == null || properties.isEmpty()) {
            throw new SqlValidateException(
                    getParserPosition(), "Connection property list can not be empty.");
        }
    }

    @Override
    protected String getScope() {
        return "CONNECTION";
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        if (isSystem) {
            writer.keyword("SYSTEM");
        }
        writer.keyword("CONNECTION");
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseComment(comment, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseProperties(properties, writer, leftPrec, rightPrec);
    }
}
