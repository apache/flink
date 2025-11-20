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

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * {@link SqlNode} to describe the DROP CONNECTION [IF EXISTS] [[catalogName.]
 * dataBasesName].connectionName syntax.
 */
public class SqlDropConnection extends SqlDrop {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP CONNECTION", SqlKind.OTHER_DDL);

    private SqlIdentifier connectionName;
    private final boolean isTemporary;
    private final boolean isSystemConnection;

    public SqlDropConnection(
            SqlParserPos pos,
            SqlIdentifier connectionName,
            boolean ifExists,
            boolean isTemporary,
            boolean isSystemConnection) {
        super(OPERATOR, pos, ifExists);
        this.connectionName = connectionName;
        this.isTemporary = isTemporary;
        this.isSystemConnection = isSystemConnection;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(connectionName);
    }

    public SqlIdentifier getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(SqlIdentifier connectionName) {
        this.connectionName = connectionName;
    }

    public boolean getIfExists() {
        return this.ifExists;
    }

    public boolean getIsTemporary() {
        return this.isTemporary;
    }

    public boolean getIsSystemConnection() {
        return this.isSystemConnection;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        if (isTemporary) {
            writer.keyword("TEMPORARY");
        }
        if (isSystemConnection) {
            writer.keyword("SYSTEM");
        }
        writer.keyword("CONNECTION");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        connectionName.unparse(writer, leftPrec, rightPrec);
    }

    public String[] fullConnectionName() {
        return connectionName.names.toArray(new String[0]);
    }
}
