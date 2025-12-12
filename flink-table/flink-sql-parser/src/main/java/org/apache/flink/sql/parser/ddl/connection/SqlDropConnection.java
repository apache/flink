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

import org.apache.flink.sql.parser.ddl.SqlDropObject;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * {@link org.apache.calcite.sql.SqlNode} to describe the DROP CONNECTION [IF EXISTS]
 * [[catalogName.] dataBasesName].connectionName syntax.
 */
public class SqlDropConnection extends SqlDropObject {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP CONNECTION", SqlKind.OTHER_DDL);

    private final boolean isTemporary;
    private final boolean isSystemConnection;

    public SqlDropConnection(
            SqlParserPos pos,
            SqlIdentifier connectionName,
            boolean ifExists,
            boolean isTemporary,
            boolean isSystemConnection) {
        super(OPERATOR, pos, connectionName, ifExists);
        this.isTemporary = isTemporary;
        this.isSystemConnection = isSystemConnection;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isSystemConnection() {
        return isSystemConnection;
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
        name.unparse(writer, leftPrec, rightPrec);
    }
}
