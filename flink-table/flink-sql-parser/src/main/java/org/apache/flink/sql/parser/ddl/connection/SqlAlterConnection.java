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

import org.apache.flink.sql.parser.ddl.SqlAlterObject;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Abstract class to describe statements like ALTER CONNECTION [IF EXISTS] [[catalogName.]
 * dataBasesName.]connectionName ...
 */
public abstract class SqlAlterConnection extends SqlAlterObject {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER CONNECTION", SqlKind.OTHER_DDL);

    protected final boolean ifConnectionExists;

    public SqlAlterConnection(
            SqlParserPos pos, SqlIdentifier connectionName, boolean ifConnectionExists) {
        super(OPERATOR, pos, "CONNECTION", connectionName);
        this.ifConnectionExists = ifConnectionExists;
    }

    /**
     * Whether to ignore the error if the connection doesn't exist.
     *
     * @return true when IF EXISTS is specified.
     */
    public boolean ifConnectionExists() {
        return ifConnectionExists;
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        if (ifConnectionExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }
}
