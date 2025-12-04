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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * ALTER CONNECTION [IF EXISTS] [[catalogName.] dataBasesName.]connectionName RENAME TO
 * newConnectionName.
 */
public class SqlAlterConnectionRename extends SqlAlterConnection {

    private final SqlIdentifier newConnectionName;

    public SqlAlterConnectionRename(
            SqlParserPos pos,
            SqlIdentifier connectionName,
            SqlIdentifier newConnectionName,
            boolean ifConnectionExists) {
        super(pos, connectionName, ifConnectionExists);
        this.newConnectionName =
                requireNonNull(newConnectionName, "newConnectionName should not be null");
    }

    public SqlIdentifier getNewConnectionName() {
        return newConnectionName;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(name, newConnectionName);
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword("RENAME TO");
        newConnectionName.unparse(writer, leftPrec, rightPrec);
    }
}
