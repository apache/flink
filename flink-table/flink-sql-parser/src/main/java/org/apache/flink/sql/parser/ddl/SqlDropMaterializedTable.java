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

/** DROP MATERIALIZED TABLE DDL sql call. */
public class SqlDropMaterializedTable extends SqlDrop {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP MATERIALIZED TABLE", SqlKind.DROP_TABLE);

    private final SqlIdentifier tableIdentifier;

    public SqlDropMaterializedTable(
            SqlParserPos pos, SqlIdentifier tableIdentifier, boolean ifExists) {
        super(OPERATOR, pos, ifExists);
        this.tableIdentifier = tableIdentifier;
    }

    public String[] fullTableName() {
        return tableIdentifier.names.toArray(new String[0]);
    }

    public boolean getIfExists() {
        return this.ifExists;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP MATERIALIZED TABLE");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        tableIdentifier.unparse(writer, leftPrec, rightPrec);
    }
}
