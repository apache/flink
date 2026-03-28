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

package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/** SHOW CREATE [OR ALTER ]MATERIALIZED TABLE sql call. */
public class SqlShowCreateMaterializedTable extends SqlShowCreate {
    private static final SqlSpecialOperator SHOW_CREATE_OPERATOR =
            new SqlSpecialOperator("SHOW CREATE MATERIALIZED TABLE", SqlKind.OTHER_DDL);

    private static final SqlSpecialOperator SHOW_CREATE_OR_ALTER_OPERATOR =
            new SqlSpecialOperator("SHOW CREATE OR ALTER MATERIALIZED TABLE", SqlKind.OTHER_DDL);

    private final boolean createOrAlter;

    public SqlShowCreateMaterializedTable(
            SqlParserPos pos, SqlIdentifier sqlIdentifier, boolean createOrAlter) {
        super(pos, sqlIdentifier);
        this.createOrAlter = createOrAlter;
    }

    public SqlIdentifier getMaterializedTableName() {
        return sqlIdentifier;
    }

    public String[] getFullMaterializedTableName() {
        return sqlIdentifier.names.toArray(new String[0]);
    }

    @Override
    public SqlOperator getOperator() {
        return createOrAlter ? SHOW_CREATE_OR_ALTER_OPERATOR : SHOW_CREATE_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(sqlIdentifier);
    }

    public boolean isCreateOrAlter() {
        return createOrAlter;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        sqlIdentifier.unparse(writer, leftPrec, rightPrec);
    }
}
