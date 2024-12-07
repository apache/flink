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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * SqlNode to describe ALTER MATERIALIZED TABLE [catalogName.] [dataBasesName.]tableName SET
 * FRESHNESS = INTERVAL '&lt;num&gt;' { SECOND | MINUTE | HOUR | DAY } clause.
 */
public class SqlAlterMaterializedTableFreshness extends SqlAlterMaterializedTable {

    private final SqlIntervalLiteral freshness;

    public SqlAlterMaterializedTableFreshness(
            SqlParserPos pos, SqlIdentifier tableName, SqlIntervalLiteral freshness) {
        super(pos, tableName);
        this.freshness = freshness;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getTableName(), freshness);
    }

    public SqlIntervalLiteral getFreshness() {
        return freshness;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("SET FRESHNESS");
        writer.keyword("=");
        freshness.unparse(writer, leftPrec, rightPrec);
    }
}
