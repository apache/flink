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

package org.apache.flink.sql.parser.dml;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/**
 * SqlNode to describe TRUNCATE TABLE statement.
 *
 * <p>We parse TRUNCATE TABLE statement in Flink since Calcite doesn't support TRUNCATE TABLE
 * statement currently. Should remove the parse logic for TRUNCATE TABLE statement from Flink after
 * the Calcite used by Flink includes [CALCITE-5688].
 */
public class SqlTruncateTable extends SqlCall {

    private final SqlIdentifier tableNameIdentifier;

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.OTHER);

    public SqlTruncateTable(SqlParserPos pos, SqlIdentifier tableNameIdentifier) {
        super(pos);
        this.tableNameIdentifier = tableNameIdentifier;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(tableNameIdentifier);
    }

    public String[] fullTableName() {
        return tableNameIdentifier.names.toArray(new String[0]);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("TRUNCATE TABLE");
        tableNameIdentifier.unparse(writer, leftPrec, rightPrec);
    }
}
