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

import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * SqlNode to describe ALTER TABLE [IF EXISTS] table_name DROP column clause.
 *
 * <p>Example: DDL like the below for drop column.
 *
 * <pre>{@code
 * -- drop single column
 * ALTER TABLE prod.db.sample DROP col1;
 *
 * -- drop multiple columns
 * ALTER TABLE prod.db.sample DROP (col1, col2, col3);
 * }</pre>
 */
public class SqlAlterTableDropColumn extends SqlAlterTable {

    private final SqlNodeList columnList;

    public SqlAlterTableDropColumn(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            boolean ifTableExists) {
        super(pos, tableName, ifTableExists);
        this.columnList = columnList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(tableIdentifier, columnList);
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("DROP");
        // unparse table column
        SqlUnparseUtils.unparseTableSchema(
                writer, leftPrec, rightPrec, columnList, Collections.emptyList(), null);
    }
}
