/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * ALTER TABLE [IF EXISTS] [catalog_name.][db_name.]table_name ADD [CONSTRAINT constraint_name]
 * (PRIMARY KEY | UNIQUE) (column, ...) [[NOT] ENFORCED].
 */
public class SqlAlterTableAddConstraint extends SqlAlterTable {
    private final SqlTableConstraint constraint;

    /**
     * Creates a add table constraint node.
     *
     * @param tableID Table ID
     * @param constraint Table constraint
     * @param pos Parser position
     * @param ifTableExists Whether IF EXISTS is specified
     */
    public SqlAlterTableAddConstraint(
            SqlIdentifier tableID,
            SqlTableConstraint constraint,
            SqlParserPos pos,
            boolean ifTableExists) {
        super(pos, tableID, ifTableExists);
        this.constraint = constraint;
    }

    public SqlTableConstraint getConstraint() {
        return constraint;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getTableName(), this.constraint);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");
        this.constraint.unparse(writer, leftPrec, rightPrec);
    }
}
