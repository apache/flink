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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * SqlNode to describe ALTER DDL for drop constraint clause, here supports two kinds of syntax as
 * follows.
 *
 * <pre>{@code
 * -- drop primary key
 *  ALTER TABLE [catalog_name.][db_name.]table_name DROP PRIMARY KEY;
 *
 *  -- drop constraint
 *  ALTER TABLE [catalog_name.][db_name.]table_name DROP CONSTRAINT constraint_name;
 * }</pre>
 */
public class SqlAlterTableDropConstraint extends SqlAlterTable {

    private final boolean isPrimaryKey;
    private final @Nullable SqlIdentifier constraintName;

    /**
     * Creates an alter table drop constraint node.
     *
     * @param tableID Table ID
     * @param constraintName Constraint name
     * @param pos Parser position
     */
    public SqlAlterTableDropConstraint(
            SqlParserPos pos,
            SqlIdentifier tableID,
            boolean isPrimaryKey,
            @Nullable SqlIdentifier constraintName) {
        super(pos, tableID);
        this.isPrimaryKey = isPrimaryKey;
        this.constraintName = constraintName;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public Optional<SqlIdentifier> getConstraintName() {
        return Optional.ofNullable(constraintName);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getTableName(), this.constraintName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        if (isPrimaryKey) {
            writer.keyword("DROP PRIMARY KEY");
        } else {
            writer.keyword("DROP CONSTRAINT");
            this.constraintName.unparse(writer, leftPrec, rightPrec);
        }
    }
}
