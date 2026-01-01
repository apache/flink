/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.sql.parser.ddl.table;

import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;

/**
 * SqlNode to describe ALTER TABLE [IF EXISTS ]table_name DROP column/constraint/watermark clause.
 *
 * <p>Example: DDL like the below for dropping column/constraint/watermark.
 *
 * <pre>{@code
 * -- drop a column (only drop of non persisted is allowed)
 * ALTER MATERIALIZED TABLE materializedTable DROP col1;
 *
 * -- drop several columns
 * ALTER MATERIALIZED TABLE materializedTable DROP (col1, col2, col3);
 *
 * -- drop a primary key
 * ALTER MATERIALIZED TABLE materializedTable DROP PRIMARY KEY;
 *
 * -- drop a constraint by name
 * ALTER MATERIALIZED TABLE materializedTable DROP CONSTRAINT constraint_name;
 *
 * -- drop a watermark
 * ALTER MATERIALIZED TABLE materializedTable DROP WATERMARK;
 * }</pre>
 */
public abstract class SqlAlterTableDrop extends SqlAlterTableSchema {

    public SqlAlterTableDrop(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList modifiedColumns,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark watermark,
            boolean ifTableExists) {
        super(pos, tableName, modifiedColumns, constraints, watermark, ifTableExists);
    }

    public SqlAlterTableDrop(SqlParserPos pos, SqlIdentifier tableName, boolean ifTableExists) {
        super(pos, tableName, SqlNodeList.EMPTY, List.of(), null, ifTableExists);
    }

    protected abstract void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec);

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(tableIdentifier);
    }

    @Override
    protected String getAlterOperation() {
        return "DROP";
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        unparseDropOperation(writer, leftPrec, rightPrec);
    }

    /** ALTER TABLE [IF EXISTS ][catalog_name.][db_name.]table_name DROP PRIMARY KEY. */
    public static class SqlAlterTableDropPrimaryKey extends SqlAlterTableDrop {

        public SqlAlterTableDropPrimaryKey(
                SqlParserPos pos, SqlIdentifier tableName, boolean ifTableExists) {
            super(pos, tableName, ifTableExists);
        }

        @Override
        protected void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("PRIMARY KEY");
        }
    }

    /**
     * ALTER TABLE [IF EXISTS ][catalog_name.][db_name.]table_name DROP CONSTRAINT constraint_name.
     */
    public static class SqlAlterTableDropConstraint extends SqlAlterTableDrop {
        private final SqlIdentifier constraintName;

        public SqlAlterTableDropConstraint(
                SqlParserPos pos,
                SqlIdentifier tableName,
                SqlIdentifier constraintName,
                boolean ifTableExists) {
            super(pos, tableName, ifTableExists);
            this.constraintName = constraintName;
        }

        public SqlIdentifier getConstraintName() {
            return constraintName;
        }

        @Override
        protected void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("CONSTRAINT");
            constraintName.unparse(writer, leftPrec, rightPrec);
        }
    }

    /** ALTER TABLE [IF EXISTS ][catalog_name.][db_name.]table_name DROP WATERMARK. */
    public static class SqlAlterTableDropWatermark extends SqlAlterTableDrop {

        public SqlAlterTableDropWatermark(
                SqlParserPos pos, SqlIdentifier tableName, boolean ifTableExists) {
            super(pos, tableName, ifTableExists);
        }

        @Override
        protected void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("WATERMARK");
        }
    }

    /**
     * SqlNode to describe ALTER TABLE [IF EXISTS ]table_name DROP column clause.
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
    public static class SqlAlterTableDropColumn extends SqlAlterTableDrop {

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
            return List.of(tableIdentifier, columnList);
        }

        public SqlNodeList getColumnList() {
            return columnList;
        }

        @Override
        protected void unparseDropOperation(SqlWriter writer, int leftPrec, int rightPrec) {
            // unparse table column
            SqlUnparseUtils.unparseTableSchema(
                    columnList, List.of(), null, writer, leftPrec, rightPrec);
        }
    }
}
