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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SqlNode to describe ALTER TABLE table_name ADD column/constraint/watermark clause.
 *
 * <p>Example: DDL like the below for add column/constraint/watermark.
 *
 * <pre>{@code
 * -- add single column
 * ALTER TABLE mytable ADD new_column STRING COMMENT 'new_column docs';
 *
 * -- add multiple columns, constraint, and watermark
 * ALTER TABLE mytable ADD (
 *     log_ts STRING COMMENT 'log timestamp string' FIRST,
 *     ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
 *     PRIMARY KEY (id) NOT ENFORCED,
 *     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
 * );
 * }</pre>
 */
public class SqlAlterTableAdd extends SqlAlterTable {

    // Whether the column is added by a paren, currently it is only used for SQL unparse.
    private final boolean withParen;
    private final SqlNodeList addedColumns;
    @Nullable private final SqlWatermark watermark;
    private final List<SqlTableConstraint> constraint;

    public SqlAlterTableAdd(
            SqlParserPos pos,
            SqlIdentifier tableName,
            boolean withParen,
            SqlNodeList addedColumns,
            @Nullable SqlWatermark sqlWatermark,
            List<SqlTableConstraint> constraint) {
        super(pos, tableName, null);
        this.withParen = withParen;
        this.addedColumns = addedColumns;
        this.watermark = sqlWatermark;
        this.constraint = constraint;
    }

    /** Returns the column constraints plus the table constraints. */
    public List<SqlTableConstraint> getFullConstraints() {
        List<SqlTableConstraint> ret = new ArrayList<>();
        this.addedColumns.getList().stream()
                .map(SqlTableColumnPosition.class::cast)
                .map(SqlTableColumnPosition::getColumn)
                .forEach(
                        column -> {
                            if (column instanceof SqlTableColumn.SqlRegularColumn) {
                                SqlTableColumn.SqlRegularColumn regularColumn =
                                        (SqlTableColumn.SqlRegularColumn) column;
                                regularColumn.getConstraint().map(ret::add);
                            }
                        });
        ret.addAll(this.constraint);
        return ret;
    }

    public SqlNodeList getColumns() {
        return addedColumns;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                getTableName(),
                addedColumns,
                watermark,
                new SqlNodeList(constraint, SqlParserPos.ZERO));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");
        // Distinguish whether the added column/watermark/constraint is in a paren
        if (withParen) {
            SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
            for (SqlNode column : addedColumns.getList()) {
                printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            if (constraint.size() > 0) {
                for (SqlTableConstraint constraint : constraint) {
                    printIndent(writer);
                    constraint.unparse(writer, leftPrec, rightPrec);
                }
            }
            if (watermark != null) {
                printIndent(writer);
                watermark.unparse(writer, leftPrec, rightPrec);
            }

            writer.newlineAndIndent();
            writer.endList(frame);
        } else {
            if (addedColumns.getList().size() == 1) {
                // add single column case
                addedColumns.getList().get(0).unparse(writer, leftPrec, rightPrec);
            } else if (watermark != null) {
                // add watermark case
                watermark.unparse(writer, leftPrec, rightPrec);
            }
            // TODO: add constraint case
        }
    }
}
