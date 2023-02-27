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

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** CREATE TABLE DDL sql call. */
public class SqlCreateTable extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final SqlNodeList propertyList;

    private final List<SqlTableConstraint> tableConstraints;

    private final SqlNodeList partitionKeyList;

    private final SqlWatermark watermark;

    private final SqlCharStringLiteral comment;

    private final boolean isTemporary;

    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlNodeList partitionKeyList,
            @Nullable SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            boolean isTemporary,
            boolean ifNotExists) {
        this(
                OPERATOR,
                pos,
                tableName,
                columnList,
                tableConstraints,
                propertyList,
                partitionKeyList,
                watermark,
                comment,
                isTemporary,
                ifNotExists);
    }

    protected SqlCreateTable(
            SqlSpecialOperator operator,
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlNodeList partitionKeyList,
            @Nullable SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            boolean isTemporary,
            boolean ifNotExists) {
        super(operator, pos, false, ifNotExists);
        this.tableName = requireNonNull(tableName, "tableName should not be null");
        this.columnList = requireNonNull(columnList, "columnList should not be null");
        this.tableConstraints =
                requireNonNull(tableConstraints, "table constraints should not be null");
        this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
        this.partitionKeyList =
                requireNonNull(partitionKeyList, "partitionKeyList should not be null");
        this.watermark = watermark;
        this.comment = comment;
        this.isTemporary = isTemporary;
    }

    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableName,
                columnList,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
                propertyList,
                partitionKeyList,
                watermark,
                comment);
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public SqlNodeList getPartitionKeyList() {
        return partitionKeyList;
    }

    public List<SqlTableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    public Optional<SqlCharStringLiteral> getComment() {
        return Optional.ofNullable(comment);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public void validate() throws SqlValidateException {
        SqlConstraintValidator.validateAndChangeColumnNullability(tableConstraints, columnList);
    }

    public boolean hasRegularColumnsOnly() {
        for (SqlNode column : columnList) {
            final SqlTableColumn tableColumn = (SqlTableColumn) column;
            if (!(tableColumn instanceof SqlRegularColumn)) {
                return false;
            }
        }
        return true;
    }

    /** Returns the column constraints plus the table constraints. */
    public List<SqlTableConstraint> getFullConstraints() {
        return SqlConstraintValidator.getFullConstraints(tableConstraints, columnList);
    }

    /**
     * Returns the projection format of the DDL columns(including computed columns). i.e. the
     * following DDL:
     *
     * <pre>
     *   create table tbl1(
     *     col1 int,
     *     col2 varchar,
     *     col3 as to_timestamp(col2)
     *   ) with (
     *     'connector' = 'csv'
     *   )
     * </pre>
     *
     * <p>is equivalent with query "col1, col2, to_timestamp(col2) as col3", caution that the
     * "computed column" operands have been reversed.
     */
    public String getColumnSqlString() {
        SqlPrettyWriter writer =
                new SqlPrettyWriter(
                        SqlPrettyWriter.config()
                                .withDialect(AnsiSqlDialect.DEFAULT)
                                .withAlwaysUseParentheses(true)
                                .withSelectListItemsOnSeparateLines(false)
                                .withIndentation(0));
        writer.startList("", "");
        for (SqlNode column : columnList) {
            writer.sep(",");
            SqlTableColumn tableColumn = (SqlTableColumn) column;
            if (tableColumn instanceof SqlComputedColumn) {
                SqlComputedColumn computedColumn = (SqlComputedColumn) tableColumn;
                computedColumn.getExpr().unparse(writer, 0, 0);
                writer.keyword("AS");
            }
            tableColumn.getName().unparse(writer, 0, 0);
        }

        return writer.toString();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword("TABLE");
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        tableName.unparse(writer, leftPrec, rightPrec);
        if (columnList.size() > 0 || tableConstraints.size() > 0 || watermark != null) {
            SqlUnparseUtils.unparseTableSchema(
                    writer, leftPrec, rightPrec, columnList, tableConstraints, watermark);
        }

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (this.partitionKeyList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            this.partitionKeyList.unparse(writer, leftPrec, rightPrec);
            writer.endList(partitionedByFrame);
            writer.newlineAndIndent();
        }

        if (this.propertyList.size() > 0) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                SqlUnparseUtils.printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    /** Table creation context. */
    public static class TableCreationContext {
        public List<SqlNode> columnList = new ArrayList<>();
        public List<SqlTableConstraint> constraints = new ArrayList<>();
        @Nullable public SqlWatermark watermark;
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }
}
