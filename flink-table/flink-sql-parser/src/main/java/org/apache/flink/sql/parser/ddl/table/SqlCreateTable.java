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

package org.apache.flink.sql.parser.ddl.table;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateObject;
import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** CREATE TABLE DDL sql call. */
public class SqlCreateTable extends SqlCreateObject implements ExtendedSqlNode {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlNodeList columnList;

    private final List<SqlTableConstraint> tableConstraints;

    private final SqlDistribution distribution;

    protected final SqlNodeList partitionKeyList;

    private final SqlWatermark watermark;

    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlDistribution distribution,
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
                distribution,
                partitionKeyList,
                watermark,
                comment,
                isTemporary,
                ifNotExists,
                false);
    }

    protected SqlCreateTable(
            SqlSpecialOperator operator,
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            @Nullable SqlDistribution distribution,
            SqlNodeList partitionKeyList,
            @Nullable SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            boolean isTemporary,
            boolean ifNotExists,
            boolean replace) {
        super(operator, pos, tableName, isTemporary, replace, ifNotExists, propertyList, comment);
        this.columnList = requireNonNull(columnList, "columnList should not be null");
        this.tableConstraints =
                requireNonNull(tableConstraints, "table constraints should not be null");
        requireNonNull(propertyList, "propertyList should not be null");
        this.distribution = distribution;
        this.partitionKeyList =
                requireNonNull(partitionKeyList, "partitionKeyList should not be null");
        this.watermark = watermark;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                name,
                columnList,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
                properties,
                partitionKeyList,
                watermark,
                comment);
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public final SqlDistribution getDistribution() {
        return distribution;
    }

    public List<String> getPartitionKeyList() {
        return SqlParseUtils.extractList(partitionKeyList, p -> ((SqlIdentifier) p).getSimple());
    }

    public List<SqlTableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    @Override
    protected String getScope() {
        return "TABLE";
    }

    @Override
    public void validate() throws SqlValidateException {
        SqlConstraintValidator.validateAndChangeColumnNullability(tableConstraints, columnList);
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
        unparseCreateIfNotExists(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseTableSchema(
                columnList, tableConstraints, watermark, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseComment(comment, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseDistribution(distribution, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparsePartitionKeyList(partitionKeyList, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseProperties(properties, writer, leftPrec, rightPrec);
    }
}
