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
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** CREATE MATERIALIZED TABLE DDL sql call. */
public class SqlCreateMaterializedTable extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE MATERIALIZED TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final List<SqlTableConstraint> tableConstraints;

    private final @Nullable SqlCharStringLiteral comment;

    private final @Nullable SqlDistribution distribution;

    private final SqlNodeList partitionKeyList;

    private final SqlWatermark watermark;

    private final SqlNodeList propertyList;

    private final @Nullable SqlIntervalLiteral freshness;

    private final @Nullable SqlLiteral refreshMode;

    private final SqlNode asQuery;

    public SqlCreateMaterializedTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            @Nullable SqlDistribution distribution,
            SqlNodeList partitionKeyList,
            SqlNodeList propertyList,
            @Nullable SqlIntervalLiteral freshness,
            @Nullable SqlLiteral refreshMode,
            SqlNode asQuery) {
        super(OPERATOR, pos, false, false);
        this.tableName = requireNonNull(tableName, "tableName should not be null");
        this.columnList = columnList;
        this.tableConstraints = tableConstraints;
        this.watermark = watermark;
        this.comment = comment;
        this.distribution = distribution;
        this.partitionKeyList =
                requireNonNull(partitionKeyList, "partitionKeyList should not be null");
        this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
        this.freshness = freshness;
        this.refreshMode = refreshMode;
        this.asQuery = requireNonNull(asQuery, "asQuery should not be null");
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableName,
                columnList,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
                watermark,
                comment,
                partitionKeyList,
                propertyList,
                freshness,
                asQuery);
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }

    public SqlNodeList getColumnList() {
        return columnList;
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

    public @Nullable SqlDistribution getDistribution() {
        return distribution;
    }

    public SqlNodeList getPartitionKeyList() {
        return partitionKeyList;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    @Nullable
    public SqlIntervalLiteral getFreshness() {
        return freshness;
    }

    @Nullable
    public SqlLiteral getRefreshMode() {
        return refreshMode;
    }

    public SqlNode getAsQuery() {
        return asQuery;
    }

    /** Returns the column constraints plus the table constraints. */
    public List<SqlTableConstraint> getFullConstraints() {
        return SqlConstraintValidator.getFullConstraints(tableConstraints, columnList);
    }

    @Override
    public void validate() throws SqlValidateException {
        if (!isSchemaWithColumnsIdentifiersOnly()) {
            SqlConstraintValidator.validateAndChangeColumnNullability(tableConstraints, columnList);
        }
    }

    public boolean isSchemaWithColumnsIdentifiersOnly() {
        // CREATE MATERIALIZED TABLE supports passing only column identifiers in the column list. If
        // the first column in the list is an identifier, then we assume the rest of the
        // columns are identifiers as well.
        return !getColumnList().isEmpty() && getColumnList().get(0) instanceof SqlIdentifier;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE MATERIALIZED TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);

        if (!columnList.isEmpty() || !tableConstraints.isEmpty() || watermark != null) {
            SqlUnparseUtils.unparseTableSchema(
                    writer, leftPrec, rightPrec, columnList, tableConstraints, watermark);
        }

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (distribution != null) {
            writer.newlineAndIndent();
            distribution.unparse(writer, leftPrec, rightPrec);
        }

        if (!partitionKeyList.isEmpty()) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            partitionKeyList.unparse(writer, leftPrec, rightPrec);
            writer.endList(partitionedByFrame);
        }

        if (!propertyList.isEmpty()) {
            writer.newlineAndIndent();
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                SqlUnparseUtils.printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }

        if (freshness != null) {
            writer.newlineAndIndent();
            writer.keyword("FRESHNESS");
            writer.keyword("=");
            freshness.unparse(writer, leftPrec, rightPrec);
        }

        if (refreshMode != null) {
            writer.newlineAndIndent();
            writer.keyword("REFRESH_MODE");
            writer.keyword("=");
            refreshMode.unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        asQuery.unparse(writer, leftPrec, rightPrec);
    }
}
