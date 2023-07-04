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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * {@link SqlNode} to describe the [CREATE OR] REPLACE TABLE AS (RTAS) syntax. The RTAS would create
 * a pipeline to compute the result of the given query and create or replace the derived table.
 *
 * <p>Notes: REPLACE TABLE AS: the derived table must exist. CREATE OR REPLACE TABLE AS: create the
 * derived table if it does not exist, otherwise replace it.
 *
 * <p>Example:
 *
 * <pre>{@code
 * CREATE TABLE base_table (
 *     id BIGINT,
 *     name STRING,
 *     time TIMESTAMP,
 *     PRIMARY KEY(id)
 * ) WITH (
 *     ‘connector’ = ‘kafka’,
 *     ‘connector.starting-offset’: ‘12345’,
 *     ‘format’ =  ‘json’
 * )
 *
 * CREATE OR REPLACE TABLE derived_table
 * WITH (
 *   'connector' = 'jdbc',
 *   'url' = 'http://localhost:10000',
 *   'table-name' = 'syncedTable'
 * )
 * AS SELECT * FROM base_table;
 * }</pre>
 */
public class SqlReplaceTableAs extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator REPLACE_OPERATOR =
            new SqlSpecialOperator("REPLACE TABLE AS", SqlKind.OTHER_DDL);

    public static final SqlSpecialOperator CREATE_OR_REPLACE_OPERATOR =
            new SqlSpecialOperator("CREATE OR REPLACE TABLE AS", SqlKind.OTHER_DDL);

    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final SqlNodeList propertyList;

    private final List<SqlTableConstraint> tableConstraints;

    private final SqlNodeList partitionKeyList;

    private final SqlWatermark watermark;

    private final SqlCharStringLiteral comment;

    private final boolean isTemporary;

    private final boolean isCreateOrReplace;

    private final SqlNode asQuery;

    public SqlReplaceTableAs(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlNodeList partitionKeyList,
            @Nullable SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            SqlNode asQuery,
            boolean isTemporary,
            boolean ifNotExists,
            boolean isCreateOrReplace) {
        super(
                isCreateOrReplace ? CREATE_OR_REPLACE_OPERATOR : REPLACE_OPERATOR,
                pos,
                true,
                ifNotExists);

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

        this.asQuery = asQuery;
        this.isCreateOrReplace = isCreateOrReplace;
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
                comment,
                asQuery);
    }

    @Override
    public void validate() throws SqlValidateException {
        SqlConstraintValidator.validateAndChangeColumnNullability(tableConstraints, columnList);
        // The following features are not currently supported by RTAS, but may be supported in the
        // future
        String errorMsg =
                isCreateOrReplace ? "CREATE OR REPLACE TABLE AS SELECT" : "REPLACE TABLE AS SELECT";

        if (isIfNotExists()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    errorMsg + " syntax does not support IF NOT EXISTS statements yet.");
        }

        if (isTemporary()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    errorMsg + " syntax does not support temporary table yet.");
        }

        if (getColumnList().size() > 0) {
            throw new SqlValidateException(
                    getParserPosition(),
                    errorMsg + " syntax does not support to specify explicit columns yet.");
        }

        if (getWatermark().isPresent()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    errorMsg + " syntax does not support to specify explicit watermark yet.");
        }
        if (getPartitionKeyList().size() > 0) {
            throw new SqlValidateException(
                    getParserPosition(),
                    errorMsg + " syntax does not support to create partitioned table yet.");
        }
        if (getFullConstraints().stream().anyMatch(SqlTableConstraint::isPrimaryKey)) {
            throw new SqlValidateException(
                    getParserPosition(),
                    errorMsg + " syntax does not support primary key constraints yet.");
        }
    }

    public SqlNode getAsQuery() {
        return asQuery;
    }

    public boolean isCreateOrReplace() {
        return isCreateOrReplace;
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

    /** Returns the column constraints plus the table constraints. */
    public List<SqlTableConstraint> getFullConstraints() {
        return SqlConstraintValidator.getFullConstraints(tableConstraints, columnList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (isCreateOrReplace) {
            writer.keyword("CREATE OR");
        }
        writer.keyword("REPLACE TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
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

        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        this.asQuery.unparse(writer, leftPrec, rightPrec);
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }
}
