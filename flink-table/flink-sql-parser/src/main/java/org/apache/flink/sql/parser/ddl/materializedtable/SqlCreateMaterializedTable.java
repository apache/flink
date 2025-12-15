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

package org.apache.flink.sql.parser.ddl.materializedtable;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateObject;
import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** CREATE MATERIALIZED TABLE DDL sql call. */
public class SqlCreateMaterializedTable extends SqlCreateObject implements ExtendedSqlNode {

    public static final SqlSpecialOperator CREATE_OPERATOR =
            new SqlSpecialOperator("CREATE MATERIALIZED TABLE", SqlKind.CREATE_TABLE);

    private final SqlNodeList columnList;

    private final List<SqlTableConstraint> tableConstraints;

    private final SqlWatermark watermark;

    private final @Nullable SqlDistribution distribution;

    private final SqlNodeList partitionKeyList;

    private final @Nullable SqlIntervalLiteral freshness;

    private final @Nullable SqlRefreshMode refreshMode;

    private final SqlNode asQuery;

    public SqlCreateMaterializedTable(
            SqlSpecialOperator operator,
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
            @Nullable SqlRefreshMode refreshMode,
            SqlNode asQuery) {
        super(operator, pos, tableName, false, false, false, propertyList, comment);
        this.columnList = columnList;
        this.tableConstraints = tableConstraints;
        this.watermark = watermark;
        this.distribution = distribution;
        this.partitionKeyList =
                requireNonNull(partitionKeyList, "partitionKeyList should not be null");
        requireNonNull(propertyList, "propertyList should not be null");
        this.freshness = freshness;
        this.refreshMode = refreshMode;
        this.asQuery = requireNonNull(asQuery, "asQuery should not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                name,
                columnList,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
                watermark,
                comment,
                partitionKeyList,
                properties,
                freshness,
                asQuery);
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    public @Nullable SqlDistribution getDistribution() {
        return distribution;
    }

    public List<String> getPartitionKeyList() {
        return SqlParseUtils.extractList(partitionKeyList, p -> ((SqlIdentifier) p).getSimple());
    }

    @Nullable
    public SqlIntervalLiteral getFreshness() {
        return freshness;
    }

    @Nullable
    public SqlRefreshMode getRefreshMode() {
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
        return !columnList.isEmpty() && columnList.get(0) instanceof SqlIdentifier;
    }

    @Override
    protected String getScope() {
        return "MATERIALIZED TABLE";
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
        SqlUnparseUtils.unparseFreshness(freshness, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseRefreshMode(refreshMode, writer);
        SqlUnparseUtils.unparseAsQuery(asQuery, writer, leftPrec, rightPrec);
    }
}
