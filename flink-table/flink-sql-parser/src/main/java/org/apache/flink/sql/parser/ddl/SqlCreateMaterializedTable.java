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
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** CREATE MATERIALIZED TABLE DDL sql call. */
public class SqlCreateMaterializedTable extends SqlCreate {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE MATERIALIZED TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final SqlCharStringLiteral comment;

    private final SqlTableConstraint tableConstraint;

    private final SqlNodeList partitionKeyList;

    private final SqlNodeList propertyList;

    private final SqlIntervalLiteral freshness;

    @Nullable private final SqlLiteral refreshMode;

    private final SqlNode asQuery;

    public SqlCreateMaterializedTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            @Nullable SqlCharStringLiteral comment,
            @Nullable SqlTableConstraint tableConstraint,
            SqlNodeList partitionKeyList,
            SqlNodeList propertyList,
            SqlIntervalLiteral freshness,
            @Nullable SqlLiteral refreshMode,
            SqlNode asQuery) {
        super(OPERATOR, pos, false, false);
        this.tableName = requireNonNull(tableName, "tableName should not be null");
        this.comment = comment;
        this.tableConstraint = tableConstraint;
        this.partitionKeyList =
                requireNonNull(partitionKeyList, "partitionKeyList should not be null");
        this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
        this.freshness = requireNonNull(freshness, "freshness should not be null");
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
                comment,
                tableConstraint,
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

    public Optional<SqlCharStringLiteral> getComment() {
        return Optional.ofNullable(comment);
    }

    public Optional<SqlTableConstraint> getTableConstraint() {
        return Optional.ofNullable(tableConstraint);
    }

    public SqlNodeList getPartitionKeyList() {
        return partitionKeyList;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public SqlIntervalLiteral getFreshness() {
        return freshness;
    }

    public Optional<SqlLiteral> getRefreshMode() {
        return Optional.ofNullable(refreshMode);
    }

    public SqlNode getAsQuery() {
        return asQuery;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE MATERIALIZED TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);

        if (tableConstraint != null) {
            writer.newlineAndIndent();
            SqlUnparseUtils.unparseTableSchema(
                    writer,
                    leftPrec,
                    rightPrec,
                    SqlNodeList.EMPTY,
                    Collections.singletonList(tableConstraint),
                    null);
        }

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (partitionKeyList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            partitionKeyList.unparse(writer, leftPrec, rightPrec);
            writer.endList(partitionedByFrame);
            writer.newlineAndIndent();
        }

        if (propertyList.size() > 0) {
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
        writer.keyword("FRESHNESS");
        writer.keyword("=");
        freshness.unparse(writer, leftPrec, rightPrec);

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
