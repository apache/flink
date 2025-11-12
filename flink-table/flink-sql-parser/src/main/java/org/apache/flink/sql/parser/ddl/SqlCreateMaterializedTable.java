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

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** CREATE MATERIALIZED TABLE DDL sql call. */
public class SqlCreateMaterializedTable extends SqlCreate {

    public static final SqlSpecialOperator CREATE_OPERATOR =
            new SqlSpecialOperator("CREATE MATERIALIZED TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final @Nullable SqlTableConstraint tableConstraint;

    private final @Nullable SqlCharStringLiteral comment;

    private final @Nullable SqlDistribution distribution;

    private final SqlNodeList partitionKeyList;

    private final SqlNodeList propertyList;

    private final @Nullable SqlIntervalLiteral freshness;

    private final @Nullable SqlRefreshMode refreshMode;

    private final SqlNode asQuery;

    public SqlCreateMaterializedTable(
            SqlSpecialOperator operator,
            SqlParserPos pos,
            SqlIdentifier tableName,
            @Nullable SqlTableConstraint tableConstraint,
            @Nullable SqlCharStringLiteral comment,
            @Nullable SqlDistribution distribution,
            SqlNodeList partitionKeyList,
            SqlNodeList propertyList,
            @Nullable SqlIntervalLiteral freshness,
            @Nullable SqlRefreshMode refreshMode,
            SqlNode asQuery) {
        super(operator, pos, false, false);
        this.tableName = requireNonNull(tableName, "tableName should not be null");
        this.comment = comment;
        this.tableConstraint = tableConstraint;
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
        return CREATE_OPERATOR;
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
    public SqlRefreshMode getRefreshMode() {
        return refreshMode;
    }

    public SqlNode getAsQuery() {
        return asQuery;
    }
}
