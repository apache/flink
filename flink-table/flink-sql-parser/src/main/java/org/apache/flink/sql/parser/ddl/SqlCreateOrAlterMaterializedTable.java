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
import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import java.util.Collections;

/** CREATE [OR ALTER] MATERIALIZED TABLE DDL sql call. */
public class SqlCreateOrAlterMaterializedTable extends SqlCreateMaterializedTable  {

    public static final SqlSpecialOperator CREATE_OR_ALTER_OPERATOR =
            new SqlSpecialOperator("CREATE OR ALTER MATERIALIZED TABLE", SqlKind.OTHER_DDL);

    private final boolean isOrAlter;

    public SqlCreateOrAlterMaterializedTable(
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
            SqlNode asQuery,
            boolean isOrAlter) {
        super(
                isOrAlter ? CREATE_OR_ALTER_OPERATOR : CREATE_OPERATOR,
                pos,
                tableName,
                columnList,
                tableConstraint,
                watermark,
                comment,
                distribution,
                partitionKeyList,
                propertyList,
                freshness,
                refreshMode,
                asQuery);
        this.isOrAlter = isOrAlter;
    }

    @Override
    public SqlOperator getOperator() {
        return isOrAlter ? CREATE_OR_ALTER_OPERATOR : CREATE_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableName,
                columnList,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
                watermark,
                comment,
                tableConstraint,
                partitionKeyList,
                propertyList,
                freshness,
                refreshMode,
                asQuery);
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

    public boolean isOrAlter() {
        return isOrAlter;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isOrAlter) {
            writer.keyword("OR ALTER");
        }
        writer.keyword("MATERIALIZED TABLE");
        getTableName().unparse(writer, leftPrec, rightPrec);

        // FIXME: we need to update this
//        if (!columnList.isEmpty() || !tableConstraints.isEmpty() || watermark != null) {
//            SqlUnparseUtils.unparseTableSchema(
//                    writer, leftPrec, rightPrec, columnList, tableConstraints, watermark);
//        }

        getTableConstraint()
                .ifPresent(
                        tableConstraint -> {
                            writer.newlineAndIndent();
                            SqlUnparseUtils.unparseTableSchema(
                                    writer,
                                    leftPrec,
                                    rightPrec,
                                    SqlNodeList.EMPTY,
                                    Collections.singletonList(tableConstraint),
                                    null);
                        });

        getComment()
                .ifPresent(
                        comment -> {
                            writer.newlineAndIndent();
                            writer.keyword("COMMENT");
                            comment.unparse(writer, leftPrec, rightPrec);
                        });

        if (getDistribution() != null) {
            writer.newlineAndIndent();
            getDistribution().unparse(writer, leftPrec, rightPrec);
        }

        if (!getPartitionKeyList().isEmpty()) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            getPartitionKeyList().unparse(writer, leftPrec, rightPrec);
            writer.endList(partitionedByFrame);
        }

        if (!getPropertyList().isEmpty()) {
            writer.newlineAndIndent();
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : getPropertyList()) {
                SqlUnparseUtils.printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }

        if (getFreshness() != null) {
            writer.newlineAndIndent();
            writer.keyword("FRESHNESS");
            writer.keyword("=");
            getFreshness().unparse(writer, leftPrec, rightPrec);
        }

        if (getRefreshMode() != null) {
            writer.newlineAndIndent();
            writer.keyword("REFRESH_MODE");
            writer.keyword("=");
            getRefreshMode().unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        getAsQuery().unparse(writer, leftPrec, rightPrec);
    }
}
