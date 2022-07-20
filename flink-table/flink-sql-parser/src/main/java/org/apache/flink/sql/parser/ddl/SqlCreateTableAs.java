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
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.Collectors;

/** CREATE TABLE AS SELECT DDL sql call. */
public class SqlCreateTableAs extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE AS SELECT", SqlKind.OTHER_DDL);

    private final SqlCreateTable sqlCreateTable;
    private final SqlNode asQuery;

    public SqlCreateTableAs(SqlCreateTable sqlCreateTable, SqlNode asQuery) {
        super(
                OPERATOR,
                sqlCreateTable.getParserPosition(),
                sqlCreateTable.getReplace(),
                sqlCreateTable.isIfNotExists());
        this.sqlCreateTable = sqlCreateTable;
        this.asQuery = asQuery;
    }

    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        SqlNode[] sqlNodes = new SqlNode[sqlCreateTable.getOperandList().size() + 1];
        List<SqlNode> operandList = sqlCreateTable.getOperandList();
        for (int i = 0; i < operandList.size(); i++) {
            sqlNodes[i] = operandList.get(i);
        }
        sqlNodes[sqlNodes.length - 1] = asQuery;
        return ImmutableNullableList.copyOf(sqlNodes);
    }

    @Override
    public void validate() throws SqlValidateException {
        sqlCreateTable.validate();
        if (sqlCreateTable.isTemporary()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE TABLE AS SELECT syntax does not yet support creating temporary table.");
        }

        if (sqlCreateTable.getColumnList().size() > 0) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE TABLE AS SELECT syntax does not yet support explicitly specifying columns.");
        }

        if (sqlCreateTable.getWatermark().isPresent()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE TABLE AS SELECT syntax does not yet support explicitly specifying watermark.");
        }
        // TODO flink dialect supports dynamic partition
        if (sqlCreateTable.getPartitionKeyList().size() > 0) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE TABLE AS SELECT syntax does not yet support creating partitioned table.");
        }
        List<SqlTableConstraint> constraints =
                sqlCreateTable.getFullConstraints().stream()
                        .filter(SqlTableConstraint::isPrimaryKey)
                        .collect(Collectors.toList());
        if (!constraints.isEmpty()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE TABLE AS SELECT syntax does not yet support primary key constraints.");
        }
    }

    public SqlCreateTable getSqlCreateTable() {
        return sqlCreateTable;
    }

    public SqlNode getAsQuery() {
        return asQuery;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        sqlCreateTable.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        this.asQuery.unparse(writer, leftPrec, rightPrec);
    }
}
