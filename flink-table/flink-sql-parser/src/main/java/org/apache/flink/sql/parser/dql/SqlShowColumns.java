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

package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** Show [[catalog.] dataBasesName.]sqlIdentifier sql call. Here we add Rich in */
public class SqlShowColumns extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW COLUMNS", SqlKind.OTHER);

    protected final SqlIdentifier tableName;
    protected final String preposition;
    protected final boolean notLike;
    protected final SqlCharStringLiteral likeLiteral;

    public SqlShowColumns(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier tableName,
            boolean notLike,
            SqlCharStringLiteral likeLiteral) {
        super(pos);
        this.preposition =
                requireNonNull(
                        preposition, "Preposition of 'SHOW COLUMNS' must be 'FROM' or 'IN'.");
        this.tableName = requireNonNull(tableName, "tableName should not be null.");
        this.notLike = notLike;
        this.likeLiteral = likeLiteral;
    }

    public String getLikeSqlPattern() {
        return Objects.isNull(this.likeLiteral) ? null : likeLiteral.getValueAs(String.class);
    }

    public boolean isNotLike() {
        return notLike;
    }

    public SqlCharStringLiteral getLikeLiteral() {
        return likeLiteral;
    }

    public boolean isWithLike() {
        return Objects.nonNull(likeLiteral);
    }

    public String getPreposition() {
        return preposition;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(tableName);
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW COLUMNS " + this.preposition);
        tableName.unparse(writer, leftPrec, rightPrec);
        if (isWithLike()) {
            if (isNotLike()) {
                writer.keyword(
                        String.format("NOT LIKE '%s'", likeLiteral.getValueAs(String.class)));
            } else {
                writer.keyword(String.format("LIKE '%s'", likeLiteral.getValueAs(String.class)));
            }
        }
    }
}
