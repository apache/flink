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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Abstract class for SHOW sql call. */
public abstract class SqlShowCall extends SqlCall {
    private final String preposition;
    private final SqlIdentifier sqlIdentifier;
    // different like type such as like, ilike
    private final String likeType;
    private final SqlCharStringLiteral likeLiteral;
    private final boolean notLike;

    public SqlShowCall(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier databaseName,
            String likeType,
            SqlCharStringLiteral likeLiteral,
            boolean notLike) {
        super(pos);
        this.preposition = preposition;
        this.sqlIdentifier = preposition != null ? databaseName : null;
        this.likeType = likeType;
        this.likeLiteral = likeType == null ? null : likeLiteral;
        this.notLike = likeType != null && notLike;
    }

    @Override
    public abstract SqlOperator getOperator();

    @Override
    public List<SqlNode> getOperandList() {
        return Objects.isNull(sqlIdentifier)
                ? Collections.emptyList()
                : Collections.singletonList(sqlIdentifier);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        String keyword = getOperationName();
        if (preposition == null) {
            writer.keyword(keyword);
        } else if (sqlIdentifier != null) {
            writer.keyword(keyword + " " + preposition);
            sqlIdentifier.unparse(writer, leftPrec, rightPrec);
        }
        if (isWithLike()) {
            final String notPrefix = isNotLike() ? "NOT " : "";
            writer.keyword(String.format("%s%s '%s'", notPrefix, likeType, getLikeSqlPattern()));
        }
    }

    public String getPreposition() {
        return preposition;
    }

    public List<String> getSqlIdentifierNameList() {
        return Objects.isNull(this.sqlIdentifier) ? Collections.emptyList() : sqlIdentifier.names;
    }

    public boolean isWithLike() {
        return likeType != null;
    }

    public String getLikeType() {
        return likeType;
    }

    public String getLikeSqlPattern() {
        return Objects.isNull(likeLiteral) ? null : likeLiteral.getValueAs(String.class);
    }

    public boolean isNotLike() {
        return notLike;
    }

    abstract String getOperationName();
}
