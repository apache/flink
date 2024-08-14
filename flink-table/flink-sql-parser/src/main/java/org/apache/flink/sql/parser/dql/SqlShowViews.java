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

/** SHOW VIEWS sql call. */
public class SqlShowViews extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW VIEWS", SqlKind.OTHER);

    protected final String preposition;
    protected final SqlIdentifier databaseName;
    protected final boolean notLike;
    protected final SqlCharStringLiteral likeLiteral;

    public SqlShowViews(SqlParserPos pos) {
        super(pos);
        this.preposition = null;
        this.databaseName = null;
        this.notLike = false;
        this.likeLiteral = null;
    }

    public SqlShowViews(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier databaseName,
            boolean notLike,
            SqlCharStringLiteral likeLiteral) {
        super(pos);
        this.preposition = preposition;
        this.databaseName =
                preposition != null
                        ? requireNonNull(databaseName, "Database must not be null")
                        : null;
        this.notLike = notLike;
        this.likeLiteral = likeLiteral;
    }

    public String getLikeSqlPattern() {
        return likeLiteral == null ? null : likeLiteral.getValueAs(String.class);
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
        return databaseName == null
                ? Collections.emptyList()
                : Collections.singletonList(databaseName);
    }

    public List<String> fullDatabaseName() {
        return databaseName == null ? Collections.emptyList() : databaseName.names;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (preposition == null) {
            writer.keyword("SHOW VIEWS");
        } else if (databaseName != null) {
            writer.keyword("SHOW VIEWS " + preposition);
            databaseName.unparse(writer, leftPrec, rightPrec);
        }

        if (isWithLike()) {
            final String prefix = isNotLike() ? "NOT " : "";
            writer.keyword(String.format("%sLIKE '%s'", prefix, getLikeSqlPattern()));
        }
    }
}
