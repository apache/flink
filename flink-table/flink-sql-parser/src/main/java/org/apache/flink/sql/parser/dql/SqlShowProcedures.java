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

/**
 * SHOW PROCEDURES sql call. The full syntax for show procedures is as followings:
 *
 * <pre>{@code
 * SHOW PROCEDURES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE)
 * <sql_like_pattern> ] statement
 * }</pre>
 */
public class SqlShowProcedures extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW PROCEDURES", SqlKind.OTHER);

    private final SqlIdentifier databaseName;
    private final String preposition;
    private final boolean notLike;

    /** ILike represents it's case insensitive when match procedures with regex expression. */
    private final boolean isILike;

    private final SqlCharStringLiteral likeLiteral;

    public SqlShowProcedures(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier databaseName,
            boolean notLike,
            boolean isILike,
            SqlCharStringLiteral likeLiteral) {
        super(pos);
        this.preposition = preposition;
        this.databaseName =
                preposition != null
                        ? requireNonNull(databaseName, "Database name must not be null.")
                        : null;
        this.notLike = notLike;
        this.likeLiteral = likeLiteral;
        this.isILike = isILike;
    }

    public String getLikeSqlPattern() {
        return Objects.isNull(this.likeLiteral) ? null : likeLiteral.getValueAs(String.class);
    }

    public boolean isNotLike() {
        return notLike;
    }

    public boolean isILike() {
        return isILike;
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
        return Objects.isNull(this.databaseName)
                ? Collections.emptyList()
                : Collections.singletonList(databaseName);
    }

    public String[] fullDatabaseName() {
        return Objects.isNull(this.databaseName)
                ? new String[] {}
                : databaseName.names.toArray(new String[0]);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.preposition == null) {
            writer.keyword("SHOW PROCEDURES");
        } else if (databaseName != null) {
            writer.keyword("SHOW PROCEDURES " + this.preposition);
            databaseName.unparse(writer, leftPrec, rightPrec);
        }
        if (isWithLike()) {
            if (isNotLike()) {
                writer.keyword(
                        String.format(
                                "NOT %s '%s'", isILike ? "ILIKE" : "LIKE", getLikeSqlPattern()));
            } else {
                writer.keyword(
                        String.format("%s '%s'", isILike ? "ILIKE" : "LIKE", getLikeSqlPattern()));
            }
        }
    }
}
