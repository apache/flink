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

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SHOW TABLES sql call. The full syntax for show functions is as followings:
 *
 * <pre>{@code
 * SHOW ( VIEWS | [ MATERIALIZED ]TABLES ) [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] LIKE
 * <sql_like_pattern> ] statement
 * }</pre>
 */
public class SqlShowTables extends SqlShowCall {

    private final SqlTableKind kind;

    public SqlShowTables(
            SqlParserPos pos,
            SqlTableKind kind,
            String preposition,
            SqlIdentifier databaseName,
            boolean notLike,
            SqlCharStringLiteral likeLiteral) {
        // only LIKE currently supported for SHOW TABLES
        super(
                pos,
                preposition,
                databaseName,
                likeLiteral == null ? null : "LIKE",
                likeLiteral,
                notLike);
        this.kind = kind;
    }

    @Override
    public SqlOperator getOperator() {
        return kind.getOperator();
    }

    @Override
    String getOperationName() {
        return getOperator().getName();
    }

    public SqlTableKind getTableKind() {
        return kind;
    }

    /**
     * The kind of table. Keep in sync with {@link
     * org.apache.flink.table.catalog.CatalogBaseTable.TableKind}.
     */
    public enum SqlTableKind {
        MATERIALIZED_TABLE(new SqlSpecialOperator("SHOW MATERIALIZED TABLES", SqlKind.OTHER)),
        TABLE(new SqlSpecialOperator("SHOW TABLES", SqlKind.OTHER)),
        VIEW(new SqlSpecialOperator("SHOW VIEWS", SqlKind.OTHER));

        private final SqlSpecialOperator operator;

        SqlTableKind(final SqlSpecialOperator operator) {
            this.operator = operator;
        }

        public SqlSpecialOperator getOperator() {
            return operator;
        }
    }
}
