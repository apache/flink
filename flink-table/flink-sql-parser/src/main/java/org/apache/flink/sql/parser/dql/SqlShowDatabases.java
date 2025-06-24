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

import org.apache.flink.sql.parser.impl.ParseException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SHOW Databases sql call. The full syntax for show databases is as followings:
 *
 * <pre>{@code
 * SHOW DATABASES [ ( FROM | IN ) catalog_name] [ [NOT] (LIKE | ILIKE)
 * <sql_like_pattern> ] statement
 * }</pre>
 */
public class SqlShowDatabases extends SqlShowCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW DATABASES", SqlKind.OTHER);

    public SqlShowDatabases(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier catalogName,
            String likeType,
            SqlCharStringLiteral likeLiteral,
            boolean notLike)
            throws ParseException {
        super(pos, preposition, catalogName, likeType, likeLiteral, notLike);
        if (catalogName != null && catalogName.names.size() > 1) {
            throw new ParseException(
                    String.format(
                            "Show databases from/in identifier [ %s ] format error, catalog must be a single part identifier.",
                            String.join(".", catalogName.names)));
        }
    }

    public String getCatalogName() {
        return getSqlIdentifierNameList().isEmpty() ? null : getSqlIdentifierNameList().get(0);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    String getOperationName() {
        return "SHOW DATABASES";
    }
}
