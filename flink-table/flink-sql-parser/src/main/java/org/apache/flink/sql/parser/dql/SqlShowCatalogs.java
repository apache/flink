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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SHOW Catalogs sql call. The full syntax for show catalogs is as followings:
 *
 * <pre>{@code
 * SHOW CATALOGS [ [NOT] (LIKE | ILIKE) <sql_like_pattern> ]
 * }</pre>
 */
public class SqlShowCatalogs extends SqlShowCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW CATALOGS", SqlKind.OTHER);

    public SqlShowCatalogs(
            SqlParserPos pos, String likeType, SqlCharStringLiteral likeLiteral, boolean notLike) {
        super(pos, null, null, likeType, likeLiteral, notLike);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    String getOperationName() {
        return "SHOW CATALOGS";
    }
}
