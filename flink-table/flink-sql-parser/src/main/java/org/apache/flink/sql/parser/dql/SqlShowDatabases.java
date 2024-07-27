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

import static java.util.Objects.requireNonNull;

/**
 * SHOW Databases sql call. The full syntax for show databases is as followings:
 *
 * <pre>{@code
 * SHOW DATABASES [ ( FROM | IN ) catalog_name] [ [NOT] (LIKE | ILIKE)
 * <sql_like_pattern> ] statement
 * }</pre>
 */
public class SqlShowDatabases extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW DATABASES", SqlKind.OTHER);

    private final String preposition;
    private final SqlIdentifier catalogName;
    private final String likeType;
    private final SqlCharStringLiteral likeLiteral;
    private final boolean notLike;

    public String[] getCatalog() {
        return catalogName == null || catalogName.names.isEmpty()
                ? new String[] {}
                : catalogName.names.toArray(new String[0]);
    }

    public SqlShowDatabases(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier catalogName,
            String likeType,
            SqlCharStringLiteral likeLiteral,
            boolean notLike)
            throws ParseException {
        super(pos);
        this.preposition = preposition;

        this.catalogName =
                preposition != null
                        ? requireNonNull(catalogName, "Catalog name must not be null.")
                        : null;
        if (this.catalogName != null && this.catalogName.names.size() > 1) {
            throw new ParseException(
                    String.format(
                            "Show databases from/in identifier [ %s ] format error, catalog must be a single part identifier.",
                            String.join(".", this.catalogName.names)));
        }

        if (likeType != null) {
            this.likeType = likeType;
            this.likeLiteral = requireNonNull(likeLiteral, "Like pattern must not be null");
            this.notLike = notLike;
        } else {
            this.likeType = null;
            this.likeLiteral = null;
            this.notLike = false;
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return catalogName == null
                ? Collections.emptyList()
                : Collections.singletonList(catalogName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW DATABASES");
        if (preposition != null) {
            writer.keyword(preposition);
            catalogName.unparse(writer, leftPrec, rightPrec);
        }
        if (likeType != null) {
            writer.keyword(
                    isNotLike()
                            ? String.format("NOT %s '%s'", likeType, getLikeSqlPattern())
                            : String.format("%s '%s'", likeType, getLikeSqlPattern()));
        }
    }

    public String getLikeSqlPattern() {
        return likeLiteral == null ? null : likeLiteral.getValueAs(String.class);
    }

    public boolean isNotLike() {
        return notLike;
    }

    public String getPreposition() {
        return preposition;
    }

    public String getLikeType() {
        return likeType;
    }
}
