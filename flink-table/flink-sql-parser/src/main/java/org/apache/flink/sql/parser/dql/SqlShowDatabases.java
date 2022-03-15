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

/** SHOW Databases sql call. */
public class SqlShowDatabases extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW DATABASES", SqlKind.OTHER);

    private String preposition;
    private boolean notLike;
    private SqlIdentifier catalogName;
    private SqlCharStringLiteral likeLiteral;

    public SqlShowDatabases(SqlParserPos pos) {
        super(pos);
    }

    public SqlShowDatabases(
            SqlParserPos pos,
            String preposition,
            SqlIdentifier catalogName,
            boolean notLike,
            SqlCharStringLiteral likeLiteral) {
        super(pos);
        this.preposition = preposition;
        this.catalogName = catalogName;
        this.notLike = notLike;
        this.likeLiteral = likeLiteral;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.EMPTY_LIST;
    }

    public String getCatalogName() {
        return Objects.isNull(this.catalogName) ? null : catalogName.getSimple();
    }

    public boolean isNotLike() {
        return notLike;
    }

    public String getPreposition() {
        return preposition;
    }

    public String getLikeSqlPattern() {
        return Objects.isNull(this.likeLiteral) ? null : likeLiteral.getValueAs(String.class);
    }

    public SqlCharStringLiteral getLikeLiteral() {
        return likeLiteral;
    }

    public boolean isWithLike() {
        return Objects.nonNull(likeLiteral);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (Objects.isNull(preposition)) {
            writer.keyword("SHOW DATABASES");
        } else if (Objects.nonNull(catalogName)) {
            writer.keyword("SHOW DATABASES " + this.preposition);
            catalogName.unparse(writer, leftPrec, rightPrec);
        }
        if (isWithLike()) {
            if (notLike) {
                writer.keyword(String.format("NOT LIKE '%s'", getLikeSqlPattern()));
            } else {
                writer.keyword(String.format("LIKE '%s'", getLikeSqlPattern()));
            }
        }
    }
}
