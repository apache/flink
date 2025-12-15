/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.ddl.view;

import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateObject;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** CREATE VIEW DDL sql call. */
public class SqlCreateView extends SqlCreateObject {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE_VIEW", SqlKind.CREATE_VIEW);

    private final SqlNodeList fieldList;
    private final SqlNode query;

    public SqlCreateView(
            SqlParserPos pos,
            SqlIdentifier viewName,
            SqlNodeList fieldList,
            SqlNode query,
            boolean replace,
            boolean isTemporary,
            boolean ifNotExists,
            SqlCharStringLiteral comment,
            SqlNodeList properties) {
        super(OPERATOR, pos, viewName, isTemporary, replace, ifNotExists, properties, comment);
        this.fieldList = requireNonNull(fieldList, "fieldList should not be null");
        this.query = requireNonNull(query, "query should not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> ops = new ArrayList<>();
        ops.add(name);
        ops.add(fieldList);
        ops.add(query);
        ops.add(SqlLiteral.createBoolean(getReplace(), SqlParserPos.ZERO));
        return ops;
    }

    public SqlNodeList getFieldList() {
        return fieldList;
    }

    public SqlNode getQuery() {
        return query;
    }

    @Override
    protected String getScope() {
        return "VIEW";
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparseCreateIfNotExists(writer, leftPrec, rightPrec);
        unparseFieldList(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseComment(comment, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseAsQuery(query, writer, leftPrec, rightPrec);
    }

    private void unparseFieldList(SqlWriter writer, int leftPrec, int rightPrec) {
        if (!fieldList.isEmpty()) {
            fieldList.unparse(writer, 1, rightPrec);
        }
    }
}
