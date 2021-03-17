/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * A <code>SqlTableRef</code> is a node of a parse tree which represents a table reference.
 *
 * <p>It can be attached with a sql hint statement, see {@link SqlHint} for details.
 *
 * <p>The class was copied over because of CALCITE-4406. The changed lines are: 43-49
 */
public class SqlTableRef extends SqlCall {

    // ~ Instance fields --------------------------------------------------------

    private final SqlIdentifier tableName;
    private final SqlNodeList hints;

    // ~ Static fields/initializers ---------------------------------------------

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("TABLE_REF", SqlKind.TABLE_REF) {
                @Override
                public SqlCall createCall(
                        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                    return new SqlTableRef(
                            pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
                }
            };

    // ~ Constructors -----------------------------------------------------------

    public SqlTableRef(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList hints) {
        super(pos);
        this.tableName = tableName;
        this.hints = hints;
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(tableName, hints);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        tableName.unparse(writer, leftPrec, rightPrec);
        if (this.hints != null && this.hints.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("/*+");
            this.hints.unparse(writer, 0, 0);
            writer.keyword("*/");
        }
    }
}
