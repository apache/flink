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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * {@link SqlNode} to describe the CREATE MODEL AS syntax. The CTAS would create a pipeline to
 * compute the result of the given query and use the data to train the model.
 *
 * <p>Example:
 *
 * <pre>{@code
 * CREATE MODEL my_model WITH (name=value, [name=value]*)
 * ) AS SELECT col1, col2, label FROM base_table;
 * }</pre>
 */
public class SqlCreateModelAs extends SqlCreateModel {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE MODEL AS", SqlKind.OTHER_DDL);

    private final SqlNode asQuery;

    public SqlCreateModelAs(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlCharStringLiteral comment,
            SqlNodeList inputColumnList,
            SqlNodeList outputColumnList,
            SqlNodeList propertyList,
            SqlNode asQuery,
            boolean isTemporary,
            boolean ifNotExists) {
        super(
                pos,
                modelName,
                comment,
                inputColumnList,
                outputColumnList,
                propertyList,
                isTemporary,
                ifNotExists);
        this.asQuery =
                requireNonNull(asQuery, "As clause is required for CREATE MODEL AS SELECT DDL");
    }

    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.<SqlNode>builder()
                .addAll(super.getOperandList())
                .add(asQuery)
                .build();
    }

    @Override
    public void validate() throws SqlValidateException {
        if (!getInputColumnList().isEmpty()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE MODEL AS SELECT syntax does not support to specify explicit input columns.");
        }
        if (!getOutputColumnList().isEmpty()) {
            throw new SqlValidateException(
                    getParserPosition(),
                    "CREATE MODEL AS SELECT syntax does not support to specify explicit output columns.");
        }
        super.validate();
    }

    public SqlNode getAsQuery() {
        return asQuery;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        this.asQuery.unparse(writer, leftPrec, rightPrec);
    }
}
