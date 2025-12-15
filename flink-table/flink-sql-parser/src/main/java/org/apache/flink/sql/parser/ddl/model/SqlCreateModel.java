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

package org.apache.flink.sql.parser.ddl.model;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateObject;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * {@link SqlNode} to describe the CREATE MODEL syntax. CREATE MODEL [IF NOT EXISTS] [[catalogName.]
 * dataBasesName].modelName WITH (name=value, [name=value]*).
 */
public class SqlCreateModel extends SqlCreateObject implements ExtendedSqlNode {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE MODEL", SqlKind.OTHER_DDL);

    private final SqlNodeList inputColumnList;
    private final SqlNodeList outputColumnList;

    public SqlCreateModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlCharStringLiteral comment,
            SqlNodeList inputColumnList,
            SqlNodeList outputColumnList,
            SqlNodeList propertyList,
            boolean isTemporary,
            boolean ifNotExists) {
        super(OPERATOR, pos, modelName, isTemporary, false, ifNotExists, propertyList, comment);
        this.inputColumnList = inputColumnList;
        this.outputColumnList = outputColumnList;
        requireNonNull(propertyList, "propertyList should not be null");
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                name, comment, inputColumnList, outputColumnList, properties);
    }

    public SqlNodeList getInputColumnList() {
        return inputColumnList;
    }

    public SqlNodeList getOutputColumnList() {
        return outputColumnList;
    }

    @Override
    protected String getScope() {
        return "MODEL";
    }

    @Override
    public void validate() throws SqlValidateException {
        if (!inputColumnList.isEmpty() && outputColumnList.isEmpty()) {
            throw new SqlValidateException(
                    inputColumnList.get(0).getParserPosition(),
                    "Output column list can not be empty with non-empty input column list.");
        }
        if (inputColumnList.isEmpty() && !outputColumnList.isEmpty()) {
            throw new SqlValidateException(
                    outputColumnList.get(0).getParserPosition(),
                    "Input column list can not be empty with non-empty output column list.");
        }
        if (properties == null || properties.isEmpty()) {
            throw new SqlValidateException(
                    getParserPosition(), "Model property list can not be empty.");
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparseCreateIfNotExists(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseListWithIndent(
                "INPUT", inputColumnList, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseListWithIndent(
                "OUTPUT", outputColumnList, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseComment(comment, true, writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseProperties(properties, writer, leftPrec, rightPrec);
    }
}
