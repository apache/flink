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

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
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
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * {@link SqlNode} to describe the CREATE MODEL syntax. CREATE MODEL [IF NOT EXISTS] [[catalogName.]
 * dataBasesName].modelName WITH (name=value, [name=value]*).
 */
public class SqlCreateModel extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE MODEL", SqlKind.OTHER_DDL);

    private final SqlIdentifier modelName;

    @Nullable private final SqlCharStringLiteral comment;

    private final SqlNodeList inputColumnList;

    private final SqlNodeList outputColumnList;

    private final SqlNodeList propertyList;

    private final boolean isTemporary;

    private final boolean ifNotExists;

    public SqlCreateModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlCharStringLiteral comment,
            SqlNodeList inputColumnList,
            SqlNodeList outputColumnList,
            SqlNodeList propertyList,
            boolean isTemporary,
            boolean ifNotExists) {
        super(OPERATOR, pos, false, ifNotExists);
        this.modelName = requireNonNull(modelName, "modelName should not be null");
        this.comment = comment;
        this.inputColumnList = inputColumnList;
        this.outputColumnList = outputColumnList;
        this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
        this.isTemporary = isTemporary;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                modelName, comment, inputColumnList, outputColumnList, propertyList);
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public Optional<SqlCharStringLiteral> getComment() {
        return Optional.ofNullable(comment);
    }

    public SqlNodeList getInputColumnList() {
        return inputColumnList;
    }

    public SqlNodeList getOutputColumnList() {
        return outputColumnList;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
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
        if (propertyList.isEmpty()) {
            throw new SqlValidateException(
                    getParserPosition(), "Model property list can not be empty.");
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("MODEL");
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        modelName.unparse(writer, leftPrec, rightPrec);
        if (!inputColumnList.isEmpty()) {
            writer.keyword("INPUT");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode column : inputColumnList) {
                SqlUnparseUtils.printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }

        if (!outputColumnList.isEmpty()) {
            writer.keyword("OUTPUT");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode column : outputColumnList) {
                SqlUnparseUtils.printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (!this.propertyList.isEmpty()) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode modelProperty : propertyList) {
                SqlUnparseUtils.printIndent(writer);
                modelProperty.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    public String[] fullModelName() {
        return modelName.names.toArray(new String[0]);
    }
}
