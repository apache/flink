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

import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * ALTER MODEL [IF EXISTS] [[catalogName.] dataBasesName].modelName SET ( name=value [,
 * name=value]*).
 */
public class SqlAlterModel extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER MODEL", SqlKind.OTHER_DDL);

    protected final SqlIdentifier modelName;
    protected final SqlIdentifier newModelName;
    protected final boolean ifModelExists;
    private final SqlNodeList propertyList;

    public SqlAlterModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlNodeList propertyList,
            boolean ifModelExists) {
        super(pos);
        this.modelName = requireNonNull(modelName, "modelName should not be null");
        this.newModelName = null;
        this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
        this.ifModelExists = ifModelExists;
    }

    public SqlAlterModel(
            SqlParserPos pos,
            SqlIdentifier modelName,
            SqlIdentifier newModelName,
            boolean ifModelExists) {
        super(pos);
        this.modelName = requireNonNull(modelName, "modelName should not be null");
        this.newModelName = requireNonNull(newModelName, "newModelName should not be null");
        this.propertyList = null;
        this.ifModelExists = ifModelExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getModelName() {
        return modelName;
    }

    public String[] fullModelName() {
        return modelName.names.toArray(new String[0]);
    }

    /**
     * Whether to ignore the error if the model doesn't exist.
     *
     * @return true when IF EXISTS is specified.
     */
    public boolean ifModelExists() {
        return ifModelExists;
    }

    public SqlIdentifier getNewModelName() {
        return newModelName;
    }

    public String[] fullNewModelName() {
        if (newModelName != null) {
            return newModelName.names.toArray(new String[0]);
        }
        return new String[0];
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        // Rename Model.
        if (newModelName != null) {
            return ImmutableNullableList.of(modelName, newModelName);
        }
        return ImmutableNullableList.of(modelName, propertyList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER MODEL");
        if (ifModelExists) {
            writer.keyword("IF EXISTS");
        }
        modelName.unparse(writer, leftPrec, rightPrec);
        if (newModelName != null) {
            // Rename Model.
            writer.keyword("RENAME TO");
            newModelName.unparse(writer, leftPrec, rightPrec);
        } else {
            writer.keyword("SET");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            if (propertyList != null) {
                for (SqlNode modelOption : propertyList) {
                    SqlUnparseUtils.printIndent(writer);
                    modelOption.unparse(writer, leftPrec, rightPrec);
                }
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }
}
