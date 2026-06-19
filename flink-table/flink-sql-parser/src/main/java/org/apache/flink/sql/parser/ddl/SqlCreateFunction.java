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

/** CREATE FUNCTION DDL sql call. */
public class SqlCreateFunction extends SqlCreateObject {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

    private final SqlCharStringLiteral functionClassName;
    private final String functionLanguage;
    private final boolean isSystemFunction;
    private final SqlNodeList resourceInfos;

    public SqlCreateFunction(
            SqlParserPos pos,
            SqlIdentifier functionIdentifier,
            SqlCharStringLiteral functionClassName,
            String functionLanguage,
            boolean ifNotExists,
            boolean isTemporary,
            boolean isSystemFunction,
            SqlNodeList resourceInfos,
            SqlNodeList propertyList) {
        super(
                OPERATOR,
                pos,
                functionIdentifier,
                isTemporary,
                false,
                ifNotExists,
                propertyList,
                null);
        this.functionClassName = requireNonNull(functionClassName);
        this.isSystemFunction = isSystemFunction;
        this.functionLanguage = functionLanguage;
        this.resourceInfos = resourceInfos;
        requireNonNull(propertyList);
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, functionClassName, resourceInfos);
    }

    public boolean isSystemFunction() {
        return isSystemFunction;
    }

    public SqlCharStringLiteral getFunctionClassName() {
        return this.functionClassName;
    }

    public String getFunctionLanguage() {
        return this.functionLanguage;
    }

    public List<SqlNode> getResourceInfos() {
        return resourceInfos.getList();
    }

    @Override
    protected String getScope() {
        return "FUNCTION";
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparseCreateIfNotExists(writer, leftPrec, rightPrec);

        unparseFunctionLanguage(writer, leftPrec, rightPrec);
        unparseResourceInfo(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseProperties(properties, writer, leftPrec, rightPrec);
    }

    @Override
    protected void unparseCreateIfNotExists(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        if (isSystemFunction) {
            writer.keyword("SYSTEM");
        }
        writer.keyword(getScope());
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        functionClassName.unparse(writer, leftPrec, rightPrec);
    }

    private void unparseFunctionLanguage(SqlWriter writer, int leftPrec, int rightPrec) {
        if (functionLanguage != null) {
            writer.keyword("LANGUAGE");
            writer.keyword(functionLanguage);
        }
    }

    private void unparseResourceInfo(SqlWriter writer, int leftPrec, int rightPrec) {
        if (!resourceInfos.isEmpty()) {
            writer.keyword("USING");
            SqlWriter.Frame withFrame = writer.startList("", "");
            for (SqlNode resourcePath : resourceInfos) {
                writer.sep(",");
                resourcePath.unparse(writer, leftPrec, rightPrec);
            }
            writer.endList(withFrame);
        }
    }
}
