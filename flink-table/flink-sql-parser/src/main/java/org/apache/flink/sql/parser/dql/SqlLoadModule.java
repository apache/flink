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

/** LOAD MODULE sql call. */
public class SqlLoadModule extends SqlCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("LOAD MODULE", SqlKind.OTHER);

    private final SqlIdentifier moduleName;

    private final SqlNodeList propertyList;

    public SqlLoadModule(SqlParserPos pos, SqlIdentifier moduleType, SqlNodeList propertyList) {
        super(pos);
        this.moduleName = requireNonNull(moduleType, "moduleName cannot be null");
        this.propertyList = requireNonNull(propertyList, "propertyList cannot be null");
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(moduleName, propertyList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("LOAD MODULE");
        moduleName.unparse(writer, leftPrec, rightPrec);

        if (this.propertyList.size() > 0) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    private void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    public SqlIdentifier getModuleName() {
        return moduleName;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public String moduleName() {
        return moduleName.getSimple();
    }
}
