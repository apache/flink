/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.sql.parser.ddl.resource;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

/** SqlNode to describe resource type and its path information. */
public class SqlResource extends SqlCall {

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("SqlResource", SqlKind.OTHER);

    private final SqlLiteral resourceType;
    private final SqlCharStringLiteral resourcePath;

    public SqlResource(
            SqlParserPos pos, SqlLiteral resourceType, SqlCharStringLiteral resourcePath) {
        super(pos);
        this.resourceType = resourceType;
        this.resourcePath = resourcePath;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(resourceType, resourcePath);
    }

    public SqlLiteral getResourceType() {
        return resourceType;
    }

    public SqlCharStringLiteral getResourcePath() {
        return resourcePath;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        resourceType.unparse(writer, leftPrec, rightPrec);
        resourcePath.unparse(writer, leftPrec, rightPrec);
    }
}
