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

package org.apache.flink.sql.parser.ddl.catalog;

import org.apache.flink.sql.parser.ddl.SqlCreateObject;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** CREATE CATALOG DDL sql call. */
public class SqlCreateCatalog extends SqlCreateObject {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE CATALOG", SqlKind.OTHER_DDL);

    public SqlCreateCatalog(
            SqlParserPos position,
            SqlIdentifier catalogName,
            SqlNodeList propertyList,
            @Nullable SqlCharStringLiteral comment,
            boolean ifNotExists) {
        super(OPERATOR, position, catalogName, false, false, ifNotExists, propertyList, comment);
        requireNonNull(propertyList, "propertyList cannot be null");
    }

    @Override
    protected String getScope() {
        return "CATALOG";
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, properties, comment);
    }

    public String catalogName() {
        return name.getSimple();
    }
}
