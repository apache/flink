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

import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.SqlUnparseUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** ALTER CATALOG catalog_name RESET (key1, ...). */
public class SqlAlterCatalogReset extends SqlAlterCatalog {

    private final SqlNodeList propertyKeyList;

    public SqlAlterCatalogReset(
            SqlParserPos position, SqlIdentifier catalogName, SqlNodeList propertyKeyList) {
        super(position, catalogName);
        this.propertyKeyList = requireNonNull(propertyKeyList, "propertyKeyList cannot be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, propertyKeyList);
    }

    public SqlNodeList getPropertyList() {
        return propertyKeyList;
    }

    public Set<String> getResetKeys() {
        return propertyKeyList.getList().stream()
                .map(SqlParseUtils::extractString)
                .collect(Collectors.toSet());
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        SqlUnparseUtils.unparseResetOptions(propertyKeyList, writer, leftPrec, rightPrec);
    }
}
