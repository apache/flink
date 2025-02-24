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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;

public class SqlDropFunctionConverter implements SqlNodeConverter<SqlDropFunction> {
    @Override
    public Operation convertSqlNode(SqlDropFunction node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(node.getFunctionIdentifier());
        if (node.isSystemFunction()) {
            return new DropTempSystemFunctionOperation(
                    unresolvedIdentifier.getObjectName(), node.getIfExists());
        } else {
            ObjectIdentifier identifier =
                    context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

            return new DropCatalogFunctionOperation(
                    identifier, node.getIfExists(), node.isTemporary());
        }
    }
}
