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

import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.resource.ResourceUri;

import java.util.List;
import java.util.Map;

/** Convert CREATE FUNCTION statement. */
public class SqlCreateFunctionConverter implements SqlNodeConverter<SqlCreateFunction> {
    @Override
    public Operation convertSqlNode(SqlCreateFunction sqlCreateFunction, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateFunction.getFullName());
        List<ResourceUri> resourceUris =
                OperationConverterUtils.getFunctionResources(sqlCreateFunction.getResourceInfos());
        final Map<String, String> options = sqlCreateFunction.getProperties();
        final FunctionLanguage language =
                OperationConverterUtils.parseLanguage(sqlCreateFunction.getFunctionLanguage());
        if (sqlCreateFunction.isSystemFunction()) {
            return new CreateTempSystemFunctionOperation(
                    unresolvedIdentifier.getObjectName(),
                    sqlCreateFunction.getFunctionClassName().getValueAs(String.class),
                    sqlCreateFunction.isIfNotExists(),
                    language,
                    resourceUris,
                    options);
        } else {
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(
                            sqlCreateFunction.getFunctionClassName().getValueAs(String.class),
                            language,
                            resourceUris,
                            options);
            ObjectIdentifier identifier =
                    context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

            return new CreateCatalogFunctionOperation(
                    identifier,
                    catalogFunction,
                    sqlCreateFunction.isIfNotExists(),
                    sqlCreateFunction.isTemporary());
        }
    }
}
