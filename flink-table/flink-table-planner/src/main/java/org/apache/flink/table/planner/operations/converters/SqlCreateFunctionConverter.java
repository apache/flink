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
import org.apache.flink.sql.parser.ddl.resource.SqlResource;
import org.apache.flink.sql.parser.ddl.resource.SqlResourceType;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;

public class SqlCreateFunctionConverter implements SqlNodeConverter<SqlCreateFunction> {
    @Override
    public Operation convertSqlNode(SqlCreateFunction node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(node.getFunctionIdentifier());
        List<ResourceUri> resourceUris = getFunctionResources(node.getResourceInfos());
        if (node.isSystemFunction()) {
            return new CreateTempSystemFunctionOperation(
                    unresolvedIdentifier.getObjectName(),
                    node.getFunctionClassName().getValueAs(String.class),
                    node.isIfNotExists(),
                    SqlNodeConvertUtils.parseLanguage(node.getFunctionLanguage()),
                    resourceUris);
        } else {
            FunctionLanguage language =
                    SqlNodeConvertUtils.parseLanguage(node.getFunctionLanguage());
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(
                            node.getFunctionClassName().getValueAs(String.class),
                            language,
                            resourceUris);
            ObjectIdentifier identifier =
                    context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

            return new CreateCatalogFunctionOperation(
                    identifier, catalogFunction, node.isIfNotExists(), node.isTemporary());
        }
    }

    private List<ResourceUri> getFunctionResources(List<SqlNode> sqlResources) {
        return sqlResources.stream()
                .map(SqlResource.class::cast)
                .map(
                        sqlResource -> {
                            // get resource type
                            SqlResourceType sqlResourceType =
                                    sqlResource.getResourceType().getValueAs(SqlResourceType.class);
                            ResourceType resourceType;
                            switch (sqlResourceType) {
                                case FILE:
                                    resourceType = ResourceType.FILE;
                                    break;
                                case JAR:
                                    resourceType = ResourceType.JAR;
                                    break;
                                case ARCHIVE:
                                    resourceType = ResourceType.ARCHIVE;
                                    break;
                                default:
                                    throw new ValidationException(
                                            String.format(
                                                    "Unsupported resource type: .",
                                                    sqlResourceType));
                            }
                            // get resource path
                            String path = sqlResource.getResourcePath().getValueAs(String.class);
                            return new ResourceUri(resourceType, path);
                        })
                .collect(Collectors.toList());
    }
}
