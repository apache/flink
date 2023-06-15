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

package org.apache.flink.table.operations.command;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.operations.ShowOperation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW JARS statement. */
public class ShowJarsOperation implements ShowOperation {

    @Override
    public String asSummaryString() {
        return "SHOW JARS";
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ResourceManager resourceManager = ctx.getResourceManager();
        FunctionCatalog functionCatalog = ctx.getFunctionCatalog();
        CatalogManager catalogManager = ctx.getCatalogManager();
        Set<String> jars = getJars(resourceManager, functionCatalog, catalogManager);

        return buildStringArrayResult("jars", jars.toArray(new String[0]));
    }

    public static Set<String> getJars(
            ResourceManager resourceManager,
            FunctionCatalog functionCatalog,
            CatalogManager catalogManager) {
        Set<String> jars =
                resourceManager.getResources().keySet().stream()
                        .map(ResourceUri::getUri)
                        .collect(Collectors.toSet());
        Set<FunctionIdentifier> identifiers =
                functionCatalog.getUserDefinedFunctions(
                        catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
        Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
        for (FunctionIdentifier identifier : identifiers) {
            if (identifier.getIdentifier().isPresent()) {
                ObjectIdentifier objectIdentifier = identifier.getIdentifier().get();
                try {
                    ObjectPath functionPath =
                            new ObjectPath(
                                    objectIdentifier.getDatabaseName(),
                                    objectIdentifier.getObjectName());
                    if (catalog.functionExists(functionPath)) {
                        CatalogFunction function = catalog.getFunction(functionPath);
                        jars.addAll(
                                function.getFunctionResources().stream()
                                        .filter(
                                                resourceUri ->
                                                        resourceUri.getResourceType()
                                                                == ResourceType.JAR)
                                        .map(ResourceUri::getUri)
                                        .collect(Collectors.toList()));
                    }
                } catch (FunctionNotExistException e) {
                    throw new TableException(
                            String.format(
                                    "Could not find catalog function [%s].", objectIdentifier),
                            e);
                }
            }
        }
        return jars;
    }
}
