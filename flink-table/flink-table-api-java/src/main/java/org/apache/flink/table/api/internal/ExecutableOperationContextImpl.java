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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.resource.ResourceManager;

/** A simple implementation of {@link ExecutableOperation.Context}. */
@Internal
public class ExecutableOperationContextImpl implements ExecutableOperation.Context {

    private final CatalogManager catalogManager;
    private final FunctionCatalog functionCatalog;
    private final ModuleManager moduleManager;
    private final ResourceManager resourceManager;
    private final TableConfig tableConfig;

    private final boolean isStreamingMode;

    public ExecutableOperationContextImpl(
            CatalogManager catalogManager,
            FunctionCatalog functionCatalog,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            TableConfig tableConfig,
            boolean isStreamingMode) {
        this.catalogManager = catalogManager;
        this.functionCatalog = functionCatalog;
        this.moduleManager = moduleManager;
        this.resourceManager = resourceManager;
        this.tableConfig = tableConfig;
        this.isStreamingMode = isStreamingMode;
    }

    @Override
    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    @Override
    public FunctionCatalog getFunctionCatalog() {
        return functionCatalog;
    }

    @Override
    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    @Override
    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    @Override
    public TableConfig getTableConfig() {
        return tableConfig;
    }

    @Override
    public boolean isStreamingMode() {
        return isStreamingMode;
    }
}
