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

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.resource.ResourceManager;

import java.util.ArrayList;
import java.util.List;

/** A simple implementation of {@link ExecutableOperation.Context}. */
public class ExecutableOperationContextImpl implements ExecutableOperation.Context {

    private final CatalogManager catalogManager;
    private final FunctionCatalog functionCatalog;
    private final ModuleManager moduleManager;
    private final ResourceManager resourceManager;
    private final TableConfig tableConfig;
    private final Planner planner;
    private final Executor execEnv;

    private final boolean isStreamingMode;

    public ExecutableOperationContextImpl(
            CatalogManager catalogManager,
            FunctionCatalog functionCatalog,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            TableConfig tableConfig,
            boolean isStreamingMode,
            Planner planner,
            Executor execEnv) {
        this.catalogManager = catalogManager;
        this.functionCatalog = functionCatalog;
        this.moduleManager = moduleManager;
        this.resourceManager = resourceManager;
        this.tableConfig = tableConfig;
        this.isStreamingMode = isStreamingMode;
        this.planner = planner;
        this.execEnv = execEnv;
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

    @Override
    public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
        return planner.translate(modifyOperations);
    }

    @Override
    public TableResultInternal executeInternal(
            List<Transformation<?>> transformations, List<String> sinkIdentifierNames) {
        final String defaultJobName = "insert-into_" + String.join(",", sinkIdentifierNames);

        resourceManager.addJarConfiguration(tableConfig);

        // We pass only the configuration to avoid reconfiguration with the rootConfiguration
        Pipeline pipeline =
                execEnv.createPipeline(
                        transformations, tableConfig.getConfiguration(), defaultJobName);
        try {
            JobClient jobClient = execEnv.executeAsync(pipeline);
            final List<Column> columns = new ArrayList<>();
            Long[] affectedRowCounts = new Long[transformations.size()];
            for (int i = 0; i < transformations.size(); ++i) {
                // use sink identifier name as field name
                columns.add(Column.physical(sinkIdentifierNames.get(i), DataTypes.BIGINT()));
                affectedRowCounts[i] = -1L;
            }

            return TableResultImpl.builder()
                    .jobClient(jobClient)
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .schema(ResolvedSchema.of(columns))
                    .resultProvider(
                            new InsertResultProvider(affectedRowCounts).setJobClient(jobClient))
                    .build();
        } catch (Exception e) {
            throw new TableException("Failed to execute sql", e);
        }
    }
}
