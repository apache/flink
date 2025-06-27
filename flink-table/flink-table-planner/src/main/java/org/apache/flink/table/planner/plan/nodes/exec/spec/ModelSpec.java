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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static org.apache.flink.util.OptionalUtils.firstPresent;

/** Spec to describe model. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelSpec {

    public static final String FIELD_NAME_CATALOG_MODEL = "model";

    private final ContextResolvedModel contextResolvedModel;
    private ModelProvider provider;

    @JsonCreator
    public ModelSpec(
            @JsonProperty(FIELD_NAME_CATALOG_MODEL) ContextResolvedModel contextResolvedModel) {
        this.contextResolvedModel = contextResolvedModel;
    }

    @JsonGetter(FIELD_NAME_CATALOG_MODEL)
    public ContextResolvedModel getContextResolvedModel() {
        return contextResolvedModel;
    }

    public void setModelProvider(ModelProvider provider) {
        this.provider = provider;
    }

    public ModelProvider getModelProvider(FlinkContext context) {
        if (provider == null) {
            final Optional<ModelProviderFactory> factoryFromCatalog =
                    contextResolvedModel
                            .getCatalog()
                            .flatMap(Catalog::getFactory)
                            .map(
                                    f ->
                                            f instanceof ModelProviderFactory
                                                    ? (ModelProviderFactory) f
                                                    : null);

            final Optional<ModelProviderFactory> factoryFromModule =
                    context.getModuleManager().getFactory(Module::getModelProviderFactory);

            // Since the catalog is more specific, we give it precedence over a factory provided by
            // any modules.
            final ModelProviderFactory factory =
                    firstPresent(factoryFromCatalog, factoryFromModule).orElse(null);

            provider =
                    FactoryUtil.createModelProvider(
                            factory,
                            contextResolvedModel.getIdentifier(),
                            contextResolvedModel.getResolvedModel(),
                            context.getTableConfig(),
                            context.getClassLoader(),
                            contextResolvedModel.isTemporary());
        }
        return provider;
    }
}
