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

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;

import java.util.Objects;

/**
 * {@link ModelProviderSpec} describes how to serialize/deserialize model provider and create {@link
 * ModelProvider} from the deserialization result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelProviderSpec {

    public static final String FIELD_NAME_CATALOG_MODEL = "model";

    private final ContextResolvedModel contextResolvedModel;
    private ModelProvider modelProvider;

    @JsonCreator
    public ModelProviderSpec(
            @JsonProperty(FIELD_NAME_CATALOG_MODEL) ContextResolvedModel contextResolvedModel) {
        this.contextResolvedModel =
                checkNotNull(contextResolvedModel, "contextResolvedModel cannot be null");
    }

    public ModelProvider getModelProvider(FlinkContext context) {
        if (modelProvider == null) {
            ModelProviderFactory factory =
                    context.getModuleManager()
                            .getFactory(Module::getModelProviderFactory)
                            .orElse(null);

            if (factory == null) {
                factory =
                        (ModelProviderFactory)
                                context.getCatalogManager()
                                        .getCatalog(
                                                contextResolvedModel
                                                        .getIdentifier()
                                                        .getCatalogName())
                                        .flatMap(Catalog::getFactory)
                                        .filter(f -> f instanceof ModelProviderFactory)
                                        .orElse(null);
            }

            modelProvider =
                    FactoryUtil.createModelProvider(
                            factory,
                            contextResolvedModel.getIdentifier(),
                            contextResolvedModel.getResolvedModel(),
                            context.getTableConfig(),
                            context.getClassLoader(),
                            contextResolvedModel.isTemporary());
        }
        return modelProvider;
    }

    public PredictRuntimeProvider getPredictRuntimeProvider(FlinkContext context) {
        ModelProvider provider = getModelProvider(context);
        if (provider instanceof PredictRuntimeProvider) {
            return (PredictRuntimeProvider) provider;
        } else {
            throw new TableException(
                    String.format(
                            "%s is not a PredictRuntimeProvider.\nPlease check it.",
                            provider.getClass().getName()));
        }
    }

    @JsonGetter(FIELD_NAME_CATALOG_MODEL)
    public ContextResolvedModel getContextResolvedModel() {
        return contextResolvedModel;
    }

    public void setModelProvider(ModelProvider modelProvider) {
        this.modelProvider = modelProvider;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ModelProviderSpec that = (ModelProviderSpec) o;
        return Objects.equals(contextResolvedModel, that.contextResolvedModel)
                && Objects.equals(modelProvider, that.modelProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextResolvedModel, modelProvider);
    }

    @Override
    public String toString() {
        return "ModelProviderSpec{"
                + "contextResolvedModel="
                + contextResolvedModel
                + ", modelProvider="
                + modelProvider
                + '}';
    }
}
