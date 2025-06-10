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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.factories.FactoryUtil.ModelProviderFactoryHelper;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;

import java.util.HashSet;
import java.util.Set;

/** Test implementation for {@link ModelProviderFactory} which creates PredictRuntimeProvider. */
public final class TestModelProviderFactory implements ModelProviderFactory {

    public static final String IDENTIFIER = "test-model";

    public static final ConfigOption<String> TASK =
            ConfigOptions.key("task")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Task of the test model.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Endpoint of the test model.");

    public static final ConfigOption<Integer> MODEL_VERSION =
            ConfigOptions.key("version")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Version of the test model.");

    @Override
    public ModelProvider createModelProvider(Context context) {
        final ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();
        return new TestModelProviderMock(context.getCatalogModel());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TASK);
        options.add(ENDPOINT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MODEL_VERSION);
        return options;
    }

    /** Test implementation of {@link ModelProvider} for testing purposes. */
    public static class TestModelProviderMock implements PredictRuntimeProvider {

        private final ResolvedCatalogModel catalogModel;

        public TestModelProviderMock(ResolvedCatalogModel catalogModel) {
            this.catalogModel = catalogModel;
        }

        @Override
        public ModelProvider copy() {
            return new TestModelProviderMock(catalogModel);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return catalogModel.equals(((TestModelProviderMock) o).catalogModel);
        }

        @Override
        public PredictFunction createPredictFunction(Context context) {
            throw new UnsupportedOperationException("To be implemented");
        }
    }
}
