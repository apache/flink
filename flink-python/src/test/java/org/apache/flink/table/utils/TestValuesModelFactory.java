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

package org.apache.flink.table.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test values model factory for Python integration tests.
 *
 * <p>This factory generates predictable output based on input: for input with first column as id
 * (BIGINT), it generates output ("x" + id, id, "z" + id).
 */
public class TestValuesModelFactory implements ModelProviderFactory {

    public static final ConfigOption<Boolean> ASYNC =
            ConfigOptions.key("async").booleanType().defaultValue(false);

    public static final ConfigOption<Integer> LATENCY =
            ConfigOptions.key("latency")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Latency in milliseconds for async prediction. "
                                    + "If not set, defaults to 0ms (no delay).");

    @Override
    public ModelProvider createModelProvider(Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();

        RowRowConverter outputConverter =
                RowRowConverter.create(
                        context.getCatalogModel()
                                .getResolvedOutputSchema()
                                .toPhysicalRowDataType());

        if (helper.getOptions().get(ASYNC)) {
            return new AsyncValuesModelProvider(
                    new GeneratingPredictFunction(outputConverter),
                    new AsyncGeneratingPredictFunction(
                            outputConverter, helper.getOptions().getOptional(LATENCY).orElse(0)));
        } else {
            return new ValuesModelProvider(new GeneratingPredictFunction(outputConverter));
        }
    }

    @Override
    public String factoryIdentifier() {
        return "python-values";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(Arrays.asList(ASYNC, LATENCY));
    }

    /** Values model provider implementing sync predict. */
    public static class ValuesModelProvider implements PredictRuntimeProvider {

        private final PredictFunction function;

        public ValuesModelProvider(PredictFunction function) {
            this.function = function;
        }

        @Override
        public PredictFunction createPredictFunction(Context context) {
            return function;
        }

        @Override
        public ModelProvider copy() {
            return new ValuesModelProvider(function);
        }
    }

    /** Async values model provider implementing both sync and async predict. */
    public static class AsyncValuesModelProvider
            implements AsyncPredictRuntimeProvider, PredictRuntimeProvider {

        private final PredictFunction predictFunction;
        private final AsyncPredictFunction asyncPredictFunction;

        public AsyncValuesModelProvider(
                PredictFunction predictFunction, AsyncPredictFunction asyncPredictFunction) {
            this.predictFunction = predictFunction;
            this.asyncPredictFunction = asyncPredictFunction;
        }

        @Override
        public AsyncPredictFunction createAsyncPredictFunction(Context context) {
            return asyncPredictFunction;
        }

        @Override
        public PredictFunction createPredictFunction(Context context) {
            return predictFunction;
        }

        @Override
        public ModelProvider copy() {
            return new AsyncValuesModelProvider(predictFunction, asyncPredictFunction);
        }
    }

    /**
     * Predict function that generates output based on input.
     *
     * <p>For input row with first column value as id (BIGINT), generates: ("x" + id, id, "z" + id).
     */
    public static class GeneratingPredictFunction extends PredictFunction {

        private final RowRowConverter outputConverter;

        public GeneratingPredictFunction(RowRowConverter outputConverter) {
            this.outputConverter = outputConverter;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            outputConverter.open(context.getUserCodeClassLoader());
        }

        @Override
        public Collection<RowData> predict(RowData features) {
            // Extract the first field as id
            long id = features.getLong(0);
            // Generate output: ("x" + id, id, "z" + id)
            GenericRowData output =
                    GenericRowData.of(
                            StringData.fromString("x" + id),
                            (int) id,
                            StringData.fromString("z" + id));
            return Collections.singletonList(output);
        }
    }

    /** Async predict function that generates output with configurable latency. */
    public static class AsyncGeneratingPredictFunction extends AsyncPredictFunction {

        private final RowRowConverter outputConverter;
        private final int latency;

        private transient ExecutorService executors;

        public AsyncGeneratingPredictFunction(RowRowConverter outputConverter, int latency) {
            this.outputConverter = outputConverter;
            this.latency = latency;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            outputConverter.open(context.getUserCodeClassLoader());
            executors = Executors.newFixedThreadPool(5);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncPredict(RowData features) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            if (latency > 0) {
                                Thread.sleep(latency);
                            }
                            // Extract the first field as id
                            long id = features.getLong(0);
                            // Generate output: ("x" + id, id, "z" + id)
                            GenericRowData output =
                                    GenericRowData.of(
                                            StringData.fromString("x" + id),
                                            (int) id,
                                            StringData.fromString("z" + id));
                            return Collections.singletonList(output);
                        } catch (Exception e) {
                            throw new RuntimeException("Prediction interrupted.", e);
                        }
                    },
                    executors);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (executors != null) {
                executors.shutdownNow();
            }
        }
    }
}
