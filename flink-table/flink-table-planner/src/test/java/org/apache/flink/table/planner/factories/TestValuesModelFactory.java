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

package org.apache.flink.table.planner.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;
import org.apache.flink.table.ml.PredictRuntimeProvider;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Values model factory. */
public class TestValuesModelFactory implements ModelProviderFactory {

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    protected static final Map<String, Map<Row, List<Row>>> REGISTERED_DATA = new HashMap<>();

    public static final ConfigOption<String> DATA_ID =
            ConfigOptions.key("data-id").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> ASYNC =
            ConfigOptions.key("async").booleanType().defaultValue(false);

    public static final ConfigOption<Integer> LATENCY =
            ConfigOptions.key("latency")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Latency in milliseconds for async prediction for each row. "
                                    + "If not set, the default is random between 0ms and 1000ms.");

    public static String registerData(Map<Row, List<Row>> data) {
        String id = String.valueOf(idCounter.incrementAndGet());
        REGISTERED_DATA.put(id, data);
        return id;
    }

    public static void clearAllData() {
        REGISTERED_DATA.clear();
    }

    @Override
    public ModelProvider createModelProvider(Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();
        String dataId = helper.getOptions().get(DATA_ID);
        Map<Row, List<Row>> rows = REGISTERED_DATA.getOrDefault(dataId, Collections.emptyMap());

        RowRowConverter inputConverter =
                RowRowConverter.create(
                        context.getCatalogModel().getResolvedInputSchema().toPhysicalRowDataType());
        RowRowConverter outputConverter =
                RowRowConverter.create(
                        context.getCatalogModel()
                                .getResolvedOutputSchema()
                                .toPhysicalRowDataType());

        if (helper.getOptions().get(ASYNC)) {
            return new AsyncValuesModelProvider(
                    new ValuesPredictFunction(rows, inputConverter, outputConverter),
                    new AsyncValuesPredictFunction(
                            rows,
                            inputConverter,
                            outputConverter,
                            helper.getOptions().getOptional(LATENCY).orElse(null)));
        } else {
            return new ValuesModelProvider(
                    new ValuesPredictFunction(rows, inputConverter, outputConverter));
        }
    }

    @Override
    public String factoryIdentifier() {
        return "values";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(Arrays.asList(ASYNC, DATA_ID, LATENCY));
    }

    private static Map<RowData, List<RowData>> toInternal(
            Map<Row, List<Row>> data,
            RowRowConverter keyConverter,
            RowRowConverter valueConverter,
            FunctionContext context) {
        keyConverter.open(context.getUserCodeClassLoader());
        valueConverter.open(context.getUserCodeClassLoader());

        Map<RowData, List<RowData>> converted = new HashMap<>();
        for (Map.Entry<Row, List<Row>> entry : data.entrySet()) {
            RowData input = keyConverter.toInternal(entry.getKey());
            converted.put(
                    input,
                    entry.getValue().stream()
                            .map(valueConverter::toInternal)
                            .collect(Collectors.toList()));
        }
        return converted;
    }

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

    /** Values Predict function. */
    public static class ValuesPredictFunction extends PredictFunction {

        private final Map<Row, List<Row>> data;
        private final RowRowConverter inputConverter;
        private final RowRowConverter outputConverter;

        private transient Map<RowData, List<RowData>> converted;

        public ValuesPredictFunction(
                Map<Row, List<Row>> data,
                RowRowConverter inputConverter,
                RowRowConverter outputConverter) {
            this.data = data;
            this.inputConverter = inputConverter;
            this.outputConverter = outputConverter;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            converted = toInternal(data, inputConverter, outputConverter, context);
        }

        @Override
        public Collection<RowData> predict(RowData features) {
            return Preconditions.checkNotNull(converted.get(features));
        }
    }

    /** Async values predict function. */
    public static class AsyncValuesPredictFunction extends AsyncPredictFunction {

        private final Map<Row, List<Row>> data;
        private final RowRowConverter inputConverter;
        private final RowRowConverter outputConverter;
        private final Integer latency;

        private transient Random random;
        private transient Map<RowData, List<RowData>> converted;
        private transient ExecutorService executors;

        public AsyncValuesPredictFunction(
                Map<Row, List<Row>> data,
                RowRowConverter inputConverter,
                RowRowConverter outputConverter,
                Integer latency) {
            this.data = data;
            this.inputConverter = inputConverter;
            this.outputConverter = outputConverter;
            this.latency = latency;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            random = new Random();
            converted = toInternal(data, inputConverter, outputConverter, context);
            executors = Executors.newFixedThreadPool(5);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncPredict(RowData features) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            int sleepTime = latency == null ? (random.nextInt(1000)) : latency;
                            Thread.sleep(sleepTime);
                            return Preconditions.checkNotNull(converted.get(features));
                        } catch (Exception e) {
                            throw new RuntimeException("Execution is interrupted.", e);
                        }
                    },
                    executors);
        }

        @Override
        public void close() throws Exception {
            super.close();
            executors.shutdownNow();
        }
    }
}
