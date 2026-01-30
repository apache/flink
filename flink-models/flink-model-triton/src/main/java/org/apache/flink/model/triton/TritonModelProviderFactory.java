/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;

import java.util.HashSet;
import java.util.Set;

/** {@link ModelProviderFactory} for Triton Inference Server model functions. */
public class TritonModelProviderFactory implements ModelProviderFactory {
    public static final String IDENTIFIER = "triton";

    @Override
    public ModelProvider createModelProvider(ModelProviderFactory.Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();

        // For now, we create a generic inference function
        // In the future, this could be extended to support different model types
        AsyncPredictFunction function =
                new TritonInferenceModelFunction(context, helper.getOptions());
        return new Provider(function);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TritonOptions.ENDPOINT);
        set.add(TritonOptions.MODEL_NAME);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TritonOptions.MODEL_VERSION);
        set.add(TritonOptions.TIMEOUT);
        set.add(TritonOptions.BATCH_SIZE);
        set.add(TritonOptions.FLATTEN_BATCH_DIM);
        set.add(TritonOptions.PRIORITY);
        set.add(TritonOptions.SEQUENCE_ID);
        set.add(TritonOptions.SEQUENCE_START);
        set.add(TritonOptions.SEQUENCE_END);
        set.add(TritonOptions.COMPRESSION);
        set.add(TritonOptions.AUTH_TOKEN);
        set.add(TritonOptions.CUSTOM_HEADERS);
        return set;
    }

    /** {@link ModelProvider} for Triton model functions. */
    public static class Provider implements AsyncPredictRuntimeProvider {
        private final AsyncPredictFunction function;

        public Provider(AsyncPredictFunction function) {
            this.function = function;
        }

        @Override
        public AsyncPredictFunction createAsyncPredictFunction(Context context) {
            return function;
        }

        @Override
        public ModelProvider copy() {
            return new Provider(function);
        }
    }
}
