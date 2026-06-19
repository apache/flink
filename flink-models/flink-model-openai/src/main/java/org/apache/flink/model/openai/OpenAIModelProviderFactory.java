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

package org.apache.flink.model.openai;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;

import java.util.HashSet;
import java.util.Set;

/** {@link ModelProviderFactory} for OpenAI model functions. */
public class OpenAIModelProviderFactory implements ModelProviderFactory {
    public static final String IDENTIFIER = "openai";

    @Override
    public ModelProvider createModelProvider(ModelProviderFactory.Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();
        String endpoint = helper.getOptions().get(OpenAIOptions.ENDPOINT);
        endpoint = endpoint.replaceAll("/*$", "").toLowerCase();

        AsyncPredictFunction function;
        if (endpoint.endsWith(OpenAIEmbeddingModelFunction.ENDPOINT_SUFFIX)) {
            function = new OpenAIEmbeddingModelFunction(context, helper.getOptions());
        } else if (endpoint.endsWith(OpenAIChatModelFunction.ENDPOINT_SUFFIX)) {
            function = new OpenAIChatModelFunction(context, helper.getOptions());
        } else {
            throw new UnsupportedOperationException("Unsupported endpoint: " + endpoint);
        }
        return new Provider(function);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(OpenAIOptions.ENDPOINT);
        set.add(OpenAIOptions.API_KEY);
        set.add(OpenAIOptions.MODEL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(OpenAIOptions.MAX_CONTEXT_SIZE);
        set.add(OpenAIOptions.CONTEXT_OVERFLOW_ACTION);
        set.add(OpenAIOptions.ERROR_HANDLING_STRATEGY);
        set.add(OpenAIOptions.RETRY_NUM);
        set.add(OpenAIOptions.RETRY_FALLBACK_STRATEGY);
        set.add(OpenAIOptions.SYSTEM_PROMPT);
        set.add(OpenAIOptions.TEMPERATURE);
        set.add(OpenAIOptions.TOP_P);
        set.add(OpenAIOptions.STOP);
        set.add(OpenAIOptions.MAX_TOKENS);
        set.add(OpenAIOptions.PRESENCE_PENALTY);
        set.add(OpenAIOptions.N);
        set.add(OpenAIOptions.SEED);
        set.add(OpenAIOptions.RESPONSE_FORMAT);
        set.add(OpenAIOptions.DIMENSION);
        return set;
    }

    /** {@link ModelProvider} for openai model functions. */
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
