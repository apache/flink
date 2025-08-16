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

package org.apache.flink.model.deepseek;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;

import java.util.HashSet;
import java.util.Set;

/** {@link ModelProviderFactory} for DeepSeek model functions. */
public class DeepSeekModelProviderFactory implements ModelProviderFactory {
    public static final String IDENTIFIER = "deepseek";

    @Override
    public ModelProvider createModelProvider(Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        String endpoint = config.get(AbstractDeepSeekModelFunction.ENDPOINT);
        String apiKey = config.get(AbstractDeepSeekModelFunction.API_KEY);
        endpoint = endpoint.replaceAll("/*$", "").toLowerCase();
        AsyncPredictFunction function = new DeepSeekChatAndEmbeddingModelFunction(context, config);
        return new Provider(function);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(DeepSeekChatAndEmbeddingModelFunction.ENDPOINT);
        set.add(DeepSeekChatAndEmbeddingModelFunction.API_KEY);
        set.add(DeepSeekChatAndEmbeddingModelFunction.MODEL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(DeepSeekChatAndEmbeddingModelFunction.SYSTEM_PROMPT);
        set.add(DeepSeekChatAndEmbeddingModelFunction.TEMPERATURE);
        set.add(DeepSeekChatAndEmbeddingModelFunction.TOP_P);
        set.add(DeepSeekChatAndEmbeddingModelFunction.STOP);
        set.add(DeepSeekChatAndEmbeddingModelFunction.MAX_TOKENS);
        return set;
    }

    /** {@link ModelProvider} for DeepSeek model functions. */
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
