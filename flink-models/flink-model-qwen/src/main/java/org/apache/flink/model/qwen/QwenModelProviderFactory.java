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

package org.apache.flink.model.qwen;

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
public class QwenModelProviderFactory implements ModelProviderFactory {
    public static final String IDENTIFIER = "qwen";

    @Override
    public ModelProvider createModelProvider(Context context) {
        FactoryUtil.ModelProviderFactoryHelper helper =
                FactoryUtil.createModelProviderFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        String endpoint = helper.getOptions().get(AbstractQwenModelFunction.MODEL);

        AsyncPredictFunction function;
        if (endpoint.startsWith(QwenEmbeddingModelFunction.MODEL_PREFIX)) {
            function = new QwenEmbeddingModelFunction(context, helper.getOptions());
        } else if (endpoint.startsWith(QwenChatModelFunction.MODEL_PREFIX)) {
            function = new QwenChatModelFunction(context, helper.getOptions());
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
        set.add(AbstractQwenModelFunction.ENDPOINT);
        set.add(AbstractQwenModelFunction.API_KEY);
        set.add(AbstractQwenModelFunction.MODEL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(QwenChatModelFunction.SYSTEM_PROMPT);
        set.add(QwenChatModelFunction.TEMPERATURE);
        set.add(QwenChatModelFunction.TOP_P);
        set.add(QwenChatModelFunction.STOP);
        set.add(QwenChatModelFunction.MAX_TOKENS);
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
