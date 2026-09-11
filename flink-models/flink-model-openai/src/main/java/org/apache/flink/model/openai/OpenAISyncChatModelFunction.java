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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.PredictFunction;

import java.util.Collection;

/** Synchronous {@link PredictFunction} for OpenAI chat completion task. */
public class OpenAISyncChatModelFunction extends AbstractOpenAISyncModelFunction {
    private static final long serialVersionUID = 1L;

    private final OpenAIChatTask task;

    public OpenAISyncChatModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);
        this.task = new OpenAIChatTask(factoryContext, config, commons);
    }

    @Override
    protected String getEndpointSuffix() {
        return OpenAIChatTask.ENDPOINT_SUFFIX;
    }

    @Override
    protected Collection<RowData> predictInternal(String input) {
        return task.convertToRowData(
                client.chat().completions().create(task.buildParams(input)), null);
    }
}
