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

package org.apache.flink.model.openai;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.openai.client.OpenAIClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Abstract parent class for {@link AsyncPredictFunction}s for OpenAI API. */
public abstract class AbstractOpenAIAsyncModelFunction extends AsyncPredictFunction {
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractOpenAIAsyncModelFunction.class);

    protected final OpenAIModelCommons commons;
    protected transient OpenAIClientAsync client;

    public AbstractOpenAIAsyncModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        this.commons = new OpenAIModelCommons(factoryContext, config, getEndpointSuffix());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.debug("Creating an OpenAI async client.");
        this.client =
                OpenAIUtils.createAsyncClient(commons.baseUrl, commons.apiKey, commons.numRetry);
        commons.initializeAtOpen();
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredict(RowData rowData) {
        String input = commons.prepareInput(rowData);
        if (input == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return asyncPredictInternal(input);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.client != null) {
            LOG.debug("Releasing the OpenAI async client.");
            OpenAIUtils.releaseAsyncClient(commons.baseUrl, commons.apiKey);
            client = null;
        }
    }

    protected abstract String getEndpointSuffix();

    protected abstract CompletableFuture<Collection<RowData>> asyncPredictInternal(String input);
}
