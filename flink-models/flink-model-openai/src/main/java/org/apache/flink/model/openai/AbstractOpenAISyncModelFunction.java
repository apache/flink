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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.PredictFunction;

import com.openai.client.OpenAIClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Abstract parent class for synchronous {@link PredictFunction}s backed by the OpenAI API. Mirrors
 * {@link AbstractOpenAIAsyncModelFunction} but holds an {@link OpenAIClient} instead of an {@link
 * com.openai.client.OpenAIClientAsync}.
 */
public abstract class AbstractOpenAISyncModelFunction extends PredictFunction {
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractOpenAISyncModelFunction.class);

    protected final OpenAIModelCommons commons;
    protected transient OpenAIClient client;

    public AbstractOpenAISyncModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        this.commons = new OpenAIModelCommons(factoryContext, config, getEndpointSuffix());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.debug("Creating an OpenAI sync client.");
        this.client =
                OpenAIUtils.createSyncClient(commons.baseUrl, commons.apiKey, commons.numRetry);
        commons.initializeAtOpen();
    }

    @Override
    public Collection<RowData> predict(RowData rowData) {
        String input = commons.prepareInput(rowData);
        if (input == null) {
            return Collections.emptyList();
        }
        try {
            return predictInternal(input);
        } catch (Throwable t) {
            return commons.handleErrorsAndRespond(t);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.client != null) {
            LOG.debug("Releasing the OpenAI sync client.");
            OpenAIUtils.releaseSyncClient(commons.baseUrl, commons.apiKey);
            client = null;
        }
    }

    protected abstract String getEndpointSuffix();

    /**
     * Issues the synchronous OpenAI request for the given pre-processed input. Implementations
     * should let any client-side exception propagate so the surrounding {@link #predict} can apply
     * the configured {@link ErrorHandlingStrategy}.
     */
    protected abstract Collection<RowData> predictInternal(String input);
}
