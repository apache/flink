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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;

import com.openai.models.embeddings.CreateEmbeddingResponse;
import com.openai.models.embeddings.EmbeddingCreateParams;
import com.openai.models.embeddings.EmbeddingCreateParams.EncodingFormat;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Per-task state and SDK plumbing for OpenAI embedding requests, shared between the async and sync
 * embedding model functions.
 */
class OpenAIEmbeddingTask implements Serializable {
    private static final long serialVersionUID = 1L;

    static final String ENDPOINT_SUFFIX = "embeddings";

    private final OpenAIModelCommons commons;
    @Nullable private final Long dimensions;
    private final int outputColumnIndex;

    OpenAIEmbeddingTask(
            ModelProviderFactory.Context factoryContext,
            ReadableConfig config,
            OpenAIModelCommons commons) {
        this.commons = commons;
        this.dimensions = config.get(OpenAIOptions.DIMENSION);
        OpenAIModelCommons.validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                new ArrayType(new FloatType()),
                "output");
        this.outputColumnIndex = commons.resolvePhysicalOutputColumnIndex();
    }

    EmbeddingCreateParams buildParams(String input) {
        EmbeddingCreateParams.Builder builder = EmbeddingCreateParams.builder();
        builder.model(commons.model);
        builder.input(input);
        builder.encodingFormat(EncodingFormat.FLOAT);
        if (dimensions != null) {
            builder.dimensions(dimensions);
        }
        return builder.build();
    }

    Collection<RowData> convertToRowData(
            @Nullable CreateEmbeddingResponse response, @Nullable Throwable throwable) {
        if (throwable != null) {
            return commons.handleErrorsAndRespond(throwable);
        }
        return response.data().stream()
                .map(
                        embedding -> {
                            GenericRowData rowData =
                                    new GenericRowData(commons.outputColumnNames.size());
                            rowData.setField(
                                    outputColumnIndex,
                                    new GenericArrayData(
                                            embedding.embedding().stream()
                                                    .map(Double::floatValue)
                                                    .toArray(Float[]::new)));
                            return rowData;
                        })
                .collect(Collectors.toList());
    }
}
