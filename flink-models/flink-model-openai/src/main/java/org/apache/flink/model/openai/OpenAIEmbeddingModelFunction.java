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
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;

import com.openai.models.embeddings.CreateEmbeddingResponse;
import com.openai.models.embeddings.EmbeddingCreateParams;
import com.openai.models.embeddings.EmbeddingCreateParams.EncodingFormat;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@link AsyncPredictFunction} for OpenAI embedding task. */
public class OpenAIEmbeddingModelFunction extends AbstractOpenAIModelFunction {
    private static final long serialVersionUID = 1L;

    public static final String ENDPOINT_SUFFIX = "embeddings";

    private final String model;
    @Nullable private final Long dimensions;
    private final int outputColumnIndex;

    public OpenAIEmbeddingModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);
        model = config.get(OpenAIOptions.MODEL);
        dimensions = config.get(OpenAIOptions.DIMENSION);

        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                new ArrayType(new FloatType()),
                "output");
        this.outputColumnIndex = getOutputColumnIndex();
    }

    private int getOutputColumnIndex() {
        for (int i = 0; i < this.outputColumnNames.size(); i++) {
            String columnName = this.outputColumnNames.get(i);
            if (ErrorMessageMetadata.get(columnName) == null) {
                // Prior checks have guaranteed that there is one and only one physical output
                // column.
                return i;
            }
        }
        throw new IllegalArgumentException(
                "There should be one and only one physical output column. Actual columns: "
                        + this.outputColumnNames);
    }

    @Override
    protected String getEndpointSuffix() {
        return ENDPOINT_SUFFIX;
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredictInternal(String input) {
        final EmbeddingCreateParams.Builder builder = EmbeddingCreateParams.builder();
        builder.model(model);
        builder.input(input);
        builder.encodingFormat(EncodingFormat.FLOAT);
        if (dimensions != null) {
            builder.dimensions(dimensions);
        }

        return client.embeddings().create(builder.build()).handle(this::convertToRowData);
    }

    private Collection<RowData> convertToRowData(
            CreateEmbeddingResponse response, Throwable throwable) {
        if (throwable != null) {
            return handleErrorsAndRespond(throwable);
        }

        return response.data().stream()
                .map(
                        embedding -> {
                            GenericRowData rowData =
                                    new GenericRowData(this.outputColumnNames.size());
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
