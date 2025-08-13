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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import java.io.IOException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@link AsyncPredictFunction} for DeepSeek chat completion task. */
public class DeepSeekChatAndEmbeddingModelFunction extends AbstractDeepSeekModelFunction {
    private static final long serialVersionUID = 1L;

    public static final String ENDPOINT_SUFFIX = "chat/completions";

    public static final ConfigOption<String> SYSTEM_PROMPT = 
            ConfigOptions.key("system-prompt")
                    .stringType()
                    .defaultValue("You are a helpful assistant.")
                    .withDescription("System message for chat tasks.");

    public static final ConfigOption<Double> TEMPERATURE = 
            ConfigOptions.key("temperature")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("Controls randomness of output, range [0.0, 1.0].");

    public static final ConfigOption<Double> TOP_P = 
            ConfigOptions.key("top-p")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "Probability cutoff for token selection (used instead of temperature).");

    public static final String STOP_SEPARATOR = ",";

    public static final ConfigOption<String> STOP = 
            ConfigOptions.key("stop")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stop sequences, comma-separated list.");

    public static final ConfigOption<Long> MAX_TOKENS = 
            ConfigOptions.key("max-tokens")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Maximum number of tokens to generate.");

    private final String model;
    private final String systemPrompt;
    @Nullable private final Double temperature;
    @Nullable private final Double topP;
    @Nullable private final List<String> stop;
    @Nullable private final Long maxTokens;
    private final String url;
    private final String apiKey;

    public DeepSeekChatAndEmbeddingModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config,String url,String apiKey) {
        super(factoryContext, config);
        model = config.get(MODEL);
        systemPrompt = config.get(SYSTEM_PROMPT);
        temperature = config.get(TEMPERATURE);
        topP = config.get(TOP_P);
        stop = 
                config.get(STOP) == null
                        ? null
                        : Arrays.asList(config.get(STOP).split(STOP_SEPARATOR));
        maxTokens = config.get(MAX_TOKENS);
        this.url = url;
        this.apiKey = apiKey;
        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                new VarCharType(VarCharType.MAX_LENGTH),
                "output");
    }

    @Override
    protected String getEndpointSuffix() {
        return ENDPOINT_SUFFIX;
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredict(RowData rowData) {
        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("model", "deepseek-chat");

        // Create messages array
        JsonObject systemMessage = new JsonObject();
        systemMessage.addProperty("role", "system");
        systemMessage.addProperty("content", systemPrompt);

        JsonObject userMessage = new JsonObject();
        userMessage.addProperty("role", "user");
        userMessage.addProperty("content", rowData.getString(0).toString());

        requestBody.add("messages", new Gson().toJsonTree(Arrays.asList(systemMessage, userMessage)));

        // Add optional parameters
        if (temperature != null) {
            requestBody.addProperty("temperature", temperature);
        }
        if (topP != null) {
            requestBody.addProperty("top_p", topP);
        }
        if (stop != null) {
            requestBody.add("stop", new Gson().toJsonTree(stop));
        }
        if (maxTokens != null) {
            requestBody.addProperty("max_tokens", maxTokens);
        }

        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");

        String jsonBody = new Gson().toJson(requestBody);

        RequestBody body = RequestBody.create(mediaType,jsonBody);

        // Create request
        Request request = new Request.Builder()
                .url(url)
                .header("Authorization", "Bearer " + apiKey)  // Adding Authorization header
                .post(body)
                .build();
        
        // Execute request asynchronously
        return CompletableFuture.supplyAsync(() -> {
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("DeepSeek API request failed: " + response);
                }
                
                String responseBody = response.body().string();
                JsonObject jsonResponse = new Gson().fromJson(responseBody, JsonObject.class);
                
                return convertToRowData(jsonResponse);
            } catch (IOException e) {
                throw new RuntimeException("Error calling DeepSeek API", e);
            }
        });
    }

    private List<RowData> convertToRowData(JsonObject chatCompletion) {
        return chatCompletion.getAsJsonArray("choices").asList().stream()
                .map(choice -> {
                    JsonObject choiceObj = choice.getAsJsonObject();
                    JsonObject messageObj = choiceObj.getAsJsonObject("message");
                    String content = messageObj.get("content").getAsString();
                    return GenericRowData.of(BinaryStringData.fromString(content));
                })
                .collect(Collectors.toList());
    }
}
