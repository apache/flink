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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.collections.IteratorUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the synchronous OpenAI predict functions ({@link OpenAISyncEmbeddingModelFunction} and
 * {@link OpenAISyncChatModelFunction}), exercised via the {@code MAP['async', 'false']} runtime
 * config on {@code ML_PREDICT}.
 */
public class OpenAISyncModelFunctionTest {

    private static final String MODEL_NAME = "m";

    private static final Schema STRING_INPUT_SCHEMA =
            Schema.newBuilder().column("input", DataTypes.STRING()).build();
    private static final Schema EMBEDDING_OUTPUT_SCHEMA =
            Schema.newBuilder().column("embedding", DataTypes.ARRAY(DataTypes.FLOAT())).build();
    private static final Schema CHAT_OUTPUT_SCHEMA =
            Schema.newBuilder().column("content", DataTypes.STRING()).build();

    private static MockWebServer server;

    private TableEnvironment tEnv;

    @BeforeAll
    public static void beforeAll() throws IOException {
        server = new MockWebServer();
        server.setDispatcher(new TestDispatcher());
        server.start();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    public void setup() {
        tEnv = TableEnvironment.create(new Configuration());
        tEnv.executeSql(
                "CREATE TABLE MyTable(input STRING) WITH ( 'connector' = 'datagen', 'number-of-rows' = '5')");
    }

    @AfterEach
    public void afterEach() {
        // The sync path uses the sync client cache; the async cache should remain untouched.
        assertThat(OpenAIUtils.getCache()).isEmpty();
        assertThat(OpenAIUtils.getSyncCache()).isEmpty();
    }

    @Test
    public void testSyncEmbedding() {
        Map<String, String> modelOptions = baseModelOptions("embeddings");
        modelOptions.put("model", "text-embedding-v3");

        createModel(modelOptions, STRING_INPUT_SCHEMA, EMBEDDING_OUTPUT_SCHEMA);

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, embedding FROM TABLE("
                                        + "ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`),"
                                        + " MAP['async', 'false']))",
                                MODEL_NAME));
        @SuppressWarnings("unchecked")
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(5);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat((Float[]) row.getFieldAs(1)).hasSize(512);
        }
    }

    @Test
    public void testSyncChat() {
        Map<String, String> modelOptions = baseModelOptions("chat/completions");
        modelOptions.put("model", "gpt-3.5-turbo");
        modelOptions.put("system-prompt", "you are a tester");

        createModel(modelOptions, STRING_INPUT_SCHEMA, CHAT_OUTPUT_SCHEMA);

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM TABLE("
                                        + "ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`),"
                                        + " MAP['async', 'false']))",
                                MODEL_NAME));
        @SuppressWarnings("unchecked")
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(5);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1)).isEqualTo("stubbed-completion");
        }
    }

    private Map<String, String> baseModelOptions(String endpointPath) {
        Map<String, String> options = new HashMap<>();
        options.put("provider", "openai");
        options.put("endpoint", server.url("/" + endpointPath).toString());
        options.put("api-key", "foobar");
        return options;
    }

    private void createModel(
            Map<String, String> modelOptions, Schema inputSchema, Schema outputSchema) {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        catalogManager.createModel(
                CatalogModel.of(inputSchema, outputSchema, modelOptions, "Test model."),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME),
                false);
    }

    private static class TestDispatcher extends Dispatcher {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public MockResponse dispatch(RecordedRequest request) {
            assert request.getRequestUrl() != null;
            String path = request.getRequestUrl().encodedPath();
            String body = request.getBody().readUtf8();

            if (path.endsWith("/embeddings")) {
                return dispatchEmbedding(body);
            }
            if (path.endsWith("/chat/completions")) {
                return dispatchChat();
            }
            return new MockResponse().setResponseCode(404);
        }

        private MockResponse dispatchEmbedding(String body) {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(body);
                int dimensions = root.has("dimensions") ? root.get("dimensions").asInt() : 512;
                String embeddingStr =
                        IntStream.range(0, dimensions)
                                .mapToDouble(i -> i % 2 == 0 ? 0.1 : -0.1)
                                .mapToObj(Double::toString)
                                .collect(Collectors.joining(", "));

                String responseBody =
                        "{"
                                + "  \"object\": \"list\","
                                + "  \"data\": [{"
                                + "    \"object\": \"embedding\","
                                + "    \"embedding\": ["
                                + embeddingStr
                                + "],"
                                + "    \"index\": 0"
                                + "  }],"
                                + "  \"model\": \"text-embedding-ada-002\","
                                + "  \"usage\": {"
                                + "    \"prompt_tokens\": 4,"
                                + "    \"total_tokens\": 4"
                                + "  }"
                                + "}";

                return new MockResponse()
                        .setHeader("Content-Type", "application/json")
                        .setBody(responseBody);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private MockResponse dispatchChat() {
            String responseBody =
                    "{"
                            + "  \"id\": \"chatcmpl-x\","
                            + "  \"object\": \"chat.completion\","
                            + "  \"created\": 0,"
                            + "  \"model\": \"gpt-3.5-turbo\","
                            + "  \"choices\": [{"
                            + "    \"index\": 0,"
                            + "    \"message\": {"
                            + "      \"role\": \"assistant\","
                            + "      \"content\": \"stubbed-completion\""
                            + "    },"
                            + "    \"finish_reason\": \"stop\""
                            + "  }],"
                            + "  \"usage\": {"
                            + "    \"prompt_tokens\": 1,"
                            + "    \"completion_tokens\": 1,"
                            + "    \"total_tokens\": 2"
                            + "  }"
                            + "}";
            return new MockResponse()
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
        }
    }
}
