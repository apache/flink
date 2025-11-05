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
import org.apache.flink.types.variant.Variant;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link OpenAIChatModelFunction}. */
public class OpenAIChatModelTest {
    private static final String MODEL_NAME = "m";

    private static final Schema INPUT_SCHEMA =
            Schema.newBuilder().column("input", DataTypes.STRING()).build();
    private static final Schema OUTPUT_SCHEMA =
            Schema.newBuilder().column("content", DataTypes.STRING()).build();

    private static MockWebServer server;

    private Map<String, String> modelOptions;

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
                "CREATE TABLE MyTable(input STRING, invalid_input DOUBLE) WITH ( 'connector' = 'datagen', 'number-of-rows' = '10')");

        modelOptions = new HashMap<>();
        modelOptions.put("provider", "openai");
        modelOptions.put("endpoint", server.url("/chat/completions").toString());
        modelOptions.put("model", "qwen-turbo");
        modelOptions.put("api-key", "foobar");
    }

    @AfterEach
    public void afterEach() {
        assertThat(OpenAIUtils.getCache()).isEmpty();
    }

    @Test
    public void testChat() {
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1))
                    .isEqualTo(
                            "This is a mocked response continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation");
        }
    }

    @Test
    public void testMaxToken() {
        int maxTokens = 20;
        modelOptions.put("max-tokens", Integer.toString(maxTokens));
        createModel();

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1))
                    .isEqualTo(
                            "This is a mocked response continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation");
            assertThat(((String) row.getFieldAs(1)).split(" ")).hasSizeLessThan(maxTokens);
        }
    }

    @Test
    public void testStop() {
        String stop = "a,the";
        modelOptions.put("stop", stop);
        createModel();

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1)).isEqualTo("This is ");
            assertThat(((String) row.getFieldAs(1)).split(" "))
                    .doesNotContain("a")
                    .doesNotContain("the");
        }
    }

    @Test
    public void testMaxContextSize() {
        modelOptions.put("model", "gpt-4");
        modelOptions.put("max-context-size", "2");
        modelOptions.put("context-overflow-action", "skipped");
        createModel();

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).isEmpty();
    }

    @Test
    public void testN() {
        modelOptions.put("n", "2");
        modelOptions.put("model", "qwen-plus");
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(20);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1))
                    .isEqualTo(
                            "This is a mocked response continuation continuation continuation continuation continuation continuation continuation continuation continuation continuation");
        }
    }

    @Test
    public void testResponseFormat() {
        modelOptions.put("response-format", "json_object");
        modelOptions.put(
                "system-prompt",
                "You are a helpful assistant. Please output your response in json format.");
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content, TRY_PARSE_JSON(content) as content_json FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            String content = row.getFieldAs(1);
            assertThat(content).isNotEmpty();
            assertThat(row.getField(2))
                    .withFailMessage("%s is not a valid json object", content)
                    .isInstanceOf(Variant.class);
            assertThat((Variant) row.getFieldAs(2))
                    .withFailMessage("%s is not a valid json object", content)
                    .isNotNull();
        }
    }

    @Test
    public void testNullValue() {
        tEnv.executeSql(
                "CREATE TABLE MyTableWithNull(input STRING, invalid_input DOUBLE) "
                        + "WITH ( 'connector' = 'datagen', 'number-of-rows' = '10', 'fields.input.null-rate' = '1')");

        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTableWithNull, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).isEmpty();
    }

    @Test
    public void testInvalidInputSchema() {
        Schema inputSchemaWithInvalidColumnType =
                Schema.newBuilder().column("input", DataTypes.DOUBLE()).build();

        createModel(inputSchemaWithInvalidColumnType, OUTPUT_SCHEMA);
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format(
                                                "SELECT * FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`invalid_input`)))",
                                                MODEL_NAME)))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("input", "DOUBLE", "STRING");
    }

    @Test
    public void testInvalidOutputSchema() {
        Schema outputSchemaWithInvalidColumnType =
                Schema.newBuilder().column("output", DataTypes.DOUBLE()).build();

        createModel(INPUT_SCHEMA, outputSchemaWithInvalidColumnType);
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format(
                                                "SELECT * FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`)))",
                                                MODEL_NAME)))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("output", "DOUBLE", "STRING");
    }

    private void createModel() {
        createModel(INPUT_SCHEMA, OUTPUT_SCHEMA);
    }

    private void createModel(Schema inputSchema, Schema outputSchema) {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        ObjectIdentifier modelIdentifier =
                ObjectIdentifier.of(
                        Objects.requireNonNull(catalogManager.getCurrentCatalog()),
                        Objects.requireNonNull(catalogManager.getCurrentDatabase()),
                        MODEL_NAME);
        catalogManager.createModel(
                CatalogModel.of(inputSchema, outputSchema, modelOptions, "This is a new model."),
                modelIdentifier,
                false);
    }

    private static class TestDispatcher extends Dispatcher {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public MockResponse dispatch(RecordedRequest request) {
            String path = request.getRequestUrl().encodedPath();

            String body = request.getBody().readUtf8();

            if (!path.endsWith("/chat/completions")) {
                return new MockResponse().setResponseCode(404);
            }

            try {
                JsonNode root = OBJECT_MAPPER.readTree(body);
                int maxTokens = root.has("max_tokens") ? root.get("max_tokens").asInt() : 16;
                List<String> stop = new ArrayList<>();
                if (root.has("stop")) {
                    root.get("stop").forEach(node -> stop.add(node.asText()));
                }

                int n = 1;
                if (root.has("n")) {
                    n = root.get("n").intValue();
                }

                StringBuilder contentBuilder = new StringBuilder("This is a mocked response");
                contentBuilder.append(" continuation".repeat(Math.max(0, maxTokens - 6)));
                for (String stopWord : stop) {
                    if (contentBuilder.toString().contains(stopWord)) {
                        int stopIndex = contentBuilder.indexOf(stopWord);
                        if (stopIndex > 0) {
                            contentBuilder.delete(stopIndex, contentBuilder.length());
                        }
                    }
                }

                String content = contentBuilder.toString();
                if (root.has("response_format")
                        && "\"json_object\""
                                .equalsIgnoreCase(
                                        root.get("response_format").get("type").toString())) {
                    content = "{\\\"content\\\": \\\"" + content + "\\\"}";
                }

                List<String> choices = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    choices.add(
                            "{"
                                    + "    \"index\": 0,"
                                    + "    \"message\": {"
                                    + "      \"role\": \"assistant\","
                                    + "      \"content\": \""
                                    + content
                                    + "\""
                                    + "    },"
                                    + "    \"finish_reason\": \"stop\""
                                    + "  }");
                }

                String responseBody =
                        "{"
                                + "  \"id\": \"chatcmpl-1234567890ABCD\","
                                + "  \"object\": \"chat.completion\","
                                + "  \"created\": 1717029203,"
                                + "  \"model\": \"gpt-3.5-turbo-0125\","
                                + "  \"choices\": "
                                + choices
                                + ","
                                + "  \"usage\": {"
                                + "    \"prompt_tokens\": 9,"
                                + "    \"completion_tokens\": "
                                + Math.min(maxTokens, 100)
                                + ","
                                + "    \"total_tokens\": "
                                + (9 + Math.min(maxTokens, 100))
                                + "  }"
                                + "}";

                return new MockResponse()
                        .setHeader("Content-Type", "application/json")
                        .setBody(responseBody);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
