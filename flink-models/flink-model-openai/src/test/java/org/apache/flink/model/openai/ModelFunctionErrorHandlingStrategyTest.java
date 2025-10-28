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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Model Function metrics and error handling strategy. */
@SuppressWarnings("unchecked")
public class ModelFunctionErrorHandlingStrategyTest {
    private static final String RETRYABLE_INPUT_DATA = "Return a retryable error code.";

    private static final Row[] INPUT_DATA =
            new Row[] {
                Row.of("Why is sky blue?", 1.0),
                Row.of("Why are stars shining?", 1.0),
                Row.of("What is the meaning of life?", 1.0),
                Row.of("What is a heart attack?", 0.0),
                Row.of("How fast can human run?", 1.0),
                Row.of("Why is the ocean salty?", 1.0),
                Row.of("How do airplanes fly?", 1.0),
                Row.of("What causes earthquakes?", 1.0),
                Row.of("Why do leaves change color in autumn?", 0.0),
                Row.of("How does photosynthesis work?", 1.0)
            };

    private static final Schema INPUT_SCHEMA =
            Schema.newBuilder().column("input", DataTypes.STRING()).build();
    private static final Schema OUTPUT_SCHEMA =
            Schema.newBuilder().column("content", DataTypes.STRING()).build();

    private static MockWebServer server;

    private String modelName;

    private Map<String, String> modelOptions;

    private StreamTableEnvironment tEnv;

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
        modelName = "Model" + System.currentTimeMillis();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> source =
                env.fromData(INPUT_DATA)
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"input", "invalid_input"},
                                        Types.STRING,
                                        Types.DOUBLE));
        tEnv = StreamTableEnvironment.create(env);
        Table sourceTable =
                tEnv.fromDataStream(
                        source,
                        Schema.newBuilder()
                                .column("input", DataTypes.STRING())
                                .column("invalid_input", DataTypes.DOUBLE())
                                .build());
        tEnv.createTemporaryView("MyTable", sourceTable);

        modelOptions = new HashMap<>();
        modelOptions.put("provider", "openai");
        modelOptions.put("endpoint", server.url("/chat/completions").toString());
        modelOptions.put("api-key", "foobar");
        modelOptions.put("model", "qwen-turbo");

        TestDispatcher.INVOKE_COUNT.set(0);
    }

    @AfterEach
    public void afterEach() {
        assertThat(OpenAIUtils.getCache()).isEmpty();
    }

    @Test
    public void testSuccessInvoke() {
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                modelName));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(INPUT_DATA.length);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1)).isNotEmpty();
        }
    }

    @Test
    public void testIgnoreError() {
        // The "t" in localhost is missing on purpose to test against invalid endpoint
        modelOptions.put("endpoint", "http://localhos:9999/chat/completions");
        modelOptions.put("error-handling-strategy", "ignore");
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                modelName));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).isEmpty();
    }

    @Test
    public void testIgnoreAndSurfaceError() {
        modelOptions.put("error-handling-strategy", "ignore");
        Schema outputSchemaWithErrorMessage =
                Schema.newBuilder()
                        .columnByMetadata("error-string", DataTypes.STRING())
                        .column("content", DataTypes.STRING())
                        .columnByMetadata("http-status-code", DataTypes.INT())
                        .columnByMetadata(
                                "http-headers-map",
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())))
                        .build();

        createModel(INPUT_SCHEMA, outputSchemaWithErrorMessage);
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "WITH v(input) AS (SELECT * FROM (VALUES ('%s'))) "
                                        + "SELECT * FROM ML_PREDICT( "
                                        + "  TABLE v, "
                                        + "  MODEL `%s`, "
                                        + "  DESCRIPTOR(`input`) "
                                        + ")",
                                RETRYABLE_INPUT_DATA, modelName));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getArity()).isEqualTo(5);
        assertThat((String) result.get(0).getFieldAs(0)).isEqualTo(RETRYABLE_INPUT_DATA);
        assertThat((String) result.get(0).getFieldAs(1))
                .isEqualTo("com.openai.errors.RateLimitException: 429: null");
        assertThat(result.get(0).getField(2)).isNull();
        assertThat((Integer) result.get(0).getFieldAs(3)).isEqualTo(429);
        assertThat((Map<String, String[]>) result.get(0).getFieldAs(4))
                .containsEntry("Content-Length", new String[] {"0"});
    }

    @Test
    public void testFailoverStrategy() {
        modelOptions.put("error-handling-strategy", "failover");
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "WITH v(input) AS (SELECT * FROM (VALUES ('%s'))) "
                                        + "SELECT * FROM ML_PREDICT( "
                                        + "  TABLE v, "
                                        + "  MODEL `%s`, "
                                        + "  DESCRIPTOR(`input`) "
                                        + ")",
                                RETRYABLE_INPUT_DATA, modelName));
        assertThatThrownBy(() -> IteratorUtils.toList(tableResult.collect()))
                .rootCause()
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "com.openai.errors.RateLimitException: 429"));
    }

    @Test
    public void testRetryWithFailoverStrategy() {
        modelOptions.put("retry-num", "3");
        modelOptions.put("error-handling-strategy", "retry");
        modelOptions.put("retry-fallback-strategy", "failover");
        createModel();
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "WITH v(input) AS (SELECT * FROM (VALUES ('%s'))) "
                                        + "SELECT * FROM ML_PREDICT( "
                                        + "  TABLE v, "
                                        + "  MODEL `%s`, "
                                        + "  DESCRIPTOR(`input`) "
                                        + ")",
                                RETRYABLE_INPUT_DATA, modelName));
        assertThatThrownBy(() -> IteratorUtils.toList(tableResult.collect()))
                .rootCause()
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                "com.openai.errors.RateLimitException: 429"));

        assertThat(TestDispatcher.INVOKE_COUNT.get()).isEqualTo(4); // retryNum + 1
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
                        modelName);
        catalogManager.createModel(
                CatalogModel.of(inputSchema, outputSchema, modelOptions, "This is a new model."),
                modelIdentifier,
                false);
    }

    private static class TestDispatcher extends Dispatcher {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static final AtomicInteger INVOKE_COUNT = new AtomicInteger(0);

        @Override
        public MockResponse dispatch(RecordedRequest request) {
            INVOKE_COUNT.incrementAndGet();

            String path = request.getRequestUrl().encodedPath();
            String body = request.getBody().readUtf8();

            if (!path.endsWith("/chat/completions")) {
                return new MockResponse().setResponseCode(404);
            }

            try {
                JsonNode root = OBJECT_MAPPER.readTree(body);

                // The messages contain one system prompt and one user message.
                Preconditions.checkArgument(root.get("messages").size() == 2);
                if (root.get("messages")
                        .get(1)
                        .get("content")
                        .toString()
                        .contains(RETRYABLE_INPUT_DATA)) {
                    return new MockResponse().setResponseCode(429);
                }

                String responseBody =
                        "{"
                                + "  \"id\": \"chatcmpl-1234567890ABCD\","
                                + "  \"object\": \"chat.completion\","
                                + "  \"created\": 1717029203,"
                                + "  \"model\": \"gpt-3.5-turbo-0125\","
                                + "  \"choices\": [{"
                                + "    \"index\": 0,"
                                + "    \"message\": {"
                                + "      \"role\": \"assistant\","
                                + "      \"content\": \"This is a mocked response\""
                                + "    },"
                                + "    \"finish_reason\": \"stop\""
                                + "  }],"
                                + "  \"usage\": {"
                                + "    \"prompt_tokens\": 9,"
                                + "    \"completion_tokens\": 16,"
                                + "    \"total_tokens\": 25"
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
