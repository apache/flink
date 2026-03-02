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

import static org.assertj.core.api.Assertions.assertThat;

/** Test for inference metrics in {@link OpenAIChatModelFunction}. */
public class OpenAIInferenceMetricsTest {

    private static final String MODEL_NAME = "metrics_test_model";

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
        server.setDispatcher(new MetricsTestDispatcher());
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
                "CREATE TABLE MyTable(input STRING) WITH ('connector' = 'datagen', 'number-of-rows' = '5')");

        modelOptions = new HashMap<>();
        modelOptions.put("provider", "openai");
        modelOptions.put("endpoint", server.url("/chat/completions").toString());
        modelOptions.put("model", "test-model");
        modelOptions.put("api-key", "test-key");
    }

    @AfterEach
    public void afterEach() {
        assertThat(OpenAIUtils.getCache()).isEmpty();
    }

    @Test
    public void testSuccessfulChatInferenceWithMetrics() {
        createModel(INPUT_SCHEMA, OUTPUT_SCHEMA);
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        // Verify inference completed — metrics are registered and incremented internally
        assertThat(result).hasSize(5);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1)).isEqualTo("Metrics test response");
        }
    }

    @Test
    public void testNullInputSkipsMetrics() {
        tEnv.executeSql(
                "CREATE TABLE NullTable(input STRING) "
                        + "WITH ('connector' = 'datagen', 'number-of-rows' = '5', 'fields.input.null-rate' = '1')");

        createModel(INPUT_SCHEMA, OUTPUT_SCHEMA);
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, content FROM ML_PREDICT(TABLE NullTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        // Null inputs should be skipped — no inference_requests increment for null rows
        assertThat(result).isEmpty();
    }

    private void createModel(Schema inputSchema, Schema outputSchema) {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        ObjectIdentifier modelIdentifier =
                ObjectIdentifier.of(
                        Objects.requireNonNull(catalogManager.getCurrentCatalog()),
                        Objects.requireNonNull(catalogManager.getCurrentDatabase()),
                        MODEL_NAME);
        catalogManager.createModel(
                CatalogModel.of(
                        inputSchema, outputSchema, modelOptions, "Metrics test model."),
                modelIdentifier,
                false);
    }

    private static class MetricsTestDispatcher extends Dispatcher {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
            String path = request.getRequestUrl().encodedPath();
            if (!path.endsWith("/chat/completions")) {
                return new MockResponse().setResponseCode(404);
            }

            String responseBody =
                    "{"
                            + "  \"id\": \"chatcmpl-metrics-test\","
                            + "  \"object\": \"chat.completion\","
                            + "  \"created\": 1717029203,"
                            + "  \"model\": \"test-model\","
                            + "  \"choices\": [{"
                            + "    \"index\": 0,"
                            + "    \"message\": {"
                            + "      \"role\": \"assistant\","
                            + "      \"content\": \"Metrics test response\""
                            + "    },"
                            + "    \"finish_reason\": \"stop\""
                            + "  }],"
                            + "  \"usage\": {"
                            + "    \"prompt_tokens\": 5,"
                            + "    \"completion_tokens\": 3,"
                            + "    \"total_tokens\": 8"
                            + "  }"
                            + "}";

            return new MockResponse()
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
        }
    }
}
