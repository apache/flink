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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link OpenAIEmbeddingModelFunction}. */
public class OpenAIEmbeddingModelTest {

    private static final String MODEL_NAME = "m";

    private static final Schema INPUT_SCHEMA =
            Schema.newBuilder().column("input", DataTypes.STRING()).build();
    private static final Schema OUTPUT_SCHEMA =
            Schema.newBuilder().column("embedding", DataTypes.ARRAY(DataTypes.FLOAT())).build();

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
        modelOptions.put("endpoint", server.url("/embeddings").toString());
        modelOptions.put("model", "text-embedding-v3");
        modelOptions.put("api-key", "foobar");
    }

    @AfterEach
    public void afterEach() {
        assertThat(OpenAIUtils.getCache()).isEmpty();
    }

    @Test
    public void testEmbedding() {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        catalogManager.createModel(
                CatalogModel.of(INPUT_SCHEMA, OUTPUT_SCHEMA, modelOptions, "This is a new model."),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME),
                false);

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, embedding FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`)))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat((Float[]) row.getFieldAs(1)).hasSize(512);
        }
    }

    @Test
    public void testEmbeddingWithSqlStatement() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "CREATE MODEL %s\n"
                                        + "INPUT (`input` STRING)\n"
                                        + "OUTPUT (`embedding` ARRAY<FLOAT>) \n"
                                        + "WITH (%s)",
                                MODEL_NAME,
                                modelOptions.entrySet().stream()
                                        .map(
                                                x ->
                                                        String.format(
                                                                "'%s'='%s'",
                                                                x.getKey(), x.getValue()))
                                        .collect(Collectors.joining(","))))
                .await();

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, embedding FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`)))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat((Float[]) row.getFieldAs(1)).hasSize(512);
        }
    }

    @Test
    public void testEmbeddingWithDimension() {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        Map<String, String> modelOptions = new HashMap<>(this.modelOptions);
        modelOptions.put("dimension", "256");
        catalogManager.createModel(
                CatalogModel.of(INPUT_SCHEMA, OUTPUT_SCHEMA, modelOptions, "This is a new model."),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME),
                false);

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, embedding FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`)))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat((Float[]) row.getFieldAs(1)).hasSize(256);
        }
    }

    @Test
    public void testMaxContextSize() {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        Map<String, String> modelOptions = new HashMap<>(this.modelOptions);
        modelOptions.put("model", "text-embedding-3-small");
        modelOptions.put("max-context-size", "2");
        modelOptions.put("context-overflow-action", "skipped");
        catalogManager.createModel(
                CatalogModel.of(INPUT_SCHEMA, OUTPUT_SCHEMA, modelOptions, "This is a new model."),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME),
                false);

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, embedding FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`)))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).isEmpty();
    }

    @Test
    public void testNullValue() {
        tEnv.executeSql(
                "CREATE TABLE MyTableWithNull(input STRING, invalid_input DOUBLE) "
                        + "WITH ( 'connector' = 'datagen', 'number-of-rows' = '10', 'fields.input.null-rate' = '1')");

        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        catalogManager.createModel(
                CatalogModel.of(INPUT_SCHEMA, OUTPUT_SCHEMA, modelOptions, "This is a new model."),
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME),
                false);

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, embedding FROM TABLE(ML_PREDICT(TABLE MyTableWithNull, MODEL %s, DESCRIPTOR(`input`)))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        assertThat(result).isEmpty();
    }

    @Test
    public void testInvalidInputSchema() {
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        ObjectIdentifier modelIdentifier =
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME);

        Schema inputSchemaWithInvalidColumnType =
                Schema.newBuilder().column("input", DataTypes.DOUBLE()).build();

        catalogManager.createModel(
                CatalogModel.of(
                        inputSchemaWithInvalidColumnType,
                        OUTPUT_SCHEMA,
                        modelOptions,
                        "This is a new model."),
                modelIdentifier,
                false);
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
        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        ObjectIdentifier modelIdentifier =
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        MODEL_NAME);

        Schema outputSchemaWithInvalidColumnType =
                Schema.newBuilder().column("output", DataTypes.DOUBLE()).build();

        catalogManager.createModel(
                CatalogModel.of(
                        INPUT_SCHEMA,
                        outputSchemaWithInvalidColumnType,
                        modelOptions,
                        "This is a new model."),
                modelIdentifier,
                false);
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format(
                                                "SELECT * FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`)))",
                                                MODEL_NAME)))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("output", "DOUBLE", "ARRAY<FLOAT>");
    }

    private static class TestDispatcher extends Dispatcher {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public MockResponse dispatch(RecordedRequest request) {
            assert request.getRequestUrl() != null;
            String path = request.getRequestUrl().encodedPath();

            String body = request.getBody().readUtf8();

            if (!path.endsWith("/embeddings")) {
                return new MockResponse().setResponseCode(404);
            }

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
    }
}
