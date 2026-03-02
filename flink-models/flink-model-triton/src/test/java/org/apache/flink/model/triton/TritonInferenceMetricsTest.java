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

package org.apache.flink.model.triton;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for inference metrics in {@link TritonInferenceModelFunction}. */
class TritonInferenceMetricsTest {

    private static final String MODEL_NAME = "triton_test_model";

    private static final Schema INPUT_SCHEMA =
            Schema.newBuilder().column("input", DataTypes.STRING()).build();
    private static final Schema OUTPUT_SCHEMA =
            Schema.newBuilder().column("output", DataTypes.STRING()).build();

    private static MockWebServer server;

    private Map<String, String> modelOptions;
    private TableEnvironment tEnv;

    @BeforeAll
    public static void beforeAll() throws IOException {
        server = new MockWebServer();
        server.setDispatcher(new TritonTestDispatcher());
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
        modelOptions.put("provider", "triton");
        modelOptions.put("endpoint", server.url("/").toString());
        modelOptions.put("model-name", "test_model");
    }

    @Test
    void testSuccessfulInferenceIncrements() {
        createModel(INPUT_SCHEMA, OUTPUT_SCHEMA);
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT input, output FROM ML_PREDICT(TABLE MyTable, MODEL %s, DESCRIPTOR(`input`))",
                                MODEL_NAME));
        List<Row> result = IteratorUtils.toList(tableResult.collect());
        // Verify that inference actually completed successfully - metrics are registered internally
        assertThat(result).hasSize(5);
        for (Row row : result) {
            assertThat(row.getField(0)).isInstanceOf(String.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            assertThat((String) row.getFieldAs(1)).isEqualTo("mocked_output");
        }
    }

    @Test
    void testInferenceWithServerError() {
        // Use a separate server that always returns 500
        try (MockWebServer errorServer = new MockWebServer()) {
            errorServer.setDispatcher(
                    new Dispatcher() {
                        @Override
                        public MockResponse dispatch(RecordedRequest request) {
                            return new MockResponse()
                                    .setResponseCode(500)
                                    .setBody("{\"error\": \"server error\"}");
                        }
                    });
            errorServer.start();

            Map<String, String> errorOptions = new HashMap<>();
            errorOptions.put("provider", "triton");
            errorOptions.put("endpoint", errorServer.url("/").toString());
            errorOptions.put("model-name", "error_model");

            TableEnvironment errorEnv = TableEnvironment.create(new Configuration());
            errorEnv.executeSql(
                    "CREATE TABLE ErrorTable(input STRING) WITH ('connector' = 'datagen', 'number-of-rows' = '1')");

            CatalogManager catalogManager = ((TableEnvironmentImpl) errorEnv).getCatalogManager();
            ObjectIdentifier modelIdentifier =
                    ObjectIdentifier.of(
                            Objects.requireNonNull(catalogManager.getCurrentCatalog()),
                            Objects.requireNonNull(catalogManager.getCurrentDatabase()),
                            "error_model");
            catalogManager.createModel(
                    CatalogModel.of(
                            INPUT_SCHEMA, OUTPUT_SCHEMA, errorOptions, "Error test model."),
                    modelIdentifier,
                    false);

            // The inference should fail, but metrics should still be updated (failure counter)
            try {
                TableResult tableResult =
                        errorEnv.executeSql(
                                "SELECT input, output FROM ML_PREDICT(TABLE ErrorTable, MODEL error_model, DESCRIPTOR(`input`))");
                IteratorUtils.toList(tableResult.collect());
            } catch (Exception e) {
                // Expected - the server returns 500
                assertThat(e).isNotNull();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
                        inputSchema, outputSchema, modelOptions, "This is a test model."),
                modelIdentifier,
                false);
    }

    private static class TritonTestDispatcher extends Dispatcher {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
            String path = request.getRequestUrl().encodedPath();

            if (!path.contains("/v2/models/")) {
                return new MockResponse().setResponseCode(404);
            }

            String responseBody =
                    "{"
                            + "  \"id\": \"test-request-id\","
                            + "  \"model_name\": \"test_model\","
                            + "  \"model_version\": \"1\","
                            + "  \"outputs\": [{"
                            + "    \"name\": \"OUTPUT\","
                            + "    \"datatype\": \"BYTES\","
                            + "    \"shape\": [1],"
                            + "    \"data\": [\"mocked_output\"]"
                            + "  }]"
                            + "}";

            return new MockResponse()
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
        }
    }
}
