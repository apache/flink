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

package org.apache.flink.model.triton;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.FunctionContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that tensor names in Triton inference requests preserve the original column name casing
 * (i.e., are not unconditionally uppercased).
 *
 * <p>Triton's KServe V2 protocol treats tensor names as case-sensitive. A model configured with
 * {@code input} and {@code output} tensors in its {@code config.pbtxt} will reject requests that
 * send {@code INPUT} and {@code OUTPUT}. This test verifies the fix for that bug.
 */
class TritonTensorNameCasingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MockWebServer mockServer;

    @BeforeEach
    void setUp() throws IOException {
        mockServer = new MockWebServer();
        mockServer.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        mockServer.shutdown();
    }

    private String baseUrl() {
        return mockServer.url("/").toString().replaceAll("/$", "");
    }

    /**
     * Verifies that lowercase column names declared in the model schema are sent to Triton as-is,
     * not converted to uppercase.
     *
     * <p>Before the fix, {@code buildInferenceRequest} called {@code inputName.toUpperCase()} and
     * {@code outputName.toUpperCase()}, causing HTTP 400 responses from any model whose {@code
     * config.pbtxt} declared lowercase tensor names.
     */
    @Test
    void testLowercaseTensorNamesArePreserved() throws Exception {
        String tritonResponse =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"FP32\","
                        + "\"shape\":[3],\"data\":[0.1,0.2,0.3]}]}";
        mockServer.enqueue(
                new MockResponse()
                        .setResponseCode(200)
                        .setBody(tritonResponse)
                        .setHeader("Content-Type", "application/json"));

        TritonInferenceModelFunction function = buildFunction("input", "output");
        function.open(new FunctionContext(null, null, null));
        try {
            GenericRowData row = GenericRowData.of(BinaryStringData.fromString("hello world"));
            function.asyncPredict(row).get(5, TimeUnit.SECONDS);
        } finally {
            function.close();
        }

        RecordedRequest captured = mockServer.takeRequest(5, TimeUnit.SECONDS);
        assertThat(captured).isNotNull();

        JsonNode body = MAPPER.readTree(captured.getBody().readUtf8());
        assertThat(body.get("inputs").get(0).get("name").asText())
                .as("input tensor name must not be uppercased")
                .isEqualTo("input");
        assertThat(body.get("outputs").get(0).get("name").asText())
                .as("output tensor name must not be uppercased")
                .isEqualTo("output");
    }

    /**
     * Verifies that mixed-case column names are also sent without modification.
     *
     * <p>The fix removes the unconditional {@code toUpperCase()} call, so any casing — not just
     * all-lowercase — must now round-trip correctly.
     */
    @Test
    void testMixedCaseTensorNamesArePreserved() throws Exception {
        String tritonResponse =
                "{\"outputs\":[{\"name\":\"myOutput\",\"datatype\":\"FP32\","
                        + "\"shape\":[1],\"data\":[0.5]}]}";
        mockServer.enqueue(
                new MockResponse()
                        .setResponseCode(200)
                        .setBody(tritonResponse)
                        .setHeader("Content-Type", "application/json"));

        TritonInferenceModelFunction function = buildFunction("myInput", "myOutput");
        function.open(new FunctionContext(null, null, null));
        try {
            GenericRowData row = GenericRowData.of(BinaryStringData.fromString("test"));
            function.asyncPredict(row).get(5, TimeUnit.SECONDS);
        } finally {
            function.close();
        }

        RecordedRequest captured = mockServer.takeRequest(5, TimeUnit.SECONDS);
        assertThat(captured).isNotNull();

        JsonNode body = MAPPER.readTree(captured.getBody().readUtf8());
        assertThat(body.get("inputs").get(0).get("name").asText())
                .as("mixed-case input tensor name must not be uppercased")
                .isEqualTo("myInput");
        assertThat(body.get("outputs").get(0).get("name").asText())
                .as("mixed-case output tensor name must not be uppercased")
                .isEqualTo("myOutput");
    }

    private TritonInferenceModelFunction buildFunction(String inputColName, String outputColName) {
        Configuration config = new Configuration();
        config.setString(TritonOptions.ENDPOINT.key(), baseUrl());
        config.setString(TritonOptions.MODEL_NAME.key(), "test-model");
        config.setString(TritonOptions.MODEL_VERSION.key(), "1");

        CatalogModel origin =
                CatalogModel.of(
                        Schema.newBuilder().column(inputColName, "STRING").build(),
                        Schema.newBuilder().column(outputColName, "ARRAY<FLOAT>").build(),
                        Collections.emptyMap(),
                        null);

        ResolvedSchema inputSchema =
                ResolvedSchema.of(Column.physical(inputColName, DataTypes.STRING()));
        ResolvedSchema outputSchema =
                ResolvedSchema.of(
                        Column.physical(outputColName, DataTypes.ARRAY(DataTypes.FLOAT())));

        ResolvedCatalogModel resolvedModel =
                ResolvedCatalogModel.of(origin, inputSchema, outputSchema);

        ModelProviderFactory.Context factoryContext =
                new ModelProviderFactory.Context() {
                    @Override
                    public ObjectIdentifier getObjectIdentifier() {
                        return ObjectIdentifier.of("default", "default", "test_model");
                    }

                    @Override
                    public ResolvedCatalogModel getCatalogModel() {
                        return resolvedModel;
                    }

                    @Override
                    public ReadableConfig getConfiguration() {
                        return config;
                    }

                    @Override
                    public ClassLoader getClassLoader() {
                        return Thread.currentThread().getContextClassLoader();
                    }

                    @Override
                    public boolean isTemporary() {
                        return true;
                    }
                };

        return new TritonInferenceModelFunction(factoryContext, config);
    }
}
