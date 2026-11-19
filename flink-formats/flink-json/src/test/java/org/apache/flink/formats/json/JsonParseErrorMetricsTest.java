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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.formats.json;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests metrics for ignored JSON parsing and conversion errors. */
class JsonParseErrorMetricsTest {

    private static final String METRIC_NAME = "numMessagesWithParseErrors";

    private static final DataType ROW_TYPE = ROW(FIELD("id", INT()), FIELD("age", INT()));

    private final MetricListener metricListener = new MetricListener();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCountsAffectedMessages(boolean jsonParserEnabled) throws Exception {
        final DeserializationSchema<RowData> schema = createAndOpenSchema(jsonParserEnabled, true);
        final Counter counter = metricListener.getCounter("json", METRIC_NAME).orElseThrow();

        assertThat(deserialize(schema, "{\"id\":1,\"age\":2}")).hasSize(1);
        assertThat(counter.getCount()).isZero();
        final List<RowData> invalidFields = deserialize(schema, "{\"id\":\"bad\",\"age\":\"bad\"}");
        assertThat(invalidFields).hasSize(1);
        assertThat(invalidFields.get(0).isNullAt(0)).isTrue();
        assertThat(invalidFields.get(0).isNullAt(1)).isTrue();
        assertThat(counter.getCount()).isEqualTo(1);

        assertThat(deserialize(schema, "not valid json")).isEmpty();
        assertThat(counter.getCount()).isEqualTo(2);
        assertThat(deserialize(schema, "{\"id\":3}").get(0).getInt(0)).isEqualTo(3);
        assertThat(counter.getCount()).isEqualTo(2);

        deserialize(schema, "[{\"id\":\"bad\"},{\"age\":\"bad\"},{\"id\":4}]");
        assertThat(counter.getCount()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAcceptedNullsAndEmptyInputs(boolean jsonParserEnabled) throws Exception {
        final DeserializationSchema<RowData> schema = createAndOpenSchema(jsonParserEnabled, true);
        final Counter counter = metricListener.getCounter("json", METRIC_NAME).orElseThrow();
        final List<RowData> output = new ArrayList<>();
        schema.deserialize(null, new ListCollector<>(output));
        for (String message : new String[] {"", "[]", "{}", "{\"id\":null}"}) {
            deserialize(schema, message);
        }
        assertThat(output).isEmpty();
        assertThat(counter.getCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStrictMode(boolean jsonParserEnabled) throws Exception {
        final DeserializationSchema<RowData> schema = createAndOpenSchema(jsonParserEnabled, false);
        assertThat(metricListener.getCounter("json", METRIC_NAME)).isEmpty();
        assertThatThrownBy(() -> deserialize(schema, "{\"id\":\"bad\"}"))
                .isInstanceOf(IOException.class);
    }

    private DeserializationSchema<RowData> createAndOpenSchema(
            boolean jsonParserEnabled, boolean ignoreErrors) throws Exception {
        final Configuration options = new Configuration();
        options.set(JsonFormatOptions.IGNORE_PARSE_ERRORS, ignoreErrors);
        options.set(JsonFormatOptions.DECODE_JSON_PARSER_ENABLED, jsonParserEnabled);
        final DeserializationSchema<RowData> schema =
                new JsonFormatFactory()
                        .createDecodingFormat(null, options)
                        .createRuntimeDecoder(ScanRuntimeProviderContext.INSTANCE, ROW_TYPE);
        schema.open(
                new DummyInitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return metricListener.getMetricGroup();
                    }
                });
        return schema;
    }

    private static List<RowData> deserialize(DeserializationSchema<RowData> schema, String message)
            throws IOException {
        final List<RowData> output = new ArrayList<>();
        schema.deserialize(message.getBytes(UTF_8), new ListCollector<>(output));
        return output;
    }
}
