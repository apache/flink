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

package org.apache.flink.connector.firehose.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;

/**
 * Test for {@link KinesisFirehoseDynamicSink} created by {@link
 * KinesisFirehoseDynamicTableFactory}.
 */
class KinesisFirehoseDynamicTableFactoryTest {
    private static final String DELIVERY_STREAM_NAME = "myDeliveryStream";

    @Test
    void testGoodTableSink() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptions().build();

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisFirehoseDynamicSink actualSink =
                (KinesisFirehoseDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisFirehoseDynamicSink expectedSink =
                new KinesisFirehoseDynamicSink.KinesisFirehoseDynamicSinkBuilder()
                        .setConsumedDataType(sinkSchema.toPhysicalRowDataType())
                        .setDeliveryStream(DELIVERY_STREAM_NAME)
                        .setFirehoseClientProperties(defaultSinkProperties())
                        .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                        .build();

        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(KinesisFirehoseSink.class);
    }

    @Test
    void testGoodTableSinkWithSinkOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptionsWithSinkOptions().build();

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisFirehoseDynamicSink actualSink =
                (KinesisFirehoseDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisFirehoseDynamicSink expectedSink =
                getDefaultSinkBuilder()
                        .setConsumedDataType(sinkSchema.toPhysicalRowDataType())
                        .setDeliveryStream(DELIVERY_STREAM_NAME)
                        .setFirehoseClientProperties(defaultSinkProperties())
                        .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                        .build();

        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(KinesisFirehoseSink.class);
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical("name", DataTypes.STRING()),
                Column.physical("curr_id", DataTypes.BIGINT()),
                Column.physical("time", DataTypes.TIMESTAMP(3)));
    }

    private TableOptionsBuilder defaultTableOptionsWithSinkOptions() {
        return defaultTableOptions()
                .withTableOption("sink.fail-on-error", "true")
                .withTableOption("sink.batch.max-size", "100")
                .withTableOption("sink.requests.max-inflight", "100")
                .withTableOption("sink.requests.max-buffered", "100")
                .withTableOption("sink.flush-buffer.size", "1000")
                .withTableOption("sink.flush-buffer.timeout", "1000");
    }

    private TableOptionsBuilder defaultTableOptions() {
        String connector = KinesisFirehoseDynamicTableFactory.IDENTIFIER;
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption(
                        KinesisFirehoseConnectorOptions.DELIVERY_STREAM, DELIVERY_STREAM_NAME)
                .withTableOption("aws.region", "us-west-2")
                .withTableOption("aws.credentials.provider", "BASIC")
                .withTableOption("aws.credentials.basic.accesskeyid", "ververicka")
                .withTableOption(
                        "aws.credentials.basic.secretkey",
                        "SuperSecretSecretSquirrel") // default format options
                .withFormatOption(TestFormatFactory.DELIMITER, ",")
                .withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
    }

    private KinesisFirehoseDynamicSink.KinesisFirehoseDynamicSinkBuilder getDefaultSinkBuilder() {
        return new KinesisFirehoseDynamicSink.KinesisFirehoseDynamicSinkBuilder()
                .setFailOnError(true)
                .setMaxBatchSize(100)
                .setMaxInFlightRequests(100)
                .setMaxBufferSizeInBytes(1000)
                .setMaxBufferedRequests(100)
                .setMaxTimeInBufferMS(1000);
    }

    private Properties defaultSinkProperties() {
        return new Properties() {
            {
                setProperty("aws.region", "us-west-2");
                setProperty("aws.credentials.provider", "BASIC");
                setProperty("aws.credentials.provider.basic.accesskeyid", "ververicka");
                setProperty(
                        "aws.credentials.provider.basic.secretkey", "SuperSecretSecretSquirrel");
            }
        };
    }
}
