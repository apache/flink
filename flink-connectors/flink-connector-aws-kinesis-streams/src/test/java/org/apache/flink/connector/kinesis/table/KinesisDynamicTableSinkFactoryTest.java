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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_FAIL_ON_ERROR;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;

/** Test for {@link KinesisDynamicSink} created by {@link KinesisDynamicTableSinkFactory}. */
public class KinesisDynamicTableSinkFactoryTest extends TestLogger {
    private static final String STREAM_NAME = "myStream";

    @Test
    public void testGoodTableSinkForPartitionedTable() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = defaultTableOptions().build();
        List<String> sinkPartitionKeys = Arrays.asList("name", "curr_id");

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisDynamicSink actualSink =
                (KinesisDynamicSink) createTableSink(sinkSchema, sinkPartitionKeys, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSink expectedSink =
                (KinesisDynamicSink)
                        new KinesisDynamicSink.KinesisDynamicTableSinkBuilder()
                                .setConsumedDataType(physicalDataType)
                                .setStream(STREAM_NAME)
                                .setKinesisClientProperties(defaultProducerProperties())
                                .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                                .setPartitioner(
                                        new RowDataFieldsKinesisPartitionKeyGenerator(
                                                (RowType) physicalDataType.getLogicalType(),
                                                sinkPartitionKeys))
                                .build();
        // verify that the constructed DynamicTableSink is as expected
        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(KinesisStreamsSink.class);
    }

    @Test
    public void testGoodTableSinkCopyForPartitionedTable() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = defaultTableOptions().build();
        List<String> sinkPartitionKeys = Arrays.asList("name", "curr_id");

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisDynamicSink actualSink =
                (KinesisDynamicSink) createTableSink(sinkSchema, sinkPartitionKeys, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSink expectedSink =
                (KinesisDynamicSink)
                        new KinesisDynamicSink.KinesisDynamicTableSinkBuilder()
                                .setConsumedDataType(physicalDataType)
                                .setStream(STREAM_NAME)
                                .setKinesisClientProperties(defaultProducerProperties())
                                .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                                .setPartitioner(
                                        new RowDataFieldsKinesisPartitionKeyGenerator(
                                                (RowType) physicalDataType.getLogicalType(),
                                                sinkPartitionKeys))
                                .build();
        Assertions.assertThat(actualSink).isEqualTo(expectedSink.copy());
        Assertions.assertThat(expectedSink).isNotSameAs(expectedSink.copy());
    }

    @Test
    public void testGoodTableSinkForNonPartitionedTable() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptions().build();

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisDynamicSink actualSink =
                (KinesisDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSink expectedSink =
                (KinesisDynamicSink)
                        new KinesisDynamicSink.KinesisDynamicTableSinkBuilder()
                                .setConsumedDataType(sinkSchema.toPhysicalRowDataType())
                                .setStream(STREAM_NAME)
                                .setKinesisClientProperties(defaultProducerProperties())
                                .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                                .setPartitioner(new RandomKinesisPartitionKeyGenerator<>())
                                .build();

        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(KinesisStreamsSink.class);
    }

    @Test
    public void testGoodTableSinkForNonPartitionedTableWithSinkOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptionsWithSinkOptions().build();

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisDynamicSink actualSink =
                (KinesisDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSink expectedSink =
                (KinesisDynamicSink)
                        getDefaultSinkBuilder()
                                .setConsumedDataType(sinkSchema.toPhysicalRowDataType())
                                .setStream(STREAM_NAME)
                                .setKinesisClientProperties(defaultProducerProperties())
                                .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                                .setPartitioner(new RandomKinesisPartitionKeyGenerator<>())
                                .build();

        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(KinesisStreamsSink.class);
    }

    @Test
    public void testGoodTableSinkForNonPartitionedTableWithProducerOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultTableOptionsWithDeprecatedOptions().build();

        // Construct actual DynamicTableSink using FactoryUtil
        KinesisDynamicSink actualSink =
                (KinesisDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        KinesisDynamicSink expectedSink =
                (KinesisDynamicSink)
                        new KinesisDynamicSink.KinesisDynamicTableSinkBuilder()
                                .setFailOnError(true)
                                .setMaxBatchSize(100)
                                .setMaxInFlightRequests(100)
                                .setMaxTimeInBufferMS(1000)
                                .setConsumedDataType(sinkSchema.toPhysicalRowDataType())
                                .setStream(STREAM_NAME)
                                .setKinesisClientProperties(defaultProducerProperties())
                                .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                                .setPartitioner(new RandomKinesisPartitionKeyGenerator<>())
                                .build();

        // verify that the constructed DynamicTableSink is as expected
        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(KinesisStreamsSink.class);
    }

    @Test
    public void testBadTableSinkForCustomPartitionerForPartitionedTable() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption(KinesisConnectorOptions.SINK_PARTITIONER, "random")
                        .build();

        Assertions.assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(
                        () ->
                                createTableSink(
                                        sinkSchema, Arrays.asList("name", "curr_id"), sinkOptions))
                .havingCause()
                .withMessageContaining(
                        String.format(
                                "Cannot set %s option for a table defined with a PARTITIONED BY clause",
                                KinesisConnectorOptions.SINK_PARTITIONER.key()));
    }

    @Test
    public void testBadTableSinkForNonExistingPartitionerClass() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption(KinesisConnectorOptions.SINK_PARTITIONER, "abc")
                        .build();

        Assertions.assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageContaining("Could not find and instantiate partitioner class 'abc'");
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical("name", DataTypes.STRING()),
                Column.physical("curr_id", DataTypes.BIGINT()),
                Column.physical("time", DataTypes.TIMESTAMP(3)));
    }

    private TableOptionsBuilder defaultTableOptionsWithSinkOptions() {
        return defaultTableOptions()
                .withTableOption(SINK_FAIL_ON_ERROR.key(), "true")
                .withTableOption(MAX_BATCH_SIZE.key(), "100")
                .withTableOption(MAX_IN_FLIGHT_REQUESTS.key(), "100")
                .withTableOption(MAX_BUFFERED_REQUESTS.key(), "100")
                .withTableOption(FLUSH_BUFFER_SIZE.key(), "1000")
                .withTableOption(FLUSH_BUFFER_TIMEOUT.key(), "1000");
    }

    private TableOptionsBuilder defaultTableOptionsWithDeprecatedOptions() {
        return defaultTableOptions()
                .withTableOption("sink.producer.record-max-buffered-time", "1000")
                .withTableOption("sink.producer.collection-max-size", "100")
                .withTableOption("sink.producer.collection-max-count", "100")
                .withTableOption("sink.producer.fail-on-error", "true");
    }

    private TableOptionsBuilder defaultTableOptions() {
        String connector = KinesisDynamicTableSinkFactory.IDENTIFIER;
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption(KinesisConnectorOptions.STREAM, STREAM_NAME)
                .withTableOption("aws.region", "us-west-2")
                .withTableOption("aws.credentials.provider", "BASIC")
                .withTableOption("aws.credentials.basic.accesskeyid", "ververicka")
                .withTableOption(
                        "aws.credentials.basic.secretkey",
                        "SuperSecretSecretSquirrel") // default format options
                .withFormatOption(TestFormatFactory.DELIMITER, ",")
                .withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
    }

    private KinesisDynamicSink.KinesisDynamicTableSinkBuilder getDefaultSinkBuilder() {
        return new KinesisDynamicSink.KinesisDynamicTableSinkBuilder()
                .setFailOnError(true)
                .setMaxBatchSize(100)
                .setMaxInFlightRequests(100)
                .setMaxBufferSizeInBytes(1000)
                .setMaxBufferedRequests(100)
                .setMaxTimeInBufferMS(1000);
    }

    private Properties defaultProducerProperties() {
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
