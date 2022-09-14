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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSinkBuilder;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/** Kinesis backed {@link AsyncDynamicTableSink}. */
@Internal
public class KinesisDynamicSink extends AsyncDynamicTableSink<PutRecordsRequestEntry>
        implements SupportsPartitioning {

    /** Consumed data type of the table. */
    private final DataType consumedDataType;

    /** The Kinesis stream to write to. */
    private final String stream;

    /** Properties for the Kinesis DataStream Sink. */
    private final Properties kinesisClientProperties;

    /** Sink format for encoding records to Kinesis. */
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /** Partitioner to select Kinesis partition for each item. */
    private final PartitionKeyGenerator<RowData> partitioner;

    private final Boolean failOnError;

    public KinesisDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            @Nullable Boolean failOnError,
            @Nullable DataType consumedDataType,
            String stream,
            @Nullable Properties kinesisClientProperties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            PartitionKeyGenerator<RowData> partitioner) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.failOnError = failOnError;
        this.kinesisClientProperties = kinesisClientProperties;
        this.consumedDataType =
                Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
        this.stream = Preconditions.checkNotNull(stream, "Kinesis stream name must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
        this.partitioner =
                Preconditions.checkNotNull(
                        partitioner, "Kinesis partition key generator must not be null");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                encodingFormat.createRuntimeEncoder(context, consumedDataType);

        KinesisStreamsSinkBuilder<RowData> builder =
                KinesisStreamsSink.<RowData>builder()
                        .setSerializationSchema(serializationSchema)
                        .setPartitionKeyGenerator(partitioner)
                        .setKinesisClientProperties(kinesisClientProperties)
                        .setStreamName(stream);

        Optional.ofNullable(failOnError).ifPresent(builder::setFailOnError);
        addAsyncOptionsToSinkBuilder(builder);
        KinesisStreamsSink<RowData> kdsSink = builder.build();
        return SinkV2Provider.of(kdsSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new KinesisDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                failOnError,
                consumedDataType,
                stream,
                kinesisClientProperties,
                encodingFormat,
                partitioner);
    }

    @Override
    public String asSummaryString() {
        return "Kinesis";
    }

    // --------------------------------------------------------------------------------------------
    // SupportsPartitioning
    // --------------------------------------------------------------------------------------------

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        if (partitioner instanceof RowDataFieldsKinesisPartitionKeyGenerator) {
            ((RowDataFieldsKinesisPartitionKeyGenerator) partitioner).setStaticFields(partition);
        } else {
            String msg =
                    ""
                            + "Cannot apply static partition optimization to a partition class "
                            + "that does not inherit from "
                            + "org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisPartitioner.";
            throw new RuntimeException(msg);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Value semantics for equals and hashCode
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KinesisDynamicSink that = (KinesisDynamicSink) o;
        return super.equals(o)
                && Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(stream, that.stream)
                && Objects.equals(kinesisClientProperties, that.kinesisClientProperties)
                && Objects.equals(encodingFormat, that.encodingFormat)
                && Objects.equals(partitioner, that.partitioner)
                && Objects.equals(failOnError, that.failOnError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                consumedDataType,
                stream,
                kinesisClientProperties,
                encodingFormat,
                partitioner,
                failOnError);
    }

    /** Builder class for {@link KinesisDynamicSink}. */
    @Internal
    public static class KinesisDynamicTableSinkBuilder
            extends AsyncDynamicTableSinkBuilder<
                    PutRecordsRequestEntry, KinesisDynamicTableSinkBuilder> {

        private DataType consumedDataType = null;
        private String stream = null;
        private Properties kinesisClientProperties = null;
        private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;
        private PartitionKeyGenerator<RowData> partitioner = null;
        private Boolean failOnError = null;

        public KinesisDynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
            this.consumedDataType = consumedDataType;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setStream(String stream) {
            this.stream = stream;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setKinesisClientProperties(
                Properties kinesisClientProperties) {
            this.kinesisClientProperties = kinesisClientProperties;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setEncodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setFailOnError(Boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public KinesisDynamicTableSinkBuilder setPartitioner(
                PartitionKeyGenerator<RowData> partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        @Override
        public KinesisDynamicSink build() {
            return new KinesisDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    failOnError,
                    consumedDataType,
                    stream,
                    kinesisClientProperties,
                    encodingFormat,
                    partitioner);
        }
    }
}
