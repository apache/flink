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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.firehose.model.Record;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.Properties;

/** Kinesis firehose backed {@link AsyncDynamicTableSink}. */
@Internal
public class KinesisFirehoseDynamicSink extends AsyncDynamicTableSink<Record> {

    /** Consumed data type of the table. */
    private final DataType consumedDataType;

    /** The Firehose delivery stream to write to. */
    private final String deliveryStream;

    /** Properties for the Firehose DataStream Sink. */
    private final Properties firehoseClientProperties;

    /** Sink format for encoding records to Kinesis. */
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    private final Boolean failOnError;

    protected KinesisFirehoseDynamicSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            @Nullable Boolean failOnError,
            @Nullable DataType consumedDataType,
            String deliveryStream,
            @Nullable Properties firehoseClientProperties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        super(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
        this.failOnError = failOnError;
        this.firehoseClientProperties = firehoseClientProperties;
        this.consumedDataType =
                Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
        this.deliveryStream =
                Preconditions.checkNotNull(
                        deliveryStream, "Firehose Delivery stream name must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                encodingFormat.createRuntimeEncoder(context, consumedDataType);

        KinesisFirehoseSinkBuilder<RowData> builder =
                KinesisFirehoseSink.<RowData>builder()
                        .setSerializationSchema(serializationSchema)
                        .setFirehoseClientProperties(firehoseClientProperties)
                        .setDeliveryStreamName(deliveryStream);

        Optional.ofNullable(failOnError).ifPresent(builder::setFailOnError);
        super.addAsyncOptionsToSinkBuilder(builder);

        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new KinesisFirehoseDynamicSink(
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                failOnError,
                consumedDataType,
                deliveryStream,
                firehoseClientProperties,
                encodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "firehose";
    }

    /** Builder class for {@link KinesisFirehoseDynamicSink}. */
    @Internal
    public static class KinesisFirehoseDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<Record, KinesisFirehoseDynamicSinkBuilder> {

        private DataType consumedDataType = null;
        private String deliveryStream = null;
        private Properties firehoseClientProperties = null;
        private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;
        private Boolean failOnError = null;

        public KinesisFirehoseDynamicSinkBuilder setConsumedDataType(DataType consumedDataType) {
            this.consumedDataType = consumedDataType;
            return this;
        }

        public KinesisFirehoseDynamicSinkBuilder setDeliveryStream(String deliveryStream) {
            this.deliveryStream = deliveryStream;
            return this;
        }

        public KinesisFirehoseDynamicSinkBuilder setFirehoseClientProperties(
                Properties firehoseClientProperties) {
            this.firehoseClientProperties = firehoseClientProperties;
            return this;
        }

        public KinesisFirehoseDynamicSinkBuilder setEncodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        public KinesisFirehoseDynamicSinkBuilder setFailOnError(Boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        @Override
        public KinesisFirehoseDynamicSink build() {
            return new KinesisFirehoseDynamicSink(
                    getMaxBatchSize(),
                    getMaxInFlightRequests(),
                    getMaxBufferedRequests(),
                    getMaxBufferSizeInBytes(),
                    getMaxTimeInBufferMS(),
                    failOnError,
                    consumedDataType,
                    deliveryStream,
                    firehoseClientProperties,
                    encodingFormat);
        }
    }
}
