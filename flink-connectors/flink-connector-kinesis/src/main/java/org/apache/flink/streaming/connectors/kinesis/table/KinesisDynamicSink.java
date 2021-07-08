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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.KinesisPartitioner;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Kinesis-backed {@link DynamicTableSink}. */
@Internal
public class KinesisDynamicSink implements DynamicTableSink, SupportsPartitioning {

    /** Consumed data type of the table. */
    private final DataType consumedDataType;

    /** The Kinesis stream to write to. */
    private final String stream;

    /** Properties for the Kinesis producer. */
    private final Properties producerProperties;

    /** Sink format for encoding records to Kinesis. */
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /** Partitioner to select Kinesis partition for each item. */
    private final KinesisPartitioner<RowData> partitioner;

    public KinesisDynamicSink(
            DataType consumedDataType,
            String stream,
            Properties producerProperties,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            KinesisPartitioner<RowData> partitioner) {

        this.consumedDataType =
                Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null");
        this.stream = Preconditions.checkNotNull(stream, "Kinesis stream name must not be null");
        this.producerProperties =
                Preconditions.checkNotNull(
                        producerProperties,
                        "Properties for the Flink Kinesis producer must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null");
        this.partitioner =
                Preconditions.checkNotNull(partitioner, "Kinesis partitioner must not be null");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                encodingFormat.createRuntimeEncoder(context, consumedDataType);

        FlinkKinesisProducer<RowData> kinesisProducer =
                new FlinkKinesisProducer<>(serializationSchema, producerProperties);
        kinesisProducer.setDefaultStream(stream);
        kinesisProducer.setCustomPartitioner(partitioner);

        return SinkFunctionProvider.of(kinesisProducer);
    }

    @Override
    public DynamicTableSink copy() {
        return new KinesisDynamicSink(
                consumedDataType, stream, producerProperties, encodingFormat, partitioner);
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
        if (partitioner instanceof RowDataFieldsKinesisPartitioner) {
            ((RowDataFieldsKinesisPartitioner) partitioner).setStaticFields(partition);
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
        return Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(stream, that.stream)
                && Objects.equals(producerProperties, that.producerProperties)
                && Objects.equals(encodingFormat, that.encodingFormat)
                && Objects.equals(partitioner, that.partitioner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                consumedDataType, stream, producerProperties, encodingFormat, partitioner);
    }
}
