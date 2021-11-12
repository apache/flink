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

package org.apache.flink.connector.kinesis.sink.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.kinesis.sink.KinesisDataStreamsSink;
import org.apache.flink.connector.kinesis.sink.KinesisDataStreamsSinkElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

/**
 * An example application demonstrating how to use the {@link KinesisDataStreamsSink} to sink into
 * KDS.
 *
 * <p>The {@link KinesisAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
public class SinkIntoKinesis {

    private static final String JSON_PAYLOAD_TEMPLATE = "{\"data\": \"%s\"}";

    private static final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            KinesisDataStreamsSinkElementConverter.<String>builder()
                    .serializationSchema(new SimpleStringSchema())
                    .partitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                    .build();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> fromGen =
                env.fromSequence(1, 10_000_000L)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(data -> String.format(JSON_PAYLOAD_TEMPLATE, data));

        KinesisDataStreamsSink<String> kdsSink =
                KinesisDataStreamsSink.<String>builder()
                        .setElementConverter(elementConverter)
                        .setStreamName("your_stream_name")
                        .build();

        fromGen.sinkTo(kdsSink);

        env.execute("KDS Async Sink Example Program");
    }
}
