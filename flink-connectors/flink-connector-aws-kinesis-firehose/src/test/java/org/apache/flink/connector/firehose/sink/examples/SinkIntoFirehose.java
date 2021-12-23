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

package org.apache.flink.connector.firehose.sink.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSinkElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.utils.ImmutableMap;

import java.util.Properties;

/**
 * An example application demonstrating how to use the {@link KinesisFirehoseSink} to sink into KDF.
 *
 * <p>The {@link FirehoseAsyncClient} used here may be configured in the standard way for the AWS
 * SDK 2.x. e.g. the provision of {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}
 * through environment variables etc.
 */
public class SinkIntoFirehose {

    private static final KinesisFirehoseSinkElementConverter<String> elementConverter =
            KinesisFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> generator =
                env.fromSequence(1, 10_000_000L)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(data -> mapper.writeValueAsString(ImmutableMap.of("data", data)));

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");

        KinesisFirehoseSink<String> kdfSink =
                KinesisFirehoseSink.<String>builder()
                        .setElementConverter(elementConverter)
                        .setDeliveryStreamName("delivery-stream")
                        .setMaxBatchSize(20)
                        .setFirehoseClientProperties(sinkProperties)
                        .build();

        generator.sinkTo(kdfSink);

        env.execute("KDF Async Sink Example Program");
    }
}
