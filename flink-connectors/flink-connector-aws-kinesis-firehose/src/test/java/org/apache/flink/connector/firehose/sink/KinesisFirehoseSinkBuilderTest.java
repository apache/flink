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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Properties;

/** Covers construction, defaults and sanity checking of {@link KinesisFirehoseSinkBuilder}. */
public class KinesisFirehoseSinkBuilderTest {

    private static final SerializationSchema<String> SERIALIZATION_SCHEMA =
            new SimpleStringSchema();

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisFirehoseSink.builder()
                                        .setDeliveryStreamName("deliveryStream")
                                        .build())
                .withMessageContaining(
                        "No SerializationSchema was supplied to the KinesisFirehoseSink builder.");
    }

    @Test
    public void streamNameOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisFirehoseSink.<String>builder()
                                        .setSerializationSchema(SERIALIZATION_SCHEMA)
                                        .build())
                .withMessageContaining(
                        "The delivery stream name must not be null when initializing the KDF Sink.");
    }

    @Test
    public void streamNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                KinesisFirehoseSink.<String>builder()
                                        .setDeliveryStreamName("")
                                        .setSerializationSchema(SERIALIZATION_SCHEMA)
                                        .build())
                .withMessageContaining(
                        "The delivery stream name must be set when initializing the KDF Sink.");
    }

    @Test
    public void defaultProtocolVersionInsertedToConfiguration() {
        Properties expectedProps = new Properties();
        expectedProps.setProperty(AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP1_1");
        Properties defaultProperties =
                KinesisFirehoseSink.<String>builder().getClientPropertiesWithDefaultHttpProtocol();

        Assertions.assertThat(defaultProperties).isEqualTo(expectedProps);
    }
}
