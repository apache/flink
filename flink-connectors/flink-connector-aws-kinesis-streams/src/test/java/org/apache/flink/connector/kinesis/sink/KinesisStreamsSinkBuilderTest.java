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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Covers construction, defaults and sanity checking of KinesisStreamsSinkBuilder. */
public class KinesisStreamsSinkBuilderTest {
    private static final SerializationSchema<String> SERIALIZATION_SCHEMA =
            new SimpleStringSchema();
    private static final PartitionKeyGenerator<String> PARTITION_KEY_GENERATOR =
            element -> String.valueOf(element.hashCode());

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> KinesisStreamsSink.builder().setStreamName("stream").build())
                .withMessageContaining(
                        "No SerializationSchema was supplied to the KinesisStreamsSinkElementConverter builder.");
    }

    @Test
    public void streamNameOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSink.<String>builder()
                                        .setPartitionKeyGenerator(PARTITION_KEY_GENERATOR)
                                        .setSerializationSchema(SERIALIZATION_SCHEMA)
                                        .build())
                .withMessageContaining(
                        "The stream name must not be null when initializing the KDS Sink.");
    }

    @Test
    public void streamNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSink.<String>builder()
                                        .setStreamName("")
                                        .setPartitionKeyGenerator(PARTITION_KEY_GENERATOR)
                                        .setSerializationSchema(SERIALIZATION_SCHEMA)
                                        .build())
                .withMessageContaining(
                        "The stream name must be set when initializing the KDS Sink.");
    }

    @Test
    public void serializationSchemaMustBeSetWhenSinkIsBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSink.<String>builder()
                                        .setStreamName("stream")
                                        .setPartitionKeyGenerator(PARTITION_KEY_GENERATOR)
                                        .build())
                .withMessageContaining(
                        "No SerializationSchema was supplied to the KinesisStreamsSinkElementConverter builder.");
    }

    @Test
    public void partitionKeyGeneratorMustBeSetWhenSinkIsBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSink.<String>builder()
                                        .setStreamName("stream")
                                        .setSerializationSchema(SERIALIZATION_SCHEMA)
                                        .build())
                .withMessageContaining(
                        "No PartitionKeyGenerator lambda was supplied to the KinesisStreamsSinkElementConverter builder.");
    }
}
