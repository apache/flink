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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link KafkaSourceBuilder}. */
public class KafkaSourceBuilderTest extends TestLogger {

    @Test
    public void testBuildSourceWithGroupId() {
        final KafkaSource<String> kafkaSource = getBasicBuilder().setGroupId("groupId").build();
        // Commit on checkpoint should be enabled by default
        Assert.assertTrue(
                kafkaSource
                        .getConfiguration()
                        .get(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT));
        // Auto commit should be disabled by default
        Assert.assertFalse(
                kafkaSource
                        .getConfiguration()
                        .get(
                                ConfigOptions.key(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                                        .booleanType()
                                        .noDefaultValue()));
    }

    @Test
    public void testBuildSourceWithoutGroupId() {
        final KafkaSource<String> kafkaSource = getBasicBuilder().build();
        // Commit on checkpoint and auto commit should be disabled because group.id is not specified
        Assert.assertFalse(
                kafkaSource
                        .getConfiguration()
                        .get(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT));
        Assert.assertFalse(
                kafkaSource
                        .getConfiguration()
                        .get(
                                ConfigOptions.key(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                                        .booleanType()
                                        .noDefaultValue()));
    }

    @Test
    public void testEnableCommitOnCheckpointWithoutGroupId() {
        final IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT
                                                        .key(),
                                                "true")
                                        .build());
        MatcherAssert.assertThat(
                exception.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when offset commit is enabled"));
    }

    @Test
    public void testEnableAutoCommitWithoutGroupId() {
        final IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                                        .build());
        MatcherAssert.assertThat(
                exception.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when offset commit is enabled"));
    }

    @Test
    public void testDisableOffsetCommitWithoutGroupId() {
        getBasicBuilder()
                .setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false")
                .build();
        getBasicBuilder().setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false").build();
    }

    @Test
    public void testUsingCommittedOffsetsInitializerWithoutGroupId() {
        // Using OffsetsInitializer#committedOffsets as starting offsets
        final IllegalStateException startingOffsetException =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setStartingOffsets(OffsetsInitializer.committedOffsets())
                                        .build());
        MatcherAssert.assertThat(
                startingOffsetException.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when using committed offset for offsets initializer"));

        // Using OffsetsInitializer#committedOffsets as stopping offsets
        final IllegalStateException stoppingOffsetException =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setBounded(OffsetsInitializer.committedOffsets())
                                        .build());
        MatcherAssert.assertThat(
                stoppingOffsetException.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when using committed offset for offsets initializer"));

        // Using OffsetsInitializer#offsets to manually specify committed offset as starting offset
        final IllegalStateException specificStartingOffsetException =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            final Map<TopicPartition, Long> offsetMap = new HashMap<>();
                            offsetMap.put(
                                    new TopicPartition("topic", 0),
                                    KafkaPartitionSplit.COMMITTED_OFFSET);
                            getBasicBuilder()
                                    .setStartingOffsets(OffsetsInitializer.offsets(offsetMap))
                                    .build();
                        });
        MatcherAssert.assertThat(
                specificStartingOffsetException.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required because partition topic-0 is initialized with committed offset"));
    }

    private KafkaSourceBuilder<String> getBasicBuilder() {
        return new KafkaSourceBuilder<String>()
                .setBootstrapServers("testServer")
                .setTopics("topic")
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));
    }

    @SuppressWarnings({"unchecked", "SameParameterValue"})
    private <T extends Throwable> T assertThrows(Class<T> exceptionClass, Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            if (exceptionClass.isInstance(t)) {
                return (T) t;
            }
            throw new AssertionError(
                    String.format(
                            "Expected %s to be thrown, but actually got %s",
                            exceptionClass, t.getClass()));
        }
        throw new AssertionError(
                String.format("Expected %s to be thrown, but nothing was thrown", exceptionClass));
    }
}
