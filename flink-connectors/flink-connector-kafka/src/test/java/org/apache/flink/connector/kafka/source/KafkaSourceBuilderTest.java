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
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaSourceBuilder}. */
@ExtendWith(TestLoggerExtension.class)
public class KafkaSourceBuilderTest {

    @Test
    public void testBuildSourceWithGroupId() {
        final KafkaSource<String> kafkaSource = getBasicBuilder().setGroupId("groupId").build();
        // Commit on checkpoint should be enabled by default
        assertThat(
                        kafkaSource
                                .getConfiguration()
                                .get(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT))
                .isTrue();
        // Auto commit should be disabled by default
        assertThat(
                        kafkaSource
                                .getConfiguration()
                                .get(
                                        ConfigOptions.key(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                                                .booleanType()
                                                .noDefaultValue()))
                .isFalse();
    }

    @Test
    public void testBuildSourceWithoutGroupId() {
        final KafkaSource<String> kafkaSource = getBasicBuilder().build();
        // Commit on checkpoint and auto commit should be disabled because group.id is not specified
        assertThat(
                        kafkaSource
                                .getConfiguration()
                                .get(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT))
                .isFalse();
        assertThat(
                        kafkaSource
                                .getConfiguration()
                                .get(
                                        ConfigOptions.key(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                                                .booleanType()
                                                .noDefaultValue()))
                .isFalse();
    }

    @Test
    public void testEnableCommitOnCheckpointWithoutGroupId() {
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT
                                                        .key(),
                                                "true")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property group.id is required when offset commit is enabled");
    }

    @Test
    public void testEnableAutoCommitWithoutGroupId() {
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property group.id is required when offset commit is enabled");
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
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setStartingOffsets(OffsetsInitializer.committedOffsets())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property group.id is required when using committed offset for offsets initializer");

        // Using OffsetsInitializer#committedOffsets as stopping offsets
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setBounded(OffsetsInitializer.committedOffsets())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property group.id is required when using committed offset for offsets initializer");

        // Using OffsetsInitializer#offsets to manually specify committed offset as starting offset
        assertThatThrownBy(
                        () -> {
                            final Map<TopicPartition, Long> offsetMap = new HashMap<>();
                            offsetMap.put(
                                    new TopicPartition("topic", 0),
                                    KafkaPartitionSplit.COMMITTED_OFFSET);
                            getBasicBuilder()
                                    .setStartingOffsets(OffsetsInitializer.offsets(offsetMap))
                                    .build();
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property group.id is required because partition topic-0 is initialized with committed offset");
    }

    @Test
    public void testSettingCustomKafkaSubscriber() {
        ExampleCustomSubscriber exampleCustomSubscriber = new ExampleCustomSubscriber();
        KafkaSourceBuilder<String> customKafkaSubscriberBuilder =
                new KafkaSourceBuilder<String>()
                        .setBootstrapServers("testServer")
                        .setKafkaSubscriber(exampleCustomSubscriber)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        StringDeserializer.class));

        assertThat(customKafkaSubscriberBuilder.build().getKafkaSubscriber())
                .isEqualTo(exampleCustomSubscriber);

        assertThatThrownBy(() -> customKafkaSubscriberBuilder.setTopics("topic"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot use topics for consumption because a ExampleCustomSubscriber is already set for consumption.");

        assertThatThrownBy(
                        () -> customKafkaSubscriberBuilder.setTopicPattern(Pattern.compile(".+")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot use topic pattern for consumption because a ExampleCustomSubscriber is already set for consumption.");

        assertThatThrownBy(
                        () ->
                                customKafkaSubscriberBuilder.setPartitions(
                                        Collections.singleton(new TopicPartition("topic", 0))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot use partitions for consumption because a ExampleCustomSubscriber is already set for consumption.");
    }

    private KafkaSourceBuilder<String> getBasicBuilder() {
        return new KafkaSourceBuilder<String>()
                .setBootstrapServers("testServer")
                .setTopics("topic")
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));
    }

    private static class ExampleCustomSubscriber implements KafkaSubscriber {

        @Override
        public Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
            return Collections.singleton(new TopicPartition("topic", 0));
        }
    }
}
