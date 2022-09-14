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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.UniformRangeGenerator;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link PulsarSourceBuilder}. */
class PulsarSourceBuilderTest {

    @Test
    void someSetterMethodCouldOnlyBeCalledOnce() {
        PulsarSourceBuilder<String> builder =
                new PulsarSourceBuilder<String>()
                        .setAdminUrl("admin-url")
                        .setServiceUrl("service-url")
                        .setSubscriptionName("set_subscription_name")
                        .setSubscriptionType(SubscriptionType.Exclusive);
        assertAll(
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setAdminUrl("admin-url2")),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setServiceUrl("service-url2")),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setSubscriptionName("set_subscription_name2")),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> builder.setSubscriptionType(SubscriptionType.Shared)));
    }

    @Test
    void topicPatternAndListCouldChooseOnlyOne() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        builder.setTopics("a", "b", "c");

        assertThatThrownBy(() -> builder.setTopicPattern("a-a-a"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rangeGeneratorRequiresKeyShared() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        builder.setSubscriptionType(SubscriptionType.Shared);
        UniformRangeGenerator rangeGenerator = new UniformRangeGenerator();

        assertThatThrownBy(() -> builder.setRangeGenerator(rangeGenerator))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void subscriptionTypeShouldNotBeOverriddenBySetMethod() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);

        Configuration config = new Configuration();
        config.set(PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Shared);
        builder.setConfig(config);

        assertThatThrownBy(() -> builder.setSubscriptionType(SubscriptionType.Failover))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void subscriptionTypeShouldNotBeOverriddenByConfiguration() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        fillRequiredFields(builder);

        builder.setSubscriptionType(SubscriptionType.Failover);

        Configuration config = new Configuration();
        config.set(PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Shared);
        assertThatThrownBy(() -> builder.setConfig(config))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void fillRequiredFields(PulsarSourceBuilder<String> builder) {
        builder.setAdminUrl("admin-url");
        builder.setServiceUrl("service-url");
        builder.setSubscriptionName("subscription-name");
        builder.setTopics("topic");
        builder.setDeserializationSchema(pulsarSchema(Schema.STRING));
    }
}
