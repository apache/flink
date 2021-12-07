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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarSourceBuilder}. */
@SuppressWarnings("java:S5778")
class PulsarSourceBuilderTest {

    @Test
    void someSetterMethodCouldOnlyBeCalledOnce() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        assertThatThrownBy(() -> builder.setAdminUrl("admin-url").setAdminUrl("admin-url2"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> builder.setServiceUrl("service-url").setServiceUrl("service-url2"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                builder.setSubscriptionName("set_subscription_name")
                                        .setSubscriptionName("set_subscription_name2"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                builder.setSubscriptionType(SubscriptionType.Exclusive)
                                        .setSubscriptionType(SubscriptionType.Shared))
                .isInstanceOf(IllegalArgumentException.class);
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

        assertThatThrownBy(() -> builder.setRangeGenerator(new UniformRangeGenerator()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingRequiredField() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
        builder.setAdminUrl("admin-url");
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
        builder.setServiceUrl("service-url");
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
        builder.setSubscriptionName("subscription-name");
        assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
        builder.setTopics("topic");
        assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
        builder.setDeserializationSchema(pulsarSchema(Schema.STRING));
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void defaultBuilder() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
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
