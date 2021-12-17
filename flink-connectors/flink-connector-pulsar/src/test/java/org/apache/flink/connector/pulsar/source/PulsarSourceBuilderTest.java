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

import org.apache.flink.connector.pulsar.source.enumerator.topic.range.UniformRangeGenerator;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link PulsarSourceBuilder}. */
@SuppressWarnings("java:S5778")
class PulsarSourceBuilderTest {

    @Test
    void someSetterMethodCouldOnlyBeCalledOnce() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setAdminUrl("admin-url").setAdminUrl("admin-url2"));
        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setServiceUrl("service-url").setServiceUrl("service-url2"));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        builder.setSubscriptionName("set_subscription_name")
                                .setSubscriptionName("set_subscription_name2"));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        builder.setSubscriptionType(SubscriptionType.Exclusive)
                                .setSubscriptionType(SubscriptionType.Shared));
    }

    @Test
    void topicPatternAndListCouldChooseOnlyOne() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        builder.setTopics("a", "b", "c");

        assertThrows(IllegalStateException.class, () -> builder.setTopicPattern("a-a-a"));
    }

    @Test
    void rangeGeneratorRequiresKeyShared() {
        PulsarSourceBuilder<String> builder = new PulsarSourceBuilder<>();
        builder.setSubscriptionType(SubscriptionType.Shared);

        assertThrows(
                IllegalArgumentException.class,
                () -> builder.setRangeGenerator(new UniformRangeGenerator()));
    }
}
