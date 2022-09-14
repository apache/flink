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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl.TopicListSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl.TopicPatternSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.RegexSubscriptionMode;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Pulsar consumer allows a few different ways to consume from the topics, including:
 *
 * <ol>
 *   <li>Subscribe from a collection of topics.
 *   <li>Subscribe to a topic pattern using Java {@code Regex}.
 * </ol>
 *
 * <p>The PulsarSubscriber provides a unified interface for the Pulsar source to support all these
 * two types of subscribing mode.
 */
@Internal
public interface PulsarSubscriber extends Serializable {

    /**
     * Get a set of subscribed {@link TopicPartition}s. The method could throw {@link
     * IllegalStateException}, an extra try catch is required.
     *
     * @param pulsarAdmin The admin interface used to retrieve subscribed topic partitions.
     * @param rangeGenerator The range for different partitions.
     * @param parallelism The parallelism of flink source.
     * @return A subscribed {@link TopicPartition} for each pulsar topic partition.
     */
    Set<TopicPartition> getSubscribedTopicPartitions(
            PulsarAdmin pulsarAdmin, RangeGenerator rangeGenerator, int parallelism);

    // ----------------- factory methods --------------

    static PulsarSubscriber getTopicListSubscriber(List<String> topics) {
        return new TopicListSubscriber(topics);
    }

    static PulsarSubscriber getTopicPatternSubscriber(
            Pattern topicPattern, RegexSubscriptionMode subscriptionMode) {
        return new TopicPatternSubscriber(topicPattern, subscriptionMode);
    }
}
