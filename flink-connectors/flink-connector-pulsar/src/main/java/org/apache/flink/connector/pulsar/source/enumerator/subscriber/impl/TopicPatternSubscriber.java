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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toSet;

/** Subscribe to matching topics based on topic pattern. */
public class TopicPatternSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 3307710093243745104L;

    private final Pattern topicPattern;
    private final RegexSubscriptionMode subscriptionMode;
    private final String namespace;

    public TopicPatternSubscriber(Pattern topicPattern, RegexSubscriptionMode subscriptionMode) {
        this.topicPattern = topicPattern;
        this.subscriptionMode = subscriptionMode;

        // Extract the namespace from topic pattern regex.
        // If no namespace provided in the regex, we would directly use "default" as the namespace.
        TopicName destination = TopicName.get(topicPattern.toString());
        NamespaceName namespaceName = destination.getNamespaceObject();
        this.namespace = namespaceName.toString();
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            PulsarAdmin pulsarAdmin, RangeGenerator rangeGenerator, int parallelism) {
        try {
            return pulsarAdmin
                    .namespaces()
                    .getTopics(namespace)
                    .parallelStream()
                    .filter(this::matchesSubscriptionMode)
                    .filter(topic -> topicPattern.matcher(topic).find())
                    .map(topic -> queryTopicMetadata(pulsarAdmin, topic))
                    .filter(Objects::nonNull)
                    .flatMap(
                            metadata -> {
                                List<TopicRange> ranges =
                                        rangeGenerator.range(metadata, parallelism);
                                return toTopicPartitions(metadata, ranges).stream();
                            })
                    .collect(toSet());
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                // Skip the topic metadata query.
                return Collections.emptySet();
            } else {
                // This method would cause the failure for subscriber.
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Filter the topic by regex subscription mode. This logic is the same as pulsar consumer's
     * regex subscription.
     */
    private boolean matchesSubscriptionMode(String topic) {
        TopicName topicName = TopicName.get(topic);
        // Filter the topic persistence.
        switch (subscriptionMode) {
            case PersistentOnly:
                return topicName.isPersistent();
            case NonPersistentOnly:
                return !topicName.isPersistent();
            default:
                // RegexSubscriptionMode.AllTopics
                return true;
        }
    }
}
