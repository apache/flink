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

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/** the implements of consuming multiple topics. */
public class TopicListSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 6473918213832993116L;

    private final List<String> topics;

    public TopicListSubscriber(List<String> topics) {
        this.topics = topics;
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            PulsarAdmin pulsarAdmin, RangeGenerator rangeGenerator, int parallelism) {

        return topics.parallelStream()
                .map(topic -> queryTopicMetadata(pulsarAdmin, topic))
                .filter(Objects::nonNull)
                .flatMap(
                        metadata -> {
                            List<TopicRange> ranges = rangeGenerator.range(metadata, parallelism);
                            return toTopicPartitions(metadata, ranges).stream();
                        })
                .collect(toSet());
    }
}
