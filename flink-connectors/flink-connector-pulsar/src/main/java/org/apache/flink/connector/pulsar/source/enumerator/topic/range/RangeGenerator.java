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

package org.apache.flink.connector.pulsar.source.enumerator.topic.range;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;

import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;
import java.util.List;

/**
 * A generator for generating the {@link TopicRange} for given topic. It was used for pulsar's
 * {@link SubscriptionType#Key_Shared} mode. {@link TopicRange} would be used in {@link
 * KeySharedPolicy} for different pulsar source readers.
 *
 * <p>If you implement this interface, make sure that each {@link TopicRange} would be assigned to a
 * specified source reader. Since flink parallelism is provided, make sure the pulsar message key's
 * hashcode is evenly distributed among these topic ranges.
 */
@PublicEvolving
@FunctionalInterface
public interface RangeGenerator extends Serializable {

    /**
     * Generate range for the given topic.
     *
     * @param metadata The metadata of the topic.
     * @param parallelism The reader size for this topic.
     */
    List<TopicRange> range(TopicMetadata metadata, int parallelism);

    default void open(Configuration configuration, SourceConfiguration sourceConfiguration) {
        // This method is used for user implementation.
    }
}
