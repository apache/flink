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

    /**
     * Defines the default behavior for Key_Shared subscription in Flink. See {@link KeySharedMode}
     * for the detailed usage of the key share mode.
     *
     * @param metadata The metadata of the topic.
     * @param parallelism The reader size for this topic.
     */
    default KeySharedMode keyShareMode(TopicMetadata metadata, int parallelism) {
        return KeySharedMode.SPLIT;
    }

    /** Initialize some extra resources when bootstrap the source. */
    default void open(SourceConfiguration sourceConfiguration) {
        // This method is used for user implementation.
        open(sourceConfiguration, sourceConfiguration);
    }

    /** @deprecated Use {@link #open(SourceConfiguration)} instead. */
    @Deprecated
    default void open(Configuration configuration, SourceConfiguration sourceConfiguration) {
        // This method is used for user implementation.
    }

    /**
     * Different Key_Shared mode means different split assignment behaviors. If you only consume a
     * subset of Pulsar's key hash range, remember to use the {@link KeySharedMode#JOIN} mode which
     * will subscribe all the range in only one reader. Otherwise, when the ranges can join into a
     * full Pulsar key hash range (0 ~ 65535) you should use {@link KeySharedMode#SPLIT} for sharing
     * the splits among all the backend readers.
     *
     * <p>In the {@link KeySharedMode#SPLIT} mode. The topic will be subscribed by multiple readers.
     * But Pulsar has one limit in this situation. That is if a Message can't find the corresponding
     * reader by the key hash range. No messages will be delivered to the current readers, until
     * there is a reader which can subscribe to such messages.
     */
    @PublicEvolving
    enum KeySharedMode {

        /**
         * The topic ranges that the {@link RangeGenerator} generated will be split among the
         * readers.
         */
        SPLIT,

        /**
         * Assign all the topic ranges to only one reader instance. This is used for partial key
         * hash range subscription.
         */
        JOIN
    }
}
