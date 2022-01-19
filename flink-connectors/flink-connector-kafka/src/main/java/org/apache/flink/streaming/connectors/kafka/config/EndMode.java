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

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;

/** End modes for the Kafka Consumer. */
@Internal
public enum EndMode {
    /** End from committed offsets in ZK / Kafka brokers of a specific consumer group (default). */
    END_GROUP_OFFSETS(KafkaTopicPartitionStateSentinel.GROUP_OFFSET),

    /** End from the earliest offset possible. */
    END_EARLIEST(KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET),

    /** End from the latest offset. */
    END_LATEST(KafkaTopicPartitionStateSentinel.LATEST_OFFSET),

    /**
     * End from user-supplied timestamp for each partition. Since this mode will have specific
     * offsets to end with, we do not need a sentinel value; using Long.MIN_VALUE as a placeholder.
     */
    END_TIMESTAMP(Long.MIN_VALUE),

    /**
     * End from user-supplied specific offsets for each partition. Since this mode will have
     * specific offsets to end with, we do not need a sentinel value; using Long.MIN_VALUE as a
     * placeholder.
     */
    END_SPECIFIC_OFFSETS(Long.MIN_VALUE),

    DEFAULT(Long.MIN_VALUE);

    /** The sentinel offset value corresponding to this End mode. */
    private long stateSentinel;

    EndMode(long stateSentinel) {
        this.stateSentinel = stateSentinel;
    }

    public long getStateSentinel() {
        return stateSentinel;
    }
}
