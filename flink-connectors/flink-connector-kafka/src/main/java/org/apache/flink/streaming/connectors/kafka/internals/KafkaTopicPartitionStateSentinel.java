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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

/**
 * Magic values used to represent special offset states before partitions are actually read.
 *
 * <p>The values are all negative. Negative offsets are not used by Kafka (invalid), so we pick a
 * number that is probably (hopefully) not used by Kafka as a magic number for anything else.
 */
@Internal
public class KafkaTopicPartitionStateSentinel {

    /** Magic number that defines an unset offset. */
    public static final long OFFSET_NOT_SET = -915623761776L;

    /**
     * Magic number that defines the partition should start from the earliest offset.
     *
     * <p>This is used as a placeholder so that the actual earliest offset can be evaluated lazily
     * when the partition will actually start to be read by the consumer.
     */
    public static final long EARLIEST_OFFSET = -915623761775L;

    /**
     * Magic number that defines the partition should start from the latest offset.
     *
     * <p>This is used as a placeholder so that the actual latest offset can be evaluated lazily
     * when the partition will actually start to be read by the consumer.
     */
    public static final long LATEST_OFFSET = -915623761774L;

    /**
     * Magic number that defines the partition should start from its committed group offset in
     * Kafka.
     *
     * <p>This is used as a placeholder so that the actual committed group offset can be evaluated
     * lazily when the partition will actually start to be read by the consumer.
     */
    public static final long GROUP_OFFSET = -915623761773L;

    public static boolean isSentinel(long offset) {
        return offset < 0;
    }
}
