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

/**
 * The offset commit mode represents the behaviour of how offsets are externally committed back to
 * Kafka brokers / Zookeeper.
 *
 * <p>The exact value of this is determined at runtime in the consumer subtasks.
 */
@Internal
public enum OffsetCommitMode {

    /** Completely disable offset committing. */
    DISABLED,

    /** Commit offsets back to Kafka only when checkpoints are completed. */
    ON_CHECKPOINTS,

    /**
     * Commit offsets periodically back to Kafka, using the auto commit functionality of internal
     * Kafka clients.
     */
    KAFKA_PERIODIC;
}
