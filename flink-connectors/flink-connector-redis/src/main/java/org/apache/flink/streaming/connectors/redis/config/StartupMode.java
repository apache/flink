/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.config;

/**
 * Startup modes for the Redis Consumer. Based on
 * flink-connector-kafka-base/src/main/java/org/apache/flink/streaming/connectors/kafka/config/StartupMode.java
 */
public enum StartupMode {

    /**
     * Start from the earliest offset possible.
     */
    EARLIEST,

    /**
     * Start from user-supplied timestamp for each partition.
     */
    TIMESTAMP,

    /**
     * Start from user-supplied specific offsets for each partition.
     */
    SPECIFIC_OFFSETS,

    /**
     * Start from the latest offset.
     */
    LATEST,

    /**
     * Start from latest group offset.
     */
    GROUP_OFFSETS

}
