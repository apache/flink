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

package org.apache.flink.connector.pulsar.sink.writer.router;

import org.apache.flink.annotation.PublicEvolving;

/** The routing policy for choosing the desired topic by the given message. */
@PublicEvolving
public enum TopicRoutingMode {

    /**
     * The producer will publish messages across all partitions in a round-robin fashion to achieve
     * maximum throughput. Please note that round-robin is not done per individual message but
     * rather it's set to the same boundary of batching delay, to ensure batching is effective.
     */
    ROUND_ROBIN,

    /**
     * If no key is provided, The partitioned producer will randomly pick one single topic partition
     * and publish all the messages into that partition. If a key is provided on the message, the
     * partitioned producer will hash the key and assign the message to a particular partition.
     */
    MESSAGE_KEY_HASH,

    /**
     * Use custom topic router implementation that will be called to determine the partition for a
     * particular message.
     */
    CUSTOM;
}
