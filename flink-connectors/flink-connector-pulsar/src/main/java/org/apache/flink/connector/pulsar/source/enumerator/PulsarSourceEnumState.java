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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssigner;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import java.util.HashSet;
import java.util.Set;

/**
 * The state class for pulsar source enumerator, used for storing the split state. This class is
 * managed and controlled by {@link SplitAssigner}.
 */
public class PulsarSourceEnumState {

    /** The topic partitions that have been appended to this source. */
    private final Set<TopicPartition> appendedPartitions;

    public PulsarSourceEnumState(Set<TopicPartition> appendedPartitions) {
        this.appendedPartitions = appendedPartitions;
    }

    public Set<TopicPartition> getAppendedPartitions() {
        return appendedPartitions;
    }

    /** The initial assignment state for Pulsar. */
    public static PulsarSourceEnumState initialState() {
        return new PulsarSourceEnumState(new HashSet<>());
    }
}
