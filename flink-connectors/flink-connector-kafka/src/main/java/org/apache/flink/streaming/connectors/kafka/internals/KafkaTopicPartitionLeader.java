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

import org.apache.kafka.common.Node;

import java.io.Serializable;

/**
 * Serializable Topic Partition info with leader Node information. This class is used at runtime.
 */
@Internal
public class KafkaTopicPartitionLeader implements Serializable {

    private static final long serialVersionUID = 9145855900303748582L;

    private final int leaderId;
    private final int leaderPort;
    private final String leaderHost;
    private final KafkaTopicPartition topicPartition;
    private final int cachedHash;

    public KafkaTopicPartitionLeader(KafkaTopicPartition topicPartition, Node leader) {
        this.topicPartition = topicPartition;
        if (leader == null) {
            this.leaderId = -1;
            this.leaderHost = null;
            this.leaderPort = -1;
        } else {
            this.leaderId = leader.id();
            this.leaderPort = leader.port();
            this.leaderHost = leader.host();
        }
        int cachedHash = (leader == null) ? 14 : leader.hashCode();
        this.cachedHash = 31 * cachedHash + topicPartition.hashCode();
    }

    public KafkaTopicPartition getTopicPartition() {
        return topicPartition;
    }

    public Node getLeader() {
        if (this.leaderId == -1) {
            return null;
        } else {
            return new Node(leaderId, leaderHost, leaderPort);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaTopicPartitionLeader)) {
            return false;
        }

        KafkaTopicPartitionLeader that = (KafkaTopicPartitionLeader) o;

        if (!topicPartition.equals(that.topicPartition)) {
            return false;
        }
        return leaderId == that.leaderId
                && leaderPort == that.leaderPort
                && leaderHost.equals(that.leaderHost);
    }

    @Override
    public int hashCode() {
        return cachedHash;
    }

    @Override
    public String toString() {
        return "KafkaTopicPartitionLeader{"
                + "leaderId="
                + leaderId
                + ", leaderPort="
                + leaderPort
                + ", leaderHost='"
                + leaderHost
                + '\''
                + ", topic="
                + topicPartition.getTopic()
                + ", partition="
                + topicPartition.getPartition()
                + '}';
    }
}
