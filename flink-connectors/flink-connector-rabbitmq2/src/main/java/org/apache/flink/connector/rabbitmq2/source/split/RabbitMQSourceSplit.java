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

package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumerator;

import java.util.HashSet;
import java.util.Set;

/**
 * This split is passed by the {@link RabbitMQSourceEnumerator} to the SourceReader. It contains the
 * configuration for the connection and the name of the queue to connect to. In case of exactly-once
 * the correlation ids for deduplication of messages might contain data. They are fIt might contain
 * data ife single reader fails and a new reader needs to be * created.
 */
public class RabbitMQSourceSplit implements SourceSplit {

    private final RabbitMQConnectionConfig connectionConfig;
    private final String rmqQueueName;
    private Set<String> correlationIds;

    public RabbitMQSourceSplit(RabbitMQConnectionConfig connectionConfig, String rmqQueueName) {
        this(connectionConfig, rmqQueueName, new HashSet<>());
    }

    public RabbitMQSourceSplit(
            RabbitMQConnectionConfig connectionConfig,
            String rmqQueueName,
            Set<String> correlationIds) {
        this.connectionConfig = connectionConfig;
        this.rmqQueueName = rmqQueueName;
        this.correlationIds = correlationIds;
    }

    /**
     * Create a copy of the the split.
     *
     * @return RabbitMQSourceSplit which is a copy of this split.
     */
    public RabbitMQSourceSplit copy() {
        return new RabbitMQSourceSplit(
                connectionConfig, rmqQueueName, new HashSet<>(correlationIds));
    }

    /**
     * Get the correlation ids specified in the split.
     *
     * @return Set of all correlation ids.
     */
    public Set<String> getCorrelationIds() {
        return correlationIds;
    }

    /**
     * Get the name of the queue to consume from defined in the split.
     *
     * @return String name of the queue
     */
    public String getQueueName() {
        return rmqQueueName;
    }

    /**
     * Get the connection configuration of RabbitMQ defined in the split.
     *
     * @return RMQConnectionConfig connection configuration of RabbitMQ.
     * @see RabbitMQConnectionConfig
     */
    public RabbitMQConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    /**
     * Set the correlation ids specified in this split.
     *
     * @param newCorrelationIds the correlation ids that will be set.
     */
    public void setCorrelationIds(Set<String> newCorrelationIds) {
        correlationIds = newCorrelationIds;
    }

    @Override
    public String splitId() {
        // Is fixed as there will be only one split that is relevant for the enumerator.
        return "0";
    }
}
