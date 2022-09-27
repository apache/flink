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

package org.apache.flink.connector.pulsar.testutils.source.cases;

import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.source.PulsarSourceTestContext;

import org.apache.pulsar.client.api.SubscriptionType;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;

/**
 * A Pulsar external context that will create only one topic and use partitions in that topic as
 * source splits.
 */
public class SingleTopicConsumingContext extends PulsarSourceTestContext {

    private final String topicName = "pulsar-single-topic-" + randomAlphanumeric(8);

    private int index = 0;

    public SingleTopicConsumingContext(PulsarTestEnvironment environment) {
        super(environment);
    }

    @Override
    protected String displayName() {
        return "consume message on single topic";
    }

    @Override
    protected String topicPattern() {
        return topicName + ".+";
    }

    @Override
    protected String subscriptionName() {
        return "pulsar-single-topic-test";
    }

    @Override
    protected SubscriptionType subscriptionType() {
        return SubscriptionType.Exclusive;
    }

    @Override
    protected String generatePartitionName() {
        if (index == 0) {
            operator.createTopic(topicName, index + 1);
        } else {
            operator.increaseTopicPartitions(topicName, index + 1);
        }

        return topicNameWithPartition(topicName, index++);
    }
}
