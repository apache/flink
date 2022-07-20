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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Collections;
import java.util.List;

/**
 * Default implementation for {@link SubscriptionType#Shared}, {@link SubscriptionType#Failover} and
 * {@link SubscriptionType#Exclusive} subscription.
 */
public class FullRangeGenerator implements RangeGenerator {
    private static final long serialVersionUID = -4571731955155036216L;

    @Override
    public List<TopicRange> range(TopicMetadata metadata, int parallelism) {
        return Collections.singletonList(TopicRange.createFullRange());
    }
}
