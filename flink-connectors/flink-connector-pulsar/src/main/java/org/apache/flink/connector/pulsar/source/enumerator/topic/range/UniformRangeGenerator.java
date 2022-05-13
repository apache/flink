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

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MAX_RANGE;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.RANGE_SIZE;

/**
 * This range generator would divide the range by the flink source parallelism. It would be the
 * default implementation for {@link SubscriptionType#Key_Shared} subscription.
 */
public class UniformRangeGenerator implements RangeGenerator {
    private static final long serialVersionUID = -7292650922683609268L;

    @Override
    public List<TopicRange> range(TopicMetadata metadata, int parallelism) {
        List<TopicRange> results = new ArrayList<>(parallelism);

        int startRange = 0;
        for (int i = 1; i < parallelism; i++) {
            int nextStartRange = i * RANGE_SIZE / parallelism;
            results.add(new TopicRange(startRange, nextStartRange - 1));
            startRange = nextStartRange;
        }
        results.add(new TopicRange(startRange, MAX_RANGE));

        return results;
    }
}
