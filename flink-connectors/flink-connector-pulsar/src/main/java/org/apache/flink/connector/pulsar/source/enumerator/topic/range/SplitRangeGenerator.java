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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MAX_RANGE;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MIN_RANGE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This range generator would divide the range by the flink source parallelism. It would be the
 * default implementation for {@link SubscriptionType#Key_Shared} subscription.
 */
@PublicEvolving
public class SplitRangeGenerator implements RangeGenerator {
    private static final long serialVersionUID = -8682286436352905249L;

    private final int start;
    private final int end;

    public SplitRangeGenerator() {
        this(MIN_RANGE, MAX_RANGE);
    }

    public SplitRangeGenerator(int start, int end) {
        checkArgument(
                start >= MIN_RANGE,
                "Start range should be equal to or great than the min range " + MIN_RANGE);
        checkArgument(
                end <= MAX_RANGE, "End range should below or less than the max range " + MAX_RANGE);
        checkArgument(start <= end, "Start range should be equal to or less than the end range");

        this.start = start;
        this.end = end;
    }

    @Override
    public List<TopicRange> range(TopicMetadata metadata, int parallelism) {
        final int range = end - start + 1;
        final int size = Math.min(range, parallelism);
        int startRange = start;

        List<TopicRange> results = new ArrayList<>(size);
        for (int i = 1; i < size; i++) {
            int nextStartRange = i * range / size + start;
            results.add(new TopicRange(startRange, nextStartRange - 1));
            startRange = nextStartRange;
        }
        results.add(new TopicRange(startRange, end));

        return results;
    }

    @Override
    public KeySharedMode keyShareMode(TopicMetadata metadata, int parallelism) {
        return KeySharedMode.SPLIT;
    }
}
