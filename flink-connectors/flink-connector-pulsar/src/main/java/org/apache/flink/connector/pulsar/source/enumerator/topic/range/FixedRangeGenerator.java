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

import java.util.List;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.TopicRangeUtils.validateTopicRanges;

/** Always return the same range set for all topics. */
@PublicEvolving
public class FixedRangeGenerator implements RangeGenerator {
    private static final long serialVersionUID = -3895203007855538734L;

    private final List<TopicRange> ranges;
    private final KeySharedMode sharedMode;

    public FixedRangeGenerator(List<TopicRange> ranges) {
        this(ranges, KeySharedMode.JOIN);
    }

    public FixedRangeGenerator(List<TopicRange> ranges, KeySharedMode sharedMode) {
        validateTopicRanges(ranges, sharedMode);

        this.ranges = ranges;
        this.sharedMode = sharedMode;
    }

    @Override
    public List<TopicRange> range(TopicMetadata metadata, int parallelism) {
        return ranges;
    }

    @Override
    public KeySharedMode keyShareMode(TopicMetadata metadata, int parallelism) {
        return sharedMode;
    }
}
