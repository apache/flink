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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The unit test for {@link SplitRangeGenerator}. */
class SplitRangeGeneratorTest {

    private static final TopicMetadata METADATA = new TopicMetadata("fake", 10);

    @Test
    void rangeWithParallelismOne() {
        SplitRangeGenerator generator = new SplitRangeGenerator(6, 65534);
        List<TopicRange> ranges = generator.range(METADATA, 1);
        List<TopicRange> desired = Collections.singletonList(new TopicRange(6, 65534));

        assertThat(ranges).hasSize(1).containsExactlyElementsOf(desired);
    }

    @Test
    void rangeBelowTheParallelism() {
        SplitRangeGenerator generator = new SplitRangeGenerator(3, 10);
        List<TopicRange> ranges = generator.range(METADATA, 12);
        List<TopicRange> desired =
                Arrays.asList(
                        new TopicRange(3, 3),
                        new TopicRange(4, 4),
                        new TopicRange(5, 5),
                        new TopicRange(6, 6),
                        new TopicRange(7, 7),
                        new TopicRange(8, 8),
                        new TopicRange(9, 9),
                        new TopicRange(10, 10));

        assertThat(ranges).hasSize(8).containsExactlyElementsOf(desired);
    }

    @Test
    void rangeWasDivideWithLastBiggerSize() {
        SplitRangeGenerator generator = new SplitRangeGenerator(0, 100);
        List<TopicRange> ranges = generator.range(METADATA, 9);
        List<TopicRange> desired =
                Arrays.asList(
                        new TopicRange(0, 10),
                        new TopicRange(11, 21),
                        new TopicRange(22, 32),
                        new TopicRange(33, 43),
                        new TopicRange(44, 55),
                        new TopicRange(56, 66),
                        new TopicRange(67, 77),
                        new TopicRange(78, 88),
                        new TopicRange(89, 100));

        assertThat(ranges).hasSize(9).containsExactlyElementsOf(desired);
    }
}
