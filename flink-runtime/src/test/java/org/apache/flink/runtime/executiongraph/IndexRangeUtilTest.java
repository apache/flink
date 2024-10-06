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

package org.apache.flink.runtime.executiongraph;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexRangeUtil}. */
class IndexRangeUtilTest {
    @Test
    void testMergeIndexRanges() {
        Collection<IndexRange> emptyList = List.of();
        List<IndexRange> emptyResult = mergeIndexRanges(emptyList);
        assertThat(emptyResult).isEmpty();

        Collection<IndexRange> singleRangeList = List.of(new IndexRange(5, 10));
        List<IndexRange> singleRangeResult = mergeIndexRanges(singleRangeList);
        assertThat(singleRangeResult).containsExactly(new IndexRange(5, 10));

        Collection<IndexRange> overlappingRangesList =
                List.of(new IndexRange(5, 10), new IndexRange(8, 12));
        List<IndexRange> overlappingRangesResult = mergeIndexRanges(overlappingRangesList);
        assertThat(overlappingRangesResult).containsExactly(new IndexRange(5, 12));

        Collection<IndexRange> nonOverlappingRangesList =
                List.of(new IndexRange(1, 5), new IndexRange(10, 15));
        List<IndexRange> nonOverlappingRangesResult = mergeIndexRanges(nonOverlappingRangesList);
        assertThat(nonOverlappingRangesResult)
                .containsExactly(new IndexRange(1, 5), new IndexRange(10, 15));

        Collection<IndexRange> touchingRangesList =
                List.of(new IndexRange(1, 5), new IndexRange(6, 10));
        List<IndexRange> touchingRangesResult = mergeIndexRanges(touchingRangesList);
        assertThat(touchingRangesResult).containsExactly(new IndexRange(1, 10));

        Collection<IndexRange> mixedRangesList =
                List.of(
                        new IndexRange(1, 3),
                        new IndexRange(2, 6),
                        new IndexRange(8, 10),
                        new IndexRange(15, 18),
                        new IndexRange(19, 20));
        List<IndexRange> mixedRangesResult = mergeIndexRanges(mixedRangesList);
        assertThat(mixedRangesResult)
                .containsExactly(
                        new IndexRange(1, 6), new IndexRange(8, 10), new IndexRange(15, 20));
    }
}
