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

package org.apache.flink.runtime.scheduler.adaptivebatch.util;

import org.apache.flink.runtime.executiongraph.IndexRange;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.cartesianProduct;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeSkewThreshold;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeTargetSize;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.median;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.tryComputeSubpartitionSliceRange;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link VertexParallelismAndInputInfosDeciderUtils}. */
class VertexParallelismAndInputInfosDeciderUtilsTest {
    @Test
    void testCartesianProduct() {
        // empty input
        List<List<Integer>> inputEmpty = List.of();
        List<List<Integer>> expectedEmpty = List.of(List.of());
        List<List<Integer>> resultEmpty = cartesianProduct(inputEmpty);
        assertThat(resultEmpty).isEqualTo(expectedEmpty);

        // two lists
        List<List<Integer>> inputTwo = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4));
        List<List<Integer>> expectedTwo =
                Arrays.asList(
                        Arrays.asList(1, 3),
                        Arrays.asList(1, 4),
                        Arrays.asList(2, 3),
                        Arrays.asList(2, 4));
        List<List<Integer>> resultTwo = cartesianProduct(inputTwo);
        assertThat(resultTwo).isEqualTo(expectedTwo);

        // three lists
        List<List<String>> inputThree =
                Arrays.asList(
                        Arrays.asList("A", "B"), Arrays.asList("1", "2"), Arrays.asList("X", "Y"));
        List<List<String>> expectedThree =
                Arrays.asList(
                        Arrays.asList("A", "1", "X"),
                        Arrays.asList("A", "1", "Y"),
                        Arrays.asList("A", "2", "X"),
                        Arrays.asList("A", "2", "Y"),
                        Arrays.asList("B", "1", "X"),
                        Arrays.asList("B", "1", "Y"),
                        Arrays.asList("B", "2", "X"),
                        Arrays.asList("B", "2", "Y"));
        List<List<String>> resultThree = cartesianProduct(inputThree);
        assertThat(resultThree).isEqualTo(expectedThree);
    }

    @Test
    void testMedian() {
        long[] numsOdd = {5, 1, 3};
        long resultOdd = median(numsOdd);
        assertThat(resultOdd).isEqualTo(3L);

        long[] numsEven = {7, 3, 9, 1};
        long resultEven = median(numsEven);
        assertThat(resultEven).isEqualTo(5L);

        long[] numsSame = {2, 2, 2, 2, 2};
        long resultSame = median(numsSame);
        assertThat(resultSame).isEqualTo(2L);

        long[] numsSingle = {8};
        long resultSingle = median(numsSingle);
        assertThat(resultSingle).isEqualTo(8L);

        long[] numsEdges = {2, 4};
        long resultEdges = median(numsEdges);
        assertThat(resultEdges).isEqualTo(3L);

        long[] numsLessThanOne = {1, 2, 3, 0, 0};
        long resultLessThanOne = median(numsLessThanOne);
        assertThat(resultLessThanOne).isEqualTo(1L);
    }

    @Test
    void computeSkewThresholdTest() {
        long mediaSize1 = 100;
        double skewedFactor1 = 1.5;
        long defaultSkewedThreshold1 = 50;
        long result1 = computeSkewThreshold(mediaSize1, skewedFactor1, defaultSkewedThreshold1);
        assertThat(result1).isEqualTo(150L);

        // threshold less than default
        long mediaSize2 = 40;
        double skewedFactor2 = 1.0;
        long defaultSkewedThreshold2 = 50;
        long result2 = computeSkewThreshold(mediaSize2, skewedFactor2, defaultSkewedThreshold2);
        assertThat(result2).isEqualTo(50L);
    }

    @Test
    void testComputeTargetSize() {
        long[] subpartitionBytes1 = {100, 200, 150, 50};
        long skewedThreshold1 = 150;
        long dataVolumePerTask1 = 75;
        long result1 = computeTargetSize(subpartitionBytes1, skewedThreshold1, dataVolumePerTask1);
        assertThat(result1).isEqualTo(100L);

        // with a larger data volume per task
        long[] subpartitionBytes2 = {200, 180, 70, 30};
        long skewedThreshold2 = 100;
        long dataVolumePerTask2 = 80;
        long result2 = computeTargetSize(subpartitionBytes2, skewedThreshold2, dataVolumePerTask2);
        assertThat(result2).isEqualTo(80L);

        // No skewed partitions
        long[] subpartitionBytes3 = {100, 50, 75};
        long skewedThreshold3 = 200;
        long dataVolumePerTask3 = 60;
        long result3 = computeTargetSize(subpartitionBytes3, skewedThreshold3, dataVolumePerTask3);
        assertThat(result3).isEqualTo(75L);
    }

    @Test
    void testComputeSubpartitionSliceRange() {
        Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber =
                Map.of(1, createSubpartitionSlices(5, new long[] {100, 200, 300, 200, 100}));

        Optional<List<IndexRange>> subpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(1, 5, 300, subpartitionSlicesByTypeNumber);
        assertThat(subpartitionSliceRanges).isNotEmpty();
        assertThat(subpartitionSliceRanges.get())
                .isEqualTo(
                        List.of(new IndexRange(0, 1), new IndexRange(2, 2), new IndexRange(3, 4)));

        // test with a big max data volume per task
        subpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(1, 5, 10000, subpartitionSlicesByTypeNumber);
        assertThat(subpartitionSliceRanges).isNotEmpty();
        assertThat(subpartitionSliceRanges.get()).isEqualTo(List.of(new IndexRange(0, 4)));

        // test with a small max data volume per task
        subpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(1, 5, 100, subpartitionSlicesByTypeNumber);
        assertThat(subpartitionSliceRanges).isNotEmpty();
        assertThat(subpartitionSliceRanges.get())
                .isEqualTo(
                        List.of(
                                new IndexRange(0, 0),
                                new IndexRange(1, 1),
                                new IndexRange(2, 2),
                                new IndexRange(3, 3),
                                new IndexRange(4, 4)));

        // test fallback to adjust to the closest parallelism
        subpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(5, 5, 200, subpartitionSlicesByTypeNumber);
        assertThat(subpartitionSliceRanges).isNotEmpty();
        assertThat(subpartitionSliceRanges.get())
                .isEqualTo(
                        List.of(
                                new IndexRange(0, 0),
                                new IndexRange(1, 1),
                                new IndexRange(2, 2),
                                new IndexRange(3, 3),
                                new IndexRange(4, 4)));

        // test fallback to evenly distributed subpartition slices
        subpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(4, 4, 200, subpartitionSlicesByTypeNumber);
        assertThat(subpartitionSliceRanges).isNotEmpty();
        assertThat(subpartitionSliceRanges.get())
                .isEqualTo(
                        List.of(
                                new IndexRange(0, 0),
                                new IndexRange(1, 1),
                                new IndexRange(2, 2),
                                new IndexRange(3, 4)));

        // test failed to compute slice range
        subpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(6, 6, 200, subpartitionSlicesByTypeNumber);
        assertThat(subpartitionSliceRanges).isEmpty();
    }

    List<SubpartitionSlice> createSubpartitionSlices(int numSlices, long[] dataBytesPerSlice) {
        List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
        for (int i = 0; i < numSlices; ++i) {
            subpartitionSlices.add(
                    SubpartitionSlice.createSubpartitionSlice(
                            new IndexRange(0, 0), new IndexRange(i, i), dataBytesPerSlice[i]));
        }
        return subpartitionSlices;
    }
}
