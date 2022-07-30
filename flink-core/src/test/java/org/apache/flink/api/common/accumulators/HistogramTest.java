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

package org.apache.flink.api.common.accumulators;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link Histogram}. */
public class HistogramTest {

    private final Histogram histogram = new Histogram();

    @Test
    void testAddNotExistedValue() {
        histogram.add(5);

        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();
        assertEquals(1, actualLocalValue.size());
        assertEquals(1, actualLocalValue.get(5));
    }

    @Test
    void testAddExistedValue() {
        histogram.add(5);
        histogram.add(5);

        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();
        assertEquals(1, actualLocalValue.size());
        assertEquals(2, actualLocalValue.get(5));
    }

    @Test
    void testAddMultipleValues() {
        histogram.add(5);
        histogram.add(10);

        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();
        assertEquals(2, actualLocalValue.size());
        assertEquals(1, actualLocalValue.get(5));
        assertEquals(1, actualLocalValue.get(10));
    }

    @Test
    void testResetLocal() {
        histogram.add(5);
        histogram.resetLocal();

        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();
        assertTrue(actualLocalValue.isEmpty());
    }

    @Test
    void testMergeEmptyAndEmpty() {
        histogram.merge(new Histogram());
        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();

        assertTrue(actualLocalValue.isEmpty());
    }

    @Test
    void testMergeNoIntersections() {
        histogram.add(5);
        Histogram otherHistogram = new Histogram();
        otherHistogram.add(10);

        histogram.merge(otherHistogram);

        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();
        assertEquals(2, actualLocalValue.size());
        assertEquals(1, actualLocalValue.get(5));
        assertEquals(1, actualLocalValue.get(10));
    }

    @Test
    void testMergeWithIntersections() {
        histogram.add(5);
        Histogram otherHistogram = new Histogram();
        otherHistogram.add(5);
        otherHistogram.add(10);

        histogram.merge(otherHistogram);

        Map<Integer, Integer> actualLocalValue = histogram.getLocalValue();
        assertEquals(2, actualLocalValue.size());
        assertEquals(2, actualLocalValue.get(5));
        assertEquals(1, actualLocalValue.get(10));
    }

    @Test
    void testClone() {
        histogram.add(5);
        Histogram actualHistogram = histogram.clone();
        histogram.add(10);

        Map<Integer, Integer> actualLocalValue = actualHistogram.getLocalValue();
        assertEquals(1, actualLocalValue.size());
        assertEquals(1, actualLocalValue.get(5));
    }
}
